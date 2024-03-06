/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.action

import com.sbt.ignite.gatling.api.IgniteApi
import com.sbt.ignite.gatling.api.TransactionApi
import com.sbt.ignite.gatling.protocol.IgniteProtocol
import com.sbt.ignite.gatling.protocol.IgniteProtocol.IgniteApiSessionKey
import com.sbt.ignite.gatling.protocol.IgniteProtocol.TransactionApiSessionKey
import io.gatling.commons.stats.KO
import io.gatling.commons.stats.OK
import io.gatling.commons.stats.Status
import io.gatling.commons.util.Clock
import io.gatling.commons.validation.Failure
import io.gatling.commons.validation.Success
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.commons.validation.Validation
import io.gatling.core.Predef._
import io.gatling.core.action.Action
import io.gatling.core.action.ChainableAction
import io.gatling.core.check.Check
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen

/**
 * Base class for all Ignite actions.
 *
 * @param actionType Action type name.
 * @param requestName Name of the request provided via the DSL. May be empty.  If so the defaultRequestName will be used.
 * @param ctx Gatling scenario context.
 * @param next Next action to execute in scenario chain.
 */
abstract class IgniteAction(val actionType: String, val requestName: Expression[String], val ctx: ScenarioContext, val next: Action)
    extends ChainableAction
    with NameGen {

    /** @return Default request name if none was provided via the DSL. */
    def defaultRequestName: Expression[String] = _ => name.success

    /** @return Action name. */
    val name: String = genName(actionType)

    /** Clock used to measure time the action takes. */
    val clock: Clock = ctx.protocolComponentsRegistry.components(IgniteProtocol.IgniteProtocolKey).coreComponents.clock

    /** Statistics engine */
    val statsEngine: StatsEngine = ctx.protocolComponentsRegistry.components(IgniteProtocol.IgniteProtocolKey).coreComponents.statsEngine

    /** Ignite protocol */
    val protocol: IgniteProtocol = ctx.protocolComponentsRegistry.components(IgniteProtocol.IgniteProtocolKey).igniteProtocol

    /**
     * Logs results of action execution and starts the next action in the chain.
     *
     * @param session Session.
     * @param requestName Name of the request.
     * @param sent Timestamp of the request processing start.
     * @param received Timestamp of the request processing finish.
     * @param status Status of the executed request.
     * @param responseCode Optional string representation of response code in case action failed.
     * @param message Optional error message in case action failed.
     */
    protected def logAndExecuteNext(
        session: Session,
        requestName: String,
        sent: Long,
        received: Long,
        status: Status,
        responseCode: Option[String],
        message: Option[String]
    ): Unit = {
        statsEngine.logResponse(
            session.scenario,
            session.groups,
            requestName,
            sent,
            received,
            status,
            responseCode,
            message
        )

        next ! session.logGroupRequestTimings(sent, received)
    }

    /**
     * Executes an action in context of Session. Logs crash if session context is not valid.
     *
     * @param session Session.
     * @param f Function executing an action.
     */
    protected def withSessionCheck(session: Session)(f: => Validation[Unit]): Unit = {
        logger.debug(s"session user id: #${session.userId}, $name")
        f.onFailure { errorMessage =>
            val resolvedRequestName =
                requestName(session).toOption.filter(_.nonEmpty).getOrElse(defaultRequestName(session).toOption.getOrElse(name))

            logger.error(
                s"Error in ignite action during session check, user id: #${session.userId}, request name: $resolvedRequestName: $errorMessage"
            )

            statsEngine.logCrash(session.scenario, session.groups, resolvedRequestName, errorMessage)

            statsEngine.logResponse(
                session.scenario,
                session.groups,
                resolvedRequestName,
                0,
                0,
                KO,
                Some("CRASH"),
                Some(errorMessage)
            )

            stopSimulation(errorMessage, session.markAsFailed)
        }
    }

    private def stopSimulation(errorMessage: String, session: Session): Unit =
        stopInjector(errorMessage).actionBuilders.head.build(
            ctx,
            exitHere.actionBuilders.head.build(ctx, next)
        ) ! session

    /**
     * Common parameters for ignite actions.
     *
     * @param requestName Name of request.
     * @param igniteApi Instance of IgniteApi.
     * @param transactionApi Instance of TransactionApi. Present in session context if Ignite transaction was started.
     */
    case class IgniteActionParameters(requestName: String, igniteApi: IgniteApi, transactionApi: Option[TransactionApi])

    /**
     * Resolves the request name.
     *
     * If name is resolved successfully to empty string (it may be either it wasn't specified via DSL
     * or explicitly configured empty) the default one is used.
     *
     * @param session Session
     * @return Some non-empty request name.
     */
    def resolveRequestName(session: Session): Validation[String] =
        requestName(session) match {
            case f: Failure => f

            case Success(value) => if (value.isEmpty) defaultRequestName(session) else value.success
        }

    /**
     * Resolves ignite action parameters using session context.
     *
     * @param session Session.
     * @return Instance of IgniteActionParameters.
     */
    def resolveIgniteParameters(session: Session): Validation[IgniteActionParameters] =
        for {
            resolvedRequestName <- resolveRequestName(session)

            igniteApi <- session(IgniteApiSessionKey).validate[IgniteApi]

            transactionApi <- session(TransactionApiSessionKey).asOption[TransactionApi].success
        } yield IgniteActionParameters(resolvedRequestName, igniteApi, transactionApi)

    /**
     * Calls function from the Ignite API,
     * performs checks against the result,
     * records outcome and execution time and
     * invokes next action in the chain.
     *
     * In case of successful execution of function calls the lambda provided to update the session state.
     *
     * @tparam R Type of the result.
     * @param func Function to execute (some function from the Ignite API).
     * @param resolvedRequestName Resolved request name.
     * @param session Session.
     * @param checks List of checks to perform (may be empty).
     * @param updateSession Function to be called to update session on successful function execution. Keep
     *                      session unchanged if absent.
     * @param s Session.
     * @param r Result of the operation.
     */
    def call[R](
        func: (R => Unit, Throwable => Unit) => Unit,
        resolvedRequestName: String,
        session: Session,
        checks: Seq[Check[R]] = Seq.empty,
        updateSession: (Session, Option[R]) => Session = (s: Session, r: Option[R]) => s
    ): Unit = {
        val startTime = clock.nowMillis

        func(
            result => {
                val finishTime = clock.nowMillis

                logger.debug(s"session user id: #${session.userId}, after $name, result $result")

                processResult(
                    result,
                    startTime,
                    finishTime,
                    resolvedRequestName,
                    session,
                    checks,
                    updateSession
                )
            },
            ex => {
                val finishTime = clock.nowMillis

                logger.debug(s"Exception in ignite actions, user id: #${session.userId}, request name: $resolvedRequestName", ex)

                handleError(
                    ex,
                    startTime,
                    resolvedRequestName,
                    session
                )
            }
        )
    }

    private def processResult[R](
        result: R,
        startTime: Long,
        finishTime: Long,
        resolvedRequestName: String,
        session: Session,
        checks: Seq[Check[R]] = Seq.empty,
        updateSession: (Session, Option[R]) => Session
    ): Unit = {
        val (checkedSession, error) = Check.check(result, session, checks.toList)

        error match {
            case Some(Failure(errorMessage)) =>
                logAndExecuteNext(
                    updateSession(checkedSession.markAsFailed, Some(result)),
                    resolvedRequestName,
                    startTime,
                    finishTime,
                    KO,
                    Some("Check ERROR"),
                    Some(errorMessage)
                )

            case None =>
                logAndExecuteNext(
                    updateSession(checkedSession, Some(result)),
                    resolvedRequestName,
                    startTime,
                    finishTime,
                    OK,
                    None,
                    None
                )
        }
    }

    private def handleError(
        ex: Throwable,
        startTime: Long,
        resolvedRequestName: String,
        session: Session
    ): Unit = {
        session(TransactionApiSessionKey).asOption[TransactionApi].foreach {
            _.close(_ => (), _ => ())
        }

        logAndExecuteNext(
            session.markAsFailed,
            resolvedRequestName,
            startTime,
            clock.nowMillis,
            KO,
            Some("ERROR"),
            Some(strip(ex.getMessage, 120))
        )
    }

    private def strip(string: String, length: Int): String =
        if (string.length <= length) {
            string
        } else {
            string.take(length) + " ..."
        }
}

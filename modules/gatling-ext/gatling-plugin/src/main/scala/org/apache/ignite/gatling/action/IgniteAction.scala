/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.gatling.action

import io.gatling.commons.stats.KO
import io.gatling.commons.stats.OK
import io.gatling.commons.stats.Status
import io.gatling.commons.util.Clock
import io.gatling.commons.validation.Failure
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.commons.validation.Validation
import io.gatling.core.Predef._
import io.gatling.core.action.Action
import io.gatling.core.action.ChainableAction
import io.gatling.core.check.Check
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen
import org.apache.ignite.gatling.api.IgniteApi
import org.apache.ignite.gatling.api.TransactionApi
import org.apache.ignite.gatling.protocol.IgniteProtocol
import org.apache.ignite.gatling.protocol.IgniteProtocol.IgniteApiSessionKey
import org.apache.ignite.gatling.protocol.IgniteProtocol.TransactionApiSessionKey

/**
 * Base class for all Ignite actions.
 *
 * @param actionType Action type name.
 * @param requestName Name of the request provided via the DSL. May be empty. If so it will be generated from action type name.
 * @param ctx Gatling scenario context.
 * @param next Next action to execute in scenario chain.
 */
abstract class IgniteAction(val actionType: String, val requestName: String, val ctx: ScenarioContext, val next: Action)
    extends ChainableAction
    with NameGen {
    /** @return Action name. */
    val name: String = genName(actionType)

    /** @return Default request name if none was provided via the DSL. */
    val request: String = if (requestName == "") name else requestName

    /** Clock used to measure time the action takes. */
    val clock: Clock = {
        val components = ctx.protocolComponentsRegistry.components(IgniteProtocol.IgniteProtocolKey)

        components.clock.getOrElse(
            components.coreComponents.clock
        )
    }

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
            logger.error(
                s"Error in ignite action during session check, user id: #${session.userId}, request name: $request: $errorMessage"
            )

            statsEngine.logCrash(session.scenario, session.groups, request, errorMessage)

            statsEngine.logResponse(
                session.scenario,
                session.groups,
                request,
                clock.nowMillis,
                clock.nowMillis,
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
     * @param igniteApi Instance of IgniteApi.
     * @param transactionApi Instance of TransactionApi. Present in session context if Ignite transaction was started.
     */
    case class IgniteActionParameters(igniteApi: IgniteApi, transactionApi: Option[TransactionApi])

    /**
     * Resolves ignite action parameters using session context.
     *
     * @param session Session.
     * @return Instance of IgniteActionParameters.
     */
    def resolveIgniteParameters(session: Session): Validation[IgniteActionParameters] =
        for {
            igniteApi <- session(IgniteApiSessionKey).validate[IgniteApi]

            transactionApi <- session(TransactionApiSessionKey).asOption[TransactionApi].success
        } yield IgniteActionParameters(igniteApi, transactionApi)

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
     * @param session Session.
     * @param checks List of checks to perform (may be empty).
     * @param updateSession Function to be called to update session on successful function execution. Keep
     *                      session unchanged if absent.
     * @param s Session.
     * @param r Result of the operation.
     */
    def call[R](
        func: (R => Unit, Throwable => Unit) => Unit,
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
                    request,
                    session,
                    checks,
                    updateSession
                )
            },
            ex => {
                logger.debug(s"Exception in ignite actions, user id: #${session.userId}, request name: $request", ex)

                handleError(
                    ex,
                    startTime,
                    request,
                    session
                )
            }
        )
    }

    private def processResult[R](
        result: R,
        startTime: Long,
        finishTime: Long,
        request: String,
        session: Session,
        checks: Seq[Check[R]] = Seq.empty,
        updateSession: (Session, Option[R]) => Session
    ): Unit = {
        val (checkedSession, error) = Check.check(result, session, checks.toList)

        error match {
            case Some(Failure(errorMessage)) =>
                logAndExecuteNext(
                    updateSession(checkedSession.markAsFailed, Some(result)),
                    request,
                    startTime,
                    finishTime,
                    KO,
                    Some("Check ERROR"),
                    Some(errorMessage)
                )

            case None =>
                logAndExecuteNext(
                    updateSession(checkedSession, Some(result)),
                    request,
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
        request: String,
        session: Session
    ): Unit = {
        session(TransactionApiSessionKey).asOption[TransactionApi].foreach {
            _.close(_ => (), _ => ())
        }

        logAndExecuteNext(
            session.markAsFailed,
            request,
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

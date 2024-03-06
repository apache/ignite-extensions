/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.action.ignite

import com.sbt.ignite.gatling.action.IgniteAction
import com.sbt.ignite.gatling.api.IgniteApi
import com.sbt.ignite.gatling.api.TransactionApi
import com.sbt.ignite.gatling.builder.transaction.TransactionParameters
import com.sbt.ignite.gatling.protocol.IgniteProtocol.TransactionApiSessionKey
import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.transactions.TransactionConcurrency
import org.apache.ignite.transactions.TransactionIsolation

/**
 * Action for the transaction rollback Ignite operation.
 *
 * @param requestName Name of the request.
 * @param params Transaction parameters.
 * @param next Next action from chain to invoke upon this one completion.
 * @param ctx Scenario context.
 */
class TransactionStartAction(requestName: Expression[String], params: TransactionParameters, next: Action, ctx: ScenarioContext)
    extends IgniteAction("txStart", requestName, ctx, next) {

    override protected def execute(session: Session): Unit = withSessionCheck(session) {
        for {
            IgniteActionParameters(resolvedRequestName, igniteApi, _) <- resolveIgniteParameters(session)
        } yield {
            logger.debug(s"session user id: #${session.userId}, before $resolvedRequestName $actionType")

            val func = txStart(igniteApi, params.concurrency, params.isolation, params.timeout, params.size)

            call(
                func,
                actionType,
                session,
                updateSession = (session, transactionApi: Option[TransactionApi]) =>
                    transactionApi
                        .map(transactionApi => session.set(TransactionApiSessionKey, transactionApi))
                        .getOrElse(session)
            )
        }
    }

    private def txStart(
        igniteApi: IgniteApi,
        concurrency: Option[TransactionConcurrency],
        isolation: Option[TransactionIsolation],
        timeout: Option[Long],
        size: Option[Int]
    ): (TransactionApi => Unit, Throwable => Unit) => Unit =
        if (isolation.isDefined && concurrency.isDefined) {
            if (timeout.isDefined) {
                igniteApi.txStart(params.concurrency.get, params.isolation.get, timeout.get, size.getOrElse(0))
            } else {
                igniteApi.txStart(params.concurrency.get, params.isolation.get)
            }
        } else {
            igniteApi.txStart()
        }
}

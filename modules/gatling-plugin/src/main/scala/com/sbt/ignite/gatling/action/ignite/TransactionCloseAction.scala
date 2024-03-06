/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.action.ignite

import com.sbt.ignite.gatling.action.IgniteAction
import com.sbt.ignite.gatling.protocol.IgniteProtocol.TransactionApiSessionKey
import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext

/**
 * Action for the transaction close Ignite operation.
 *
 * @param requestName Name of the request.
 * @param next Next action from chain to invoke upon this one completion.
 * @param ctx Scenario context.
 */
class TransactionCloseAction(requestName: Expression[String], next: Action, ctx: ScenarioContext)
    extends IgniteAction("txClose", requestName, ctx, next) {

    override protected def execute(session: Session): Unit =
        for {
            IgniteActionParameters(resolvedRequestName, _, transactionApiOptional) <- resolveIgniteParameters(session)
        } yield {
            logger.debug(s"session user id: #${session.userId}, before $resolvedRequestName $actionType")

            transactionApiOptional.fold {
                next ! session
            } { transactionApi =>
                call(
                    transactionApi.close,
                    actionType,
                    session,
                    updateSession = (session, _: Option[Unit]) => session.remove(TransactionApiSessionKey)
                )
            }
        }
}

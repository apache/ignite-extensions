/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.action.ignite

import com.sbt.ignite.gatling.action.IgniteAction
import com.sbt.ignite.gatling.protocol.IgniteClientConfigurationCfg
import com.sbt.ignite.gatling.protocol.IgniteConfigurationCfg
import com.sbt.ignite.gatling.protocol.IgniteProtocol.IgniteApiSessionKey
import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext

/**
 * Action for the close Ignite API operation (either closes the thin client or stops the node working in client mode).
 *
 * @param requestName Name of the request.
 * @param next Next action from chain to invoke upon this one completion.
 * @param ctx Scenario context.
 */
class CloseClientAction(requestName: Expression[String], next: Action, ctx: ScenarioContext)
    extends IgniteAction("close", requestName, ctx, next) {

    override protected def execute(session: Session): Unit = withSessionCheck(session) {
        val noOp: (Unit => Unit, Throwable => Unit) => Unit = (s, _) => s.apply(())

        for {
            IgniteActionParameters(resolvedRequestName, igniteApi, _) <- resolveIgniteParameters(session)
        } yield {
            logger.debug(s"session user id: #${session.userId}, before $resolvedRequestName")

            val func = protocol.cfg match {
                case IgniteClientConfigurationCfg(_) | IgniteConfigurationCfg(_) if protocol.explicitClientStart =>
                    igniteApi.close() _

                case _ => noOp
            }

            call(func, resolvedRequestName, session, updateSession = (session, _: Option[Unit]) => session.remove(IgniteApiSessionKey))
        }
    }
}

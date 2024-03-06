/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.action.ignite

import com.sbt.ignite.gatling.action.IgniteAction
import com.sbt.ignite.gatling.api.IgniteApi
import com.sbt.ignite.gatling.protocol.IgniteProtocol.IgniteApiSessionKey
import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext

/**
 * Action for the start Ignite API operation (starts either the thin client or the node working in client mode).
 *
 * @param requestName Name of the request.
 * @param next Next action from chain to invoke upon this one completion.
 * @param ctx Scenario context.
 */
class StartClientAction(requestName: Expression[String], next: Action, ctx: ScenarioContext)
    extends IgniteAction("start", requestName, ctx, next) {

    override protected def execute(session: Session): Unit = withSessionCheck(session) {
        for {
            resolvedRequestName <- resolveRequestName(session)
        } yield {

            logger.debug(s"session user id: #${session.userId}, before $resolvedRequestName")

            val func = IgniteApi.start(protocol, session) _

            call(
                func,
                resolvedRequestName,
                session,
                updateSession = (session, igniteApi: Option[IgniteApi]) =>
                    igniteApi
                        .map(igniteApi => session.set(IgniteApiSessionKey, igniteApi))
                        .getOrElse(session)
            )
        }
    }
}

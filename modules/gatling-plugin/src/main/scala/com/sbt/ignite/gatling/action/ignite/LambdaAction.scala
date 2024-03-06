/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.action.ignite

import scala.util.Try

import com.sbt.ignite.gatling.action.IgniteAction
import com.sbt.ignite.gatling.api.IgniteApi
import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext

/**
 * Action executing the arbitrary lambda function accepting Ignite API instance and session.
 *
 * Lambda function may make changes in the session instance passed (say save some result into the session).
 *
 * To report the operation failure lambda function is assumed to throw the exception.
 *
 * @tparam I Ignite Api class.
 * @param requestName Name of the request.
 * @param function Lambda function to be executed.
 * @param next Next action from chain to invoke upon this one completion.
 * @param ctx Scenario context.
 */
class LambdaAction[I](requestName: Expression[String], function: (I, Session) => Any, next: Action, ctx: ScenarioContext)
    extends IgniteAction("lambda", requestName, ctx, next) {

    override protected def execute(session: Session): Unit = withSessionCheck(session) {
        for {
            IgniteActionParameters(resolvedRequestName, igniteApi, transactionApi) <- resolveIgniteParameters(session)
        } yield {
            val func = withCompletion(function, session, igniteApi) _

            call(func, resolvedRequestName, session)
        }
    }

    private def withCompletion(function: (I, Session) => Any, session: Session, igniteApi: IgniteApi)(
        s: Any => Unit,
        f: Throwable => Unit
    ): Unit = Try {
        function(igniteApi.wrapped, session)
    }.fold(f, s)
}

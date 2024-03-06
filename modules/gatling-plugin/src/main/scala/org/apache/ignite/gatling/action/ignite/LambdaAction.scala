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
package org.apache.ignite.gatling.action.ignite

import scala.util.Try

import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.gatling.action.IgniteAction
import org.apache.ignite.gatling.api.IgniteApi

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

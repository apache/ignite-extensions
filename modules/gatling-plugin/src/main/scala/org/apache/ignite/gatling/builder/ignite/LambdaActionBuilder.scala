/*
 * Copyright 2023 JSC SberTech
 */
package org.apache.ignite.gatling.builder.ignite

import org.apache.ignite.gatling.action.ignite.LambdaAction
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.EmptyStringExpressionSuccess
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext

/**
 * Lambda function action builder.
 *
 * @tparam I Ignite Api class.
 * @param function Lambda function to be executed.
 * @param requestName Request name.
 */
case class LambdaActionBuilder[I](
    function: (I, Session) => Any,
    requestName: Expression[String] = EmptyStringExpressionSuccess
) extends ActionBuilder {
    /**
     * Specify request name for action.
     *
     * @param requestName Request name.
     * @return itself.
     */
    def as(requestName: Expression[String]): ActionBuilder = this.copy(requestName = requestName)

    override def build(ctx: ScenarioContext, next: Action): Action =
        new LambdaAction(requestName, function, next, ctx)
}

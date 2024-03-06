/*
 * Copyright 2023 JSC SberTech
 */
package org.apache.ignite.gatling.builder.ignite

import org.apache.ignite.gatling.action.ignite.CloseClientAction
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.EmptyStringExpressionSuccess
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext

/**
 * Close Ignite API instance action builder.
 *
 * @param requestName Request name.
 */
case class CloseClientActionBuilder(requestName: Expression[String] = EmptyStringExpressionSuccess) extends ActionBuilder {
    /**
     * Specify request name for action.
     *
     * @param requestName Request name.
     * @return itself.
     */
    def as(requestName: Expression[String]): ActionBuilder = this.copy(requestName = requestName)

    /**
     * Builds an action.
     *
     * @param ctx The scenario context.
     * @param next The action that will be chained with the Action build by this builder.
     * @return The resulting action.
     */
    override def build(ctx: ScenarioContext, next: Action): Action =
        new CloseClientAction(requestName, next, ctx)
}

/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.action.ignite

import com.sbt.ignite.gatling.action.IgniteAction
import com.sbt.ignite.gatling.api.TransactionApi
import io.gatling.commons.validation.Failure
import io.gatling.commons.validation.Success
import io.gatling.commons.validation.Validation
import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext

/**
 * Action for the transaction rollback Ignite operation.
 *
 * @param requestName Name of the request.
 * @param next Next action from chain to invoke upon this one completion.
 * @param ctx Scenario context.
 */
class TransactionRollbackAction(requestName: Expression[String], next: Action, ctx: ScenarioContext)
    extends IgniteAction("rollback", requestName, ctx, next) {

    override protected def execute(session: Session): Unit = withSessionCheck(session) {
        for {
            IgniteActionParameters(resolvedRequestName, _, transactionApiOptional) <- resolveIgniteParameters(session)
            transactionApi <- transactionApiOptional.fold[Validation[TransactionApi]](Failure("no transaction found in session"))(t =>
                Success(t)
            )
        } yield {
            logger.debug(s"session user id: #${session.userId}, before $resolvedRequestName")

            call(
                transactionApi.rollback,
                resolvedRequestName,
                session
            )
        }
    }
}

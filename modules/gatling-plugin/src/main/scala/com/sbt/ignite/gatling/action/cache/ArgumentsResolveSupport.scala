/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.action.cache

import io.gatling.commons.validation.SuccessWrapper
import io.gatling.commons.validation.Validation
import io.gatling.core.session.Expression
import io.gatling.core.session.Session

/**
 * Helper to resolve arguments represented as a sequence of expressions.
 */
trait ArgumentsResolveSupport {
    /**
     * Resolves sequence of arguments using the session context.
     *
     * @param session Session.
     * @param arguments List of arguments to resolve.
     * @return List of the resolved arguments.
     */
    def resolveArguments(session: Session, arguments: Seq[Expression[Any]]): Validation[List[Any]] =
        arguments
            .foldLeft(List[Any]().success) { (acc, v) =>
                acc.flatMap(acc => v(session).map(v => v :: acc))
            }
            .map(l => l.reverse)
}

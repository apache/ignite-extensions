/*
 * Copyright 2023 JSC SberTech
 */
package org.apache.ignite.gatling.builder.cache

import java.util.concurrent.locks.Lock

import org.apache.ignite.gatling.action.cache.CacheUnlockAction
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext

/**
 * Cache entry release lock action builder.
 *
 * @param cacheName Cache name.
 * @param lock Lock instance.
 */
class CacheUnlockActionBuilder(
    cacheName: Expression[String],
    lock: Expression[Lock]
) extends ActionBuilder
    with CacheActionCommonParameters {

    /**
     * Builds an action.
     *
     * @param ctx The scenario context.
     * @param next The action that will be chained with the Action build by this builder.
     * @return The resulting action.
     */
    override def build(ctx: ScenarioContext, next: Action): Action =
        new CacheUnlockAction(requestName, cacheName, lock, keepBinary = withKeepBinary, next, ctx)
}

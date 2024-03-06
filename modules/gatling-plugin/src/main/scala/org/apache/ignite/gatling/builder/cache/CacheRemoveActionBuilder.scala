/*
 * Copyright 2023 JSC SberTech
 */
package org.apache.ignite.gatling.builder.cache

import org.apache.ignite.gatling.action.cache.CacheRemoveAction
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext

/**
 * Cache entry remove action builder.
 *
 * @tparam K Type of the cache key.
 * @param cacheName Cache name.
 * @param key The cache entry key.
 */
class CacheRemoveActionBuilder[K](
    cacheName: Expression[String],
    key: Expression[K]
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
        new CacheRemoveAction(requestName, cacheName, key, withKeepBinary, withAsync, next, ctx)
}

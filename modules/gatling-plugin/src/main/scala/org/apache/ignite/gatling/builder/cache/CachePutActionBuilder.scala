/*
 * Copyright 2023 JSC SberTech
 */
package org.apache.ignite.gatling.builder.cache

import org.apache.ignite.gatling.action.cache.CachePutAction
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext

/**
 * Cache entry put action builder.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @param cacheName Cache name.
 * @param pair The cache entry key.
 */
class CachePutActionBuilder[K, V](
    cacheName: Expression[String],
    pair: Expression[(K, V)]
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
        new CachePutAction(requestName, cacheName, pair, withKeepBinary, withAsync, next, ctx)
}

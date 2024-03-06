/*
 * Copyright 2023 JSC SberTech
 */
package org.apache.ignite.gatling.builder.cache

import org.apache.ignite.gatling.action.cache.CacheGetAndPutAction
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext

/**
 * Cache entry getAndPut action builder.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @param cacheName Cache name.
 * @param key The cache entry key.
 * @param value The cache entry value.
 */
class CacheGetAndPutActionBuilder[K, V](
    cacheName: Expression[String],
    key: Expression[K],
    value: Expression[V]
) extends ActionBuilder
    with CacheActionCommonParameters
    with CheckParameters[K, V] {

    /**
     * Builds an action.
     *
     * @param ctx The scenario context.
     * @param next The action that will be chained with the Action build by this builder.
     * @return The resulting action.
     */
    override def build(ctx: ScenarioContext, next: Action): Action =
        new CacheGetAndPutAction(requestName, cacheName, key, value, withKeepBinary, withAsync, checks, next, ctx)
}

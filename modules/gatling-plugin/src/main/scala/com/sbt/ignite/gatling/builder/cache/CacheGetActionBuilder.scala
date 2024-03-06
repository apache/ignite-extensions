/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.builder.cache

import com.sbt.ignite.gatling.action.cache.CacheGetAction
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext

/**
 * Cache entry get action builder.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @param cacheName Cache name.
 * @param key The cache entry key.
 */
class CacheGetActionBuilder[K, V](
    cacheName: Expression[String],
    key: Expression[K]
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
        new CacheGetAction(requestName, cacheName, key, withKeepBinary, withAsync, checks, next, ctx)
}
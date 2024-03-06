/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.builder.cache

import java.util.concurrent.locks.Lock

import com.sbt.ignite.gatling.action.cache.CacheLockAction
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext

/**
 * Cache entry obtain lock action builder.
 *
 * @tparam K Type of the cache key.
 * @param cacheName Cache name.
 * @param key The cache entry key.
 */
class CacheLockActionBuilder[K](
    cacheName: Expression[String],
    key: Expression[K]
) extends ActionBuilder
    with CacheActionCommonParameters
    with CheckParameters[K, Lock] {

    /**
     * Builds an action.
     *
     * @param ctx The scenario context.
     * @param next The action that will be chained with the Action build by this builder.
     * @return The resulting action.
     */
    override def build(ctx: ScenarioContext, next: Action): Action =
        new CacheLockAction(requestName, cacheName, key, keepBinary = withKeepBinary, checks, next, ctx)
}

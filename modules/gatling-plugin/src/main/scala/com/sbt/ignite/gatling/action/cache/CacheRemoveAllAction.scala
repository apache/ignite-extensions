/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.action.cache

import scala.collection.SortedSet

import com.sbt.ignite.gatling.action.CacheAction
import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext

/**
 * Action for the removeAll Ignite operation.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @param requestName Name of the request.
 * @param cacheName Name of cache.
 * @param keys Collection of cache entry keys.
 * @param keepBinary True if it should operate with binary objects.
 * @param async True if async API should be used.
 * @param next Next action from chain to invoke upon this one completion.
 * @param ctx Scenario context.
 */
class CacheRemoveAllAction[K, V](
    requestName: Expression[String],
    cacheName: Expression[String],
    keys: Expression[SortedSet[K]],
    keepBinary: Boolean,
    async: Boolean,
    next: Action,
    ctx: ScenarioContext
) extends CacheAction[K, V]("removeAll", requestName, ctx, next, cacheName, keepBinary, async) {

    override protected def execute(session: Session): Unit = withSessionCheck(session) {
        for {
            resolvedKeys <- keys(session)
            CacheActionParameters(resolvedRequestName, cacheApi, transactionApi) <- resolveCacheParameters(session)
        } yield {
            logger.debug(s"session user id: #${session.userId}, before $resolvedRequestName")

            val func = if (async) cacheApi.removeAllAsync _ else cacheApi.removeAll _

            call(func(resolvedKeys), resolvedRequestName, session)
        }
    }
}

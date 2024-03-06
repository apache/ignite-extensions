/*
 * Copyright 2023 JSC SberTech
 */
package org.apache.ignite.gatling.action.cache

import org.apache.ignite.gatling.Predef.IgniteCheck
import org.apache.ignite.gatling.action.CacheAction
import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext

/**
 * Action for the getAndRemove Ignite operation.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @param requestName Name of the request.
 * @param cacheName Name of cache.
 * @param key Cache entry key.
 * @param keepBinary True if it should operate with binary objects.
 * @param async True if async API should be used.
 * @param checks Collection of checks to perform against the operation result.
 * @param next Next action from chain to invoke upon this one completion.
 * @param ctx Scenario context.
 */
class CacheGetAndRemoveAction[K, V](
    requestName: Expression[String],
    cacheName: Expression[String],
    key: Expression[K],
    keepBinary: Boolean,
    async: Boolean,
    checks: Seq[IgniteCheck[K, V]],
    next: Action,
    ctx: ScenarioContext
) extends CacheAction[K, V]("getAndRemove", requestName, ctx, next, cacheName, keepBinary, async) {

    override protected def execute(session: Session): Unit = withSessionCheck(session) {
        for {
            CacheActionParameters(resolvedRequestName, cacheApi, transactionApi) <- resolveCacheParameters(session)
            resolvedKey <- key(session)
        } yield {
            logger.debug(s"session user id: #${session.userId}, before $resolvedRequestName")

            val func = if (async) cacheApi.getAndRemoveAsync _ else cacheApi.getAndRemove _

            call(func(resolvedKey), resolvedRequestName, session, checks)
        }
    }
}

/*
 * Copyright 2023 JSC SberTech
 */
package org.apache.ignite.gatling.action.cache

import java.util.concurrent.locks.Lock

import org.apache.ignite.gatling.action.CacheAction
import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext

/**
 * Action for the unlock cache entry Ignite operation.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @param requestName Name of the request.
 * @param cacheName Name of cache.
 * @param lock Lock object.
 * @param keepBinary True if it should operate with binary objects.
 * @param next Next action from chain to invoke upon this one completion.
 * @param ctx Scenario context.
 */
class CacheUnlockAction[K, V](
    requestName: Expression[String],
    cacheName: Expression[String],
    lock: Expression[Lock],
    keepBinary: Boolean,
    next: Action,
    ctx: ScenarioContext
) extends CacheAction[K, V]("unlock", requestName, ctx, next, cacheName, keepBinary) {

    override protected def execute(session: Session): Unit = withSessionCheck(session) {
        for {
            CacheActionParameters(resolvedRequestName, cacheApi, _) <- resolveCacheParameters(session)
            resolvedLock <- lock(session)
        } yield {
            logger.debug(s"session user id: #${session.userId}, before $resolvedRequestName")

            val func = cacheApi.unlock(resolvedLock) _

            call(func, resolvedRequestName, session)
        }
    }
}

/*
 * Copyright 2023 JSC SberTech
 */
package org.apache.ignite.gatling.action.cache

import java.util.concurrent.locks.Lock

import org.apache.ignite.gatling.Predef.IgniteCheck
import org.apache.ignite.gatling.action.CacheAction
import org.apache.ignite.gatling.protocol.IgniteProtocol.ExplicitLockWasUsedSessionKey
import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext

/**
 * Action for the lock cache entry Ignite operation.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @param requestName Name of the request.
 * @param cacheName Name of cache.
 * @param key Cache entry key.
 * @param keepBinary True if it should operate with binary objects.
 * @param checks Collection of checks to perform against the operation result.
 * @param next Next action from chain to invoke upon this one completion.
 * @param ctx Scenario context.
 */
class CacheLockAction[K, V](
    requestName: Expression[String],
    cacheName: Expression[String],
    key: Expression[K],
    keepBinary: Boolean,
    checks: Seq[IgniteCheck[K, Lock]],
    next: Action,
    ctx: ScenarioContext
) extends CacheAction[K, V]("lock", requestName, ctx, next, cacheName, keepBinary) {

    override protected def execute(session: Session): Unit = withSessionCheck(session) {
        for {
            CacheActionParameters(resolvedRequestName, cacheApi, _) <- resolveCacheParameters(session)
            resolvedKey <- key(session)
        } yield {
            logger.debug(s"session user id: #${session.userId}, before $resolvedRequestName")

            val func = (s: Map[K, Lock] => Unit, f: Throwable => Unit) =>
                cacheApi.lock(resolvedKey)(
                    v => s(Map(resolvedKey -> v)),
                    ex => f(ex)
                )

            call(
                func,
                resolvedRequestName,
                session,
                checks,
                updateSession = (session, _: Option[Map[K, Lock]]) => session.set(ExplicitLockWasUsedSessionKey, true)
            )
        }
    }
}

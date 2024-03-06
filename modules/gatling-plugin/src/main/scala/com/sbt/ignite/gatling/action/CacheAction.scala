/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.action

import com.sbt.ignite.gatling.api.CacheApi
import com.sbt.ignite.gatling.api.TransactionApi
import com.sbt.ignite.gatling.protocol.IgniteProtocol.ExplicitLockWasUsedSessionKey
import com.typesafe.scalalogging.StrictLogging
import io.gatling.commons.validation.Failure
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.commons.validation.Validation
import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext

/**
 * Base action class for cache operations.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @param actionType Action type name.
 * @param requestName Name of the request provided via the DSL. May be empty. If so name will be generated as specified
 *                    in the defaultRequestName implementation.
 * @param ctx Gatling scenario context.
 * @param next Next action to execute in scenario chain.
 * @param cacheName Name of cache.
 * @param keepBinary True if it should operate with binary objects.
 * @param async True if async API should be used.
 */
abstract class CacheAction[K, V](
    actionType: String,
    requestName: Expression[String],
    ctx: ScenarioContext,
    next: Action,
    val cacheName: Expression[String],
    val keepBinary: Boolean,
    val async: Boolean = false
) extends IgniteAction(actionType, requestName, ctx, next)
    with StrictLogging {

    override val defaultRequestName: Expression[String] =
        s => cacheName(s).map(cacheName => s"$actionType $cacheName")

    /**
     * Common parameters for cache actions.
     *
     * @param resolvedRequestName Name of request.
     * @param cacheApi Instance of CacheApi.
     * @param transactionApi Instance of TransactionApi.
     */
    case class CacheActionParameters(resolvedRequestName: String, cacheApi: CacheApi[K, V], transactionApi: Option[TransactionApi])

    /**
     * Resolves cache action parameters using session context.
     *
     * @param session Session.
     * @return Instance of CacheActionParameters
     */
    def resolveCacheParameters(session: Session): Validation[CacheActionParameters] =
        for {
            IgniteActionParameters(resolvedRequestName, igniteApi, transactionApi) <- resolveIgniteParameters(session)

            explicitLocksWereUsed <- session(ExplicitLockWasUsedSessionKey).asOption[Boolean].success

            resolvedCacheName <- cacheName(session)

            cacheApi <- {
                if (async && (explicitLocksWereUsed.nonEmpty || transactionApi.isDefined)) {
                    Failure("Async Ignite API can not be used in transaction context or along with the explicit locks.")
                } else {
                    if (keepBinary) {
                        igniteApi.cache[K, V](resolvedCacheName).map(cache => cache.withKeepBinary())
                    } else {
                        igniteApi.cache[K, V](resolvedCacheName)
                    }
                }
            }
        } yield CacheActionParameters(resolvedRequestName, cacheApi, transactionApi)
}

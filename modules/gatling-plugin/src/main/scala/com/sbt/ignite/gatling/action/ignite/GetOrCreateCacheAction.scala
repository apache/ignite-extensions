/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.action.ignite

import com.sbt.ignite.gatling.action.IgniteAction
import com.sbt.ignite.gatling.api.CacheApi
import com.sbt.ignite.gatling.api.IgniteApi
import com.sbt.ignite.gatling.builder.ignite.Configuration
import com.sbt.ignite.gatling.builder.ignite.SimpleCacheConfiguration
import com.sbt.ignite.gatling.builder.ignite.ThickConfiguration
import com.sbt.ignite.gatling.builder.ignite.ThinConfiguration
import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.configuration.CacheConfiguration

/**
 * Action for cache create Ignite operation.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @param requestName Name of the request.
 * @param cacheName Name of the cache.
 * @param config Abstract cache configuration.
 * @param next Next action from chain to invoke upon this one completion.
 * @param ctx Scenario context.
 */
class GetOrCreateCacheAction[K, V](
    requestName: Expression[String],
    cacheName: Expression[String],
    config: Configuration,
    next: Action,
    ctx: ScenarioContext
) extends IgniteAction("getOrCreateCache", requestName, ctx, next) {

    override val defaultRequestName: Expression[String] =
        s => cacheName(s).map(cacheName => s"$actionType $cacheName")

    override protected def execute(session: Session): Unit = withSessionCheck(session) {
        for {
            IgniteActionParameters(resolvedRequestName, igniteApi, _) <- resolveIgniteParameters(session)
            resolvedCacheName <- cacheName(session)
        } yield {
            logger.debug(s"session user id: #${session.userId}, before $resolvedRequestName")

            val func = getOrCreateCache(igniteApi, resolvedCacheName, config)

            call(func, resolvedRequestName, session)
        }
    }

    private def getOrCreateCache(
        igniteApi: IgniteApi,
        cacheName: String,
        config: Configuration
    ): (CacheApi[K, V] => Unit, Throwable => Unit) => Unit = config match {
        case cfg: SimpleCacheConfiguration => igniteApi.getOrCreateCacheBySimpleConfig(cacheName, cfg)

        case ThinConfiguration(cfg) =>
            if (cfg.getName == null) {
                cfg.setName(cacheName)
            }
            igniteApi.getOrCreateCacheByClientConfiguration(cfg)

        case ThickConfiguration(cfg) =>
            if (cfg.getName == null) {
                cfg.setName(cacheName)
            }
            igniteApi.getOrCreateCacheByConfiguration(cfg.asInstanceOf[CacheConfiguration[K, V]])
    }
}

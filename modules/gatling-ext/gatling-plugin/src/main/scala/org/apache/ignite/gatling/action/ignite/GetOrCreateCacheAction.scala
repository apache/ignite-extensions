/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.gatling.action.ignite

import io.gatling.core.action.Action
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.gatling.action.IgniteAction
import org.apache.ignite.gatling.api.CacheApi
import org.apache.ignite.gatling.api.IgniteApi
import org.apache.ignite.gatling.builder.ignite.Configuration
import org.apache.ignite.gatling.builder.ignite.SimpleCacheConfiguration
import org.apache.ignite.gatling.builder.ignite.ThickConfiguration
import org.apache.ignite.gatling.builder.ignite.ThinConfiguration

/**
 * Action for cache create Ignite operation.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @param requestName Name of the request provided via the DSL. May be empty.
 * @param cacheName Name of the cache.
 * @param config Abstract cache configuration.
 * @param next Next action from chain to invoke upon this one completion.
 * @param ctx Scenario context.
 */
class GetOrCreateCacheAction[K, V](
    requestName: String,
    cacheName: String,
    config: Configuration,
    next: Action,
    ctx: ScenarioContext
) extends IgniteAction("getOrCreateCache", requestName, ctx, next) {

    override val request: String = if (requestName == "") s"$name $cacheName" else requestName

    override protected def execute(session: Session): Unit = withSessionCheck(session) {
        for {
            IgniteActionParameters(igniteApi, _) <- resolveIgniteParameters(session)
        } yield {
            logger.debug(s"session user id: #${session.userId}, before $request")

            val func = getOrCreateCache(igniteApi, cacheName, config)

            call(func, session)
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

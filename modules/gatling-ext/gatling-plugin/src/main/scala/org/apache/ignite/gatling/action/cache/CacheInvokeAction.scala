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
package org.apache.ignite.gatling.action.cache

import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.cache.CacheEntryProcessor
import org.apache.ignite.gatling.Predef.IgniteCheck
import org.apache.ignite.gatling.action.CacheAction

/**
 * Action for the invoke Ignite operation.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @tparam T Type of the operation result.
 * @param requestName Name of the request.
 * @param cacheName Name of cache.
 * @param key Cache entry key.
 * @param entryProcessor Instance of CacheEntryProcessor.
 * @param arguments Additional arguments to pass to the entry processor.
 * @param keepBinary True if it should operate with binary objects.
 * @param async True if async API should be used.
 * @param checks Collection of checks to perform against the operation result.
 * @param next Next action from chain to invoke upon this one completion.
 * @param ctx Scenario context.
 */
class CacheInvokeAction[K, V, T](
    requestName: String,
    cacheName: String,
    key: Expression[K],
    entryProcessor: CacheEntryProcessor[K, V, T],
    arguments: Seq[Expression[Any]],
    keepBinary: Boolean,
    async: Boolean,
    checks: Seq[IgniteCheck[K, T]],
    next: Action,
    ctx: ScenarioContext
) extends CacheAction[K, V]("invoke", requestName, ctx, next, cacheName, keepBinary, async)
    with ArgumentsResolveSupport {

    override protected def execute(session: Session): Unit = withSessionCheck(session) {
        for {
            CacheActionParameters(cacheApi, _) <- resolveCacheParameters(session)

            resolvedKey <- key(session)

            resolvedArguments <- resolveArguments(session, arguments)
        } yield {
            logger.debug(s"session user id: #${session.userId}, before $request")

            val func = if (async) cacheApi.invokeAsync[T] _ else cacheApi.invoke[T] _

            call(func(resolvedKey, entryProcessor, resolvedArguments), session, checks)
        }
    }
}

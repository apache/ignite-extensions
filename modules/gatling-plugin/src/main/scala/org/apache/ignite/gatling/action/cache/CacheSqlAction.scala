/*
 * Copyright 2023 JSC SberTech
 */
package org.apache.ignite.gatling.action.cache

import org.apache.ignite.gatling.Predef.SqlCheck
import org.apache.ignite.gatling.action.CacheAction
import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.cache.query.SqlFieldsQuery

/**
 * Action for the SQL query Ignite operation.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @param requestName Name of the request.
 * @param cacheName Name of cache.
 * @param sql SQL query.
 * @param arguments Collection of positional arguments for SQL query (may be empty).
 * @param partitions List of cache partitions to use for query (may be empty).
 * @param keepBinary True if it should operate with binary objects.
 * @param checks Collection of checks to perform against the operation result.
 * @param next Next action from chain to invoke upon this one completion.
 * @param ctx Scenario context.
 */
class CacheSqlAction[K, V](
    requestName: Expression[String],
    cacheName: Expression[String],
    sql: Expression[String],
    arguments: Seq[Expression[Any]],
    partitions: Expression[List[Int]],
    keepBinary: Boolean,
    checks: Seq[SqlCheck],
    next: Action,
    ctx: ScenarioContext
) extends CacheAction[K, V]("sql", requestName, ctx, next, cacheName, keepBinary)
    with ArgumentsResolveSupport {

    override protected def execute(session: Session): Unit = withSessionCheck(session) {
        for {
            CacheActionParameters(resolvedRequestName, cacheApi, _) <- resolveCacheParameters(session)
            resolvedSql <- sql(session)
            resolvedArgs <- resolveArguments(session, arguments)
            resolvedPartitions <- partitions(session)
        } yield {
            logger.debug(s"session user id: #${session.userId}, before $resolvedRequestName")

            val query: SqlFieldsQuery = new SqlFieldsQuery(resolvedSql)

            if (resolvedArgs.nonEmpty) {
                query.setArgs(resolvedArgs: _*)
            }

            if (resolvedPartitions.nonEmpty) {
                query.setPartitions(resolvedPartitions: _*)
            }

            query.setSchema("PUBLIC")

            val func = cacheApi.sql(query) _

            call(func, resolvedRequestName, session, checks)
        }
    }
}

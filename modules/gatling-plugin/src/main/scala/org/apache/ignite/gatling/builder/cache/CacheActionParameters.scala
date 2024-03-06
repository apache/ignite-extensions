/*
 * Copyright 2023 JSC SberTech
 */
package org.apache.ignite.gatling.builder.cache

import org.apache.ignite.gatling.Predef.IgniteCheck
import io.gatling.core.session.EmptyStringExpressionSuccess
import io.gatling.core.session.Expression

/**
 * DSL operations for collection of common cache action parameters.
 */
trait CacheActionCommonParameters {
    /** Request name. */
    var requestName: Expression[String] = EmptyStringExpressionSuccess

    /** True if it should operate with binary objects. */
    var withKeepBinary: Boolean = false

    /** True if it should use async variant of API. */
    var withAsync: Boolean = false

    /**
     * Specify whether it should operate with binary objects.
     *
     * @return itself.
     */
    def keepBinary: this.type = {
        withKeepBinary = true
        this
    }

    /**
     * Specify whether it should use the async Ignite API.
     *
     * @return itself.
     */
    def async: this.type = {
        withAsync = true
        this
    }

    /**
     * Specify request name for action.
     *
     * @param newRequestName Request name.
     * @return itself.
     */
    def as(newRequestName: Expression[String]): this.type = {
        requestName = newRequestName
        this
    }
}

/**
 * DSL operations for collection of checks for cache action.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 */
trait CheckParameters[K, V] {
    /** Collection of check to be performed on the query result. */
    var checks: Seq[IgniteCheck[K, V]] = Seq.empty

    /**
     * Specify collection of check to be performed on the query result.
     *
     * @param newChecks collection of check to be performed on the query result.
     * @return itself.
     */
    def check(newChecks: IgniteCheck[K, V]*): this.type = {
        checks = newChecks
        this
    }
}

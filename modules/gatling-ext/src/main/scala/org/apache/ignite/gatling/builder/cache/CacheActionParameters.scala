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
package org.apache.ignite.gatling.builder.cache

import org.apache.ignite.gatling.Predef.IgniteCheck

/**
 * DSL operations for collection of common cache action parameters.
 */
trait CacheActionCommonParameters {
    /** Request name. */
    var requestName: String = ""

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
     * @param requestName Request name.
     * @return itself.
     */
    def as(requestName: String): this.type = {
        this.requestName = requestName

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

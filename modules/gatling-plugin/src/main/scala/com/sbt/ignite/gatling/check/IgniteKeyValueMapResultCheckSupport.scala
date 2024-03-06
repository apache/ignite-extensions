/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.check

import io.gatling.commons.validation.SuccessWrapper
import io.gatling.commons.validation.Validation
import io.gatling.core.check.CheckBuilder
import io.gatling.core.check.CheckMaterializer
import io.gatling.core.check.Extractor
import io.gatling.core.check.Preparer
import io.gatling.core.check.identityPreparer
import io.gatling.core.session.Expression
import io.gatling.core.session.ExpressionSuccessWrapper

/**
 * Support checks for the Ignite key-value operations results represented as a map.
 */
trait IgniteKeyValueMapResultCheckSupport extends IgniteKeyValueCheckSupport {
    /**
     * Check of Key-Value operation result represented as a map.
     */
    trait IgniteMapResultCheckType

    /**
     * Type of the Ignite key-value operation result.
     *
     * @tparam K Type of the cache key.
     * @tparam V Type of the operation result.
     */
    type MapResult[K, V] = RawResult[K, V]

    /**
     * Materializer for Ignite key-value result check.
     *
     * Does nothing in fact - just return the raw operation result.
     *
     * @tparam K Type of the cache key.
     * @tparam V Type of the operation result.
     */
    class MapResultCheckMaterializer[K, V]
        extends CheckMaterializer[IgniteMapResultCheckType, IgniteCheck[K, V], MapResult[K, V], MapResult[K, V]](identity) {
        /**
         * Transform the raw response into something that will be used as check input.
         * @return No-op preparer.
         */
        override protected def preparer: Preparer[MapResult[K, V], MapResult[K, V]] = identityPreparer
    }

    /**
     * @tparam K Type of the cache key.
     * @tparam V Type of the operation result.
     * @return Implicit materializer for Ignite key-value result check.
     */
    implicit def mapResultCheckMaterializer[K, V]: MapResultCheckMaterializer[K, V] = new MapResultCheckMaterializer[K, V]

    /**
     * mapResult extractor - does nothing - just return the prepared result as a whole.
     *
     * @tparam K Type of the cache key.
     * @tparam V Type of the operation result.
     * @return Extractor.
     */
    def mapResultExtractor[K, V]: Expression[Extractor[MapResult[K, V], MapResult[K, V]]] =
        new Extractor[MapResult[K, V], MapResult[K, V]] {
            override val name: String = "map"

            override def apply(prepared: MapResult[K, V]): Validation[Option[MapResult[K, V]]] = Some(prepared).success

            override val arity: String = "find"
        }.expressionSuccess

    /**
     * Builder for the Ignite key-value operations result check exposed as a `mapResult` DSL function.
     *
     * @tparam K Type of the cache key.
     * @tparam V Type of the operation result.
     * @return Check builder.
     */
    def mapResult[K, V]: CheckBuilder.Find.Default[IgniteMapResultCheckType, MapResult[K, V], MapResult[K, V]] =
        new CheckBuilder.Find.Default[IgniteMapResultCheckType, MapResult[K, V], MapResult[K, V]](
            mapResultExtractor,
            displayActualValue = true
        )
}

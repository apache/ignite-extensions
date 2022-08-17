package org.apache.ignite.spark.impl.optimization

package object accumulator {

    /**
     * Read spark context and return value of "spark.sql.caseSensitive" property
     * @param igniteQueryContext: IgniteQueryContext
     * @return value of "spark.sql.caseSensitive" config property
     */
    def isCaseSensitiveEnabled(igniteQueryContext: IgniteQueryContext): Boolean = {
        igniteQueryContext.sqlContext
            .getConf("spark.sql.caseSensitive", "false").toBoolean
    }
}

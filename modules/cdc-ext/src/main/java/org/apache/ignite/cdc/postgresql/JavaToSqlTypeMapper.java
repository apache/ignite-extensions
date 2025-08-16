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

package org.apache.ignite.cdc.postgresql;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;

/** */
class JavaToSqlTypeMapper {
    /** */
    private static final Map<String, JavaToSqlType> JAVA_TO_SQL_TYPE_MAP = new HashMap<>();

    static {
        for (JavaToSqlType type : JavaToSqlType.values())
            JAVA_TO_SQL_TYPE_MAP.put(type.javaTypeName(), type);
    }

    /**
     * Sets a value in the PreparedStatement at the given index using the appropriate setter
     * based on the runtime type of the object.
     * @param stmt {@link PreparedStatement}
     * @param idx value index in {@link PreparedStatement}
     * @param obj value
     */
    public void setValue(PreparedStatement stmt, int idx, Object obj) {
        try {
            assert obj != null;

            if (obj instanceof byte[]) {
                stmt.setBytes(idx, (byte[])obj); // Preferred setter for byte[]

                return;
            }

            JavaToSqlType type = JAVA_TO_SQL_TYPE_MAP.get(obj.getClass().getName());

            if (type != null)
                stmt.setObject(idx, obj, type.typeId());
            else
                throw new IgniteException("Java-to-SQL type mapping is not defined for class: "
                    + obj.getClass().getName());
        }
        catch (Throwable e) {
            throw new IgniteException("Failed to set value to statement [stmt=" + stmt + ", valueType=" +
                (obj == null ? "null" : obj.getClass().getName()) + ", index=" + idx + ']', e);
        }
    }

    /**
     * Renders the SQL type declaration for a given Java class name based on its mapping,
     * optionally including precision and scale if the SQL type supports them.
     *
     * @param clsName  the fully qualified Java class name used to look up the corresponding SQL type
     * @param precision optional precision value to include in the SQL type, if supported
     * @param scale     optional scale value to include in the SQL type, if supported
     * @return the SQL type string with appropriate precision and scale formatting
     */
    public String renderSqlType(String clsName, Integer precision, Integer scale) {
        JavaToSqlType type = JAVA_TO_SQL_TYPE_MAP.get(clsName);

        if (type == null)
            throw new IgniteException("Java-to-SQL type mapping is not defined for class: " + clsName);

        String sqlType = type.sqlType();

        if (type.precision() && precision != null) {
            return type.scale() && scale != null
                ? String.format("%s(%d, %d)", sqlType, precision, scale)
                : String.format("%s(%d)", sqlType, precision);
        }

        return sqlType;
    }

    /** */
    enum JavaToSqlType {
        /** */
        STRING(String.class, "VARCHAR", true, false, Types.VARCHAR),

        /** */
        INTEGER(Integer.class, "INT", false, false, Types.INTEGER),

        /** */
        LONG(Long.class, "BIGINT", false, false, Types.BIGINT),

        /** */
        BOOLEAN(Boolean.class, "BOOL", false, false, Types.BOOLEAN),

        /** */
        DOUBLE(Double.class, "NUMERIC", true, true, Types.DOUBLE),

        /** */
        FLOAT(Float.class, "NUMERIC", true, true, Types.FLOAT),

        /** */
        BIG_DECIMAL(BigDecimal.class, "NUMERIC", true, true, Types.DECIMAL),

        /** */
        SHORT(Short.class, "SMALLINT", false, false, Types.SMALLINT),

        /** */
        BYTE(Byte.class, "SMALLINT", false, false, Types.SMALLINT),

        /** */
        SQL_DATE(java.sql.Date.class, "DATE", false, false, Types.DATE),

        /** */
        SQL_TIME(java.sql.Time.class, "TIME", true, false, Types.TIME),

        /** */
        SQL_TIMESTAMP(java.sql.Timestamp.class, "TIMESTAMP", true, false, Types.TIMESTAMP),

        /** */
        UTIL_DATE(java.util.Date.class, "TIMESTAMP", true, false, Types.TIMESTAMP),

        /** */
        UUID_TYPE(UUID.class, "UUID", false, false, Types.OTHER),

        /** */
        LOCAL_DATE(LocalDate.class, "DATE", false, false, Types.DATE),

        /** */
        LOCAL_TIME(LocalTime.class, "TIME", true, false, Types.TIME),

        /** */
        LOCAL_DATE_TIME(LocalDateTime.class, "TIMESTAMP", true, false, Types.TIMESTAMP),

        /** */
        OFFSET_TIME(OffsetTime.class, "VARCHAR", true, false, Types.VARCHAR),

        /** */
        OFFSET_DATE_TIME(OffsetDateTime.class, "TIMESTAMP WITH TIME ZONE", false, false,
            Types.TIMESTAMP_WITH_TIMEZONE),

        /** */
        BYTE_ARRAY(byte[].class, "BYTEA", false, false, Types.OTHER);

        /** */
        private final String javaTypeName;

        /** */
        private final String sqlType;

        /** */
        private final boolean precision;

        /** */
        private final boolean scale;

        /** */
        private final int typeId;

        /**
         * @param javaTypeCls Java type class.
         * @param sqlType Sql type.
         * @param precision Has precision.
         * @param scale Has scale.
         * @param typeId {@link Types}
         */
        JavaToSqlType(
            Class<?> javaTypeCls,
            String sqlType,
            boolean precision,
            boolean scale,
            int typeId
        ) {
            this.javaTypeName = javaTypeCls.getName();
            this.sqlType = sqlType;
            this.precision = precision;
            this.scale = scale;
            this.typeId = typeId;
        }

        /** */
        String javaTypeName() {
            return javaTypeName;
        }

        /** */
        String sqlType() {
            return sqlType;
        }

        /** */
        boolean precision() {
            return precision;
        }

        /** */
        boolean scale() {
            return scale;
        }

        /** */
        int typeId() {
            return typeId;
        }
    }
}


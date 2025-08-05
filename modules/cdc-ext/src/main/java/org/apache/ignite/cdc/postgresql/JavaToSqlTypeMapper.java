package org.apache.ignite.cdc.postgresql;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.lang.RunnableX;

/** */
public class JavaToSqlTypeMapper {
    /** */
    private static final int NO_SQL_TYPE = -1;

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
    public void setEventFieldValue(PreparedStatement stmt, Integer idx, Object obj) {
        if (obj == null) {
            setSafe(() -> stmt.setNull(idx, Types.NULL));

            return;
        }

        int types = JAVA_TO_SQL_TYPE_MAP.get(obj.getClass().getName()).types();

        if (types != -1)
            setSafe(() -> stmt.setObject(idx, obj, types));
        else if (obj instanceof Duration) {
            Duration dur = (Duration)obj;

            BigDecimal durVal = BigDecimal.valueOf(dur.getSeconds()).add(BigDecimal.valueOf(dur.getNano(), 9));

            setSafe(() -> stmt.setBigDecimal(idx, durVal));
        }
        else if (obj instanceof byte[])
            setSafe(() -> stmt.setBytes(idx, (byte[])obj));
        else
            setSafe(() -> stmt.setObject(idx, obj));
    }

    /**
     * Renders the SQL type for a given Java class name, including both precision and scale if supported.
     *
     * @param clsName  The fully qualified Java class name (e.g., {@code java.math.BigDecimal}).
     * @param precision The numeric precision to include in the SQL type declaration.
     * @param scale     The numeric scale to include in the SQL type declaration.
     * @return A SQL type string (e.g., {@code DECIMAL (10, 2)}) corresponding to the given class and numeric metadata.
     *         If the SQL type does not support scale, {@link #renderSqlType(String, int)} is used instead.
     */
    public String renderSqlType(String clsName, int precision, int scale) {
        JavaToSqlType type = JAVA_TO_SQL_TYPE_MAP.get(clsName);

        if (!type.scale())
            return renderSqlType(clsName, precision);

        return type.sqlType().replace("?", String.format("(%d, %d)", precision, scale));
    }

    /**
     * Renders the SQL type for a given Java class name, including precision if supported.
     *
     * @param clsName   The fully qualified Java class name (e.g., {@code java.lang.String}).
     * @param precision The numeric precision or length to include in the SQL type declaration.
     * @return A SQL type string (e.g., {@code VARCHAR (255)}) corresponding to the given class and precision.
     *         If the SQL type does not support precision, {@link #renderSqlType(String)} is used instead.
     */
    public String renderSqlType(String clsName, int precision) {
        JavaToSqlType type = JAVA_TO_SQL_TYPE_MAP.get(clsName);

        if (!type.precision())
            return renderSqlType(clsName);

        return type.sqlType().replace("?", String.format("(%d)", precision));
    }

    /**
     * Renders the SQL type for a given Java class name without any precision or scale.
     *
     * @param clsName The fully qualified Java class name (e.g., {@code java.lang.Integer}).
     * @return A SQL type string (e.g., {@code INTEGER}, {@code VARCHAR}, or {@code DECIMAL}) with or without
     * placeholders removed. If the mapped SQL type includes a precision placeholder, it will be removed.
     */
    public String renderSqlType(String clsName) {
        JavaToSqlType type = JAVA_TO_SQL_TYPE_MAP.get(clsName);

        if (type.precision())
            return type.sqlType().replace("?", "");

        return type.sqlType();
    }

    /**
     * Executes the given operation that may throw an exception, converting any thrown {@link Throwable}
     * into an {@link IgniteException}.
     *
     * @param op the operation to execute, represented as a {@link RunnableX}.
     * @throws IgniteException if the operation throws any exception.
     */
    private void setSafe(RunnableX op) {
        try {
            op.runx();
        }
        catch (Throwable e) {
            throw new IgniteException("Failed to set value for type!", e);
        }
    }

    /** */
    enum JavaToSqlType {
        /** */
        STRING(String.class, "VARCHAR?", true, false, Types.VARCHAR),

        /** */
        INTEGER(Integer.class, "INT", false, false, Types.INTEGER),

        /** */
        LONG(Long.class, "BIGINT", false, false, Types.BIGINT),

        /** */
        BOOLEAN(Boolean.class, "BOOL", false, false, Types.BOOLEAN),

        /** */
        DOUBLE(Double.class, "NUMERIC?", true, true, Types.DOUBLE),

        /** */
        FLOAT(Float.class, "NUMERIC?", true, true, Types.FLOAT),

        /** */
        BIG_DECIMAL(BigDecimal.class, "NUMERIC?", true, true, Types.DECIMAL),

        /** */
        SHORT(Short.class, "SMALLINT", false, false, Types.SMALLINT),

        /** */
        BYTE(Byte.class, "SMALLINT", false, false, Types.SMALLINT),

        /** */
        SQL_DATE(java.sql.Date.class, "DATE", false, false, Types.DATE),

        /** */
        SQL_TIME(java.sql.Time.class, "TIME?", true, false, Types.TIME),

        /** */
        SQL_TIMESTAMP(java.sql.Timestamp.class, "TIMESTAMP?", true, false, Types.TIMESTAMP),

        /** */
        UTIL_DATE(java.util.Date.class, "TIMESTAMP?", true, false, Types.TIMESTAMP),

        /** */
        UUID_TYPE(UUID.class, "UUID", false, false, Types.OTHER),

        /** */
        PERIOD(Period.class, "INTERVAL", false, false, Types.OTHER),

        /** */
        DURATION(Duration.class, "NUMERIC", false, false, NO_SQL_TYPE),

        /** */
        LOCAL_DATE(LocalDate.class, "DATE", false, false, Types.DATE),

        /** */
        LOCAL_TIME(LocalTime.class, "TIME?", true, false, Types.TIME),

        /** */
        LOCAL_DATE_TIME(LocalDateTime.class, "TIMESTAMP?", true, false, Types.TIMESTAMP),

        /** */
        OFFSET_TIME(OffsetTime.class, "VARCHAR?", true, false, Types.VARCHAR),

        /** */
        OFFSET_DATE_TIME(OffsetDateTime.class, "TIMESTAMP WITH TIME ZONE", false, false,
            Types.TIMESTAMP_WITH_TIMEZONE),

        /** */
        BYTE_ARRAY(byte[].class, "BYTEA", false, false, NO_SQL_TYPE),

        /** */
        OBJECT(Object.class, "OTHER", false, false, NO_SQL_TYPE);

        /** */
        private final String javaTypeName;

        /** */
        private final String sqlType;

        /** */
        private final boolean precision;

        /** */
        private final boolean scale;

        /** */
        private final int types;

        /**
         * @param javaTypeName Java type name.
         * @param sqlType Sql type.
         * @param precision Has precision.
         * @param scale Has scale.
         * @param types {@link Types}
         */
        JavaToSqlType(
            Class<?> javaTypeName,
            String sqlType,
            boolean precision,
            boolean scale,
            int types
        ) {
            this.javaTypeName = javaTypeName.getName();
            this.sqlType = sqlType;
            this.precision = precision;
            this.scale = scale;
            this.types = types;
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
        int types() {
            return types;
        }
    }
}


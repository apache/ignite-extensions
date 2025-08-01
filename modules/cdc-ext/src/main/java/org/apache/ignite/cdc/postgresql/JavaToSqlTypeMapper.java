package org.apache.ignite.cdc.postgresql;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
import org.apache.logging.log4j.util.TriConsumer;

/** */
public class JavaToSqlTypeMapper {
    /**
     * Sets a value in the PreparedStatement at the given index using the appropriate setter
     * based on the runtime type of the object.
     * @param stmt {@link PreparedStatement}
     * @param idx value index in {@link PreparedStatement}
     * @param obj value
     */
    public static void setEventFieldValue(PreparedStatement stmt, Integer idx, Object obj) {
        if (obj == null) {
            setSafe(() -> stmt.setNull(idx, Types.NULL));

            return;
        }

        JavaToSqlType.get(obj.getClass().getName()).setter().accept(stmt, idx, obj);
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
    public static String renderSqlType(String clsName, int precision, int scale) {
        JavaToSqlType type = JavaToSqlType.get(clsName);

        if (!type.hasScale())
            return renderSqlType(clsName, precision);

        return type.getSqlType().replace("?", String.format("(%d, %d)", precision, scale));
    }

    /**
     * Renders the SQL type for a given Java class name, including precision if supported.
     *
     * @param clsName   The fully qualified Java class name (e.g., {@code java.lang.String}).
     * @param precision The numeric precision or length to include in the SQL type declaration.
     * @return A SQL type string (e.g., {@code VARCHAR (255)}) corresponding to the given class and precision.
     *         If the SQL type does not support precision, {@link #renderSqlType(String)} is used instead.
     */
    public static String renderSqlType(String clsName, int precision) {
        JavaToSqlType type = JavaToSqlType.get(clsName);

        if (!type.hasPrecision())
            return renderSqlType(clsName);

        return type.getSqlType().replace("?", String.format("(%d)", precision));
    }

    /**
     * Renders the SQL type for a given Java class name without any precision or scale.
     *
     * @param clsName The fully qualified Java class name (e.g., {@code java.lang.Integer}).
     * @return A SQL type string (e.g., {@code INTEGER}, {@code VARCHAR}, or {@code DECIMAL}) with or without
     * placeholders removed. If the mapped SQL type includes a precision placeholder, it will be removed.
     */
    public static String renderSqlType(String clsName) {
        JavaToSqlType type = JavaToSqlType.get(clsName);

        if (type.hasPrecision())
            return type.getSqlType().replace("?", "");

        return type.getSqlType();
    }

    /**
     * Creates a {@link TriConsumer} that safely sets an object into the given {@link PreparedStatement}
     * using the specified SQL type from {@link java.sql.Types}.
     *
     * <p>This method is useful when the SQL type must be explicitly specified during the binding,
     * such as {@code Types.DATE}, {@code Types.VARCHAR}, {@code Types.OTHER}, etc.</p>
     *
     * @param type the SQL type from {@link java.sql.Types} to be used when setting the object.
     * @return a {@link TriConsumer} that accepts a {@link PreparedStatement}, a parameter index, and an object to set.
     * @throws IgniteException if a {@link SQLException} occurs during the parameter setting.
     */
    public static TriConsumer<PreparedStatement, Integer, Object> setObjectWithTypes(int type) {
        return (stmt, idx, obj) -> setSafe(() -> stmt.setObject(idx, obj, type));
    }

    /**
     * Safely sets a {@link Duration} value into the given {@link PreparedStatement}.
     *
     * @param stmt the {@link PreparedStatement} in which the value will be set.
     * @param idx the index of the parameter to set, starting at 1.
     * @param obj the value to be set; must be a {@link Duration}.
     * @throws IgniteException if a {@link SQLException} occurs during the parameter setting.
     */
    public static void setDuration(PreparedStatement stmt, int idx, Object obj) {
        Duration dur = (Duration)obj;

        setSafe(() -> stmt.setBigDecimal(idx, toBigDecimal(dur)));
    }

    /**
     * Converts a {@link java.time.Duration} instance to a {@link java.math.BigDecimal}.
     * The resulting {@code BigDecimal} represents the total duration in seconds,
     * including the fractional part derived from the nanoseconds component.
     *
     * @param dur the {@link Duration} to convert; must not be null
     * @return a {@link BigDecimal} representing the duration in seconds with nanosecond precision
     * @throws NullPointerException if {@code dur} is null
     */
    public static BigDecimal toBigDecimal(Duration dur) {
        return BigDecimal.valueOf(dur.getSeconds()).add(BigDecimal.valueOf(dur.getNano(), 9));
    }

    /**
     * Safely sets a {@code byte[]} value into the given {@link PreparedStatement}.
     *
     * @param stmt the {@link PreparedStatement} in which the value will be set.
     * @param idx the index of the parameter to set, starting at 1.
     * @param obj the value to be set; must be a {@code byte[]}.
     * @throws IgniteException if a {@link SQLException} occurs during the parameter setting.
     */
    public static void setByteArray(PreparedStatement stmt, int idx, Object obj) {
        setSafe(() -> stmt.setBytes(idx, (byte[])obj));
    }

    /**
     * Safely sets a {@link Object} value into the given {@link PreparedStatement}.
     *
     * @param stmt the {@link PreparedStatement} in which the value will be set.
     * @param idx the index of the parameter to set, starting at 1.
     * @param obj the value to be set; must be a {@link Object}.
     * @throws IgniteException if a {@link SQLException} occurs during the parameter setting.
     */
    public static void setObject(PreparedStatement stmt, int idx, Object obj) {
        setSafe(() -> stmt.setObject(idx, obj));
    }

    /**
     * Executes the given operation that may throw an exception, converting any thrown {@link Throwable}
     * into an {@link IgniteException}.
     *
     * @param op the operation to execute, represented as a {@link RunnableX}.
     * @throws IgniteException if the operation throws any exception.
     */
    private static void setSafe(RunnableX op) {
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
        STRING(String.class, "VARCHAR?", true, false, setObjectWithTypes(Types.VARCHAR)),

        /** */
        INTEGER(Integer.class, "INT", false, false, setObjectWithTypes(Types.INTEGER)),

        /** */
        LONG(Long.class, "BIGINT", false, false, setObjectWithTypes(Types.BIGINT)),

        /** */
        BOOLEAN(Boolean.class, "BOOL", false, false, setObjectWithTypes(Types.BOOLEAN)),

        /** */
        DOUBLE(Double.class, "NUMERIC?", true, true, setObjectWithTypes(Types.DOUBLE)),

        /** */
        FLOAT(Float.class, "NUMERIC?", true, true, setObjectWithTypes(Types.FLOAT)),

        /** */
        BIG_DECIMAL(BigDecimal.class, "NUMERIC?", true, true, setObjectWithTypes(Types.DECIMAL)),

        /** */
        SHORT(Short.class, "SMALLINT", false, false, setObjectWithTypes(Types.SMALLINT)),

        /** */
        BYTE(Byte.class, "SMALLINT", false, false, setObjectWithTypes(Types.SMALLINT)),

        /** */
        SQL_DATE(java.sql.Date.class, "DATE", false, false, setObjectWithTypes(Types.DATE)),

        /** */
        SQL_TIME(java.sql.Time.class, "TIME?", true, false, setObjectWithTypes(Types.TIME)),

        /** */
        SQL_TIMESTAMP(java.sql.Timestamp.class, "TIMESTAMP?", true, false, setObjectWithTypes(Types.TIMESTAMP)),

        /** */
        UTIL_DATE(java.util.Date.class, "TIMESTAMP?", true, false, setObjectWithTypes(Types.TIMESTAMP)),

        /** */
        UUID_TYPE(UUID.class, "UUID", false, false, setObjectWithTypes(Types.OTHER)),

        /** */
        PERIOD(Period.class, "INTERVAL", false, false, setObjectWithTypes(Types.OTHER)),

        /** */
        DURATION(Duration.class, "NUMERIC", false, false, JavaToSqlTypeMapper::setDuration),

        /** */
        LOCAL_DATE(LocalDate.class, "DATE", false, false, setObjectWithTypes(Types.DATE)),

        /** */
        LOCAL_TIME(LocalTime.class, "TIME?", true, false, setObjectWithTypes(Types.TIME)),

        /** */
        LOCAL_DATE_TIME(LocalDateTime.class, "TIMESTAMP?", true, false, setObjectWithTypes(Types.TIMESTAMP)),

        /** */
        OFFSET_TIME(OffsetTime.class, "VARCHAR?", true, false, setObjectWithTypes(Types.VARCHAR)),

        /** */
        OFFSET_DATE_TIME(OffsetDateTime.class, "TIMESTAMP WITH TIME ZONE", false, false,
            setObjectWithTypes(Types.TIMESTAMP_WITH_TIMEZONE)),

        /** */
        BYTE_ARRAY(byte[].class, "BYTEA", false, false, JavaToSqlTypeMapper::setByteArray),

        /** */
        OBJECT(Object.class, "OTHER", false, false, JavaToSqlTypeMapper::setObject);

        /** */
        private static final Map<String, JavaToSqlType> LOOKUP = new HashMap<>();

        static {
            for (JavaToSqlType type : values())
                LOOKUP.put(type.getJavaTypeName(), type);
        }

        /** */
        private final String javaTypeName;

        /** */
        private final String sqlType;

        /** */
        private final boolean hasPrecision;

        /** */
        private final boolean hasScale;

        /** */
        private final TriConsumer<PreparedStatement, Integer, Object> setter;

        /**
         * @param javaType Java type.
         * @param sqlType Sql type.
         * @param hasPrecision Has precision.
         * @param hasScale Has scale.
         */
        JavaToSqlType(
            Class<?> javaType,
            String sqlType,
            boolean hasPrecision,
            boolean hasScale,
            TriConsumer<PreparedStatement, Integer, Object> setter
        ) {
            this.javaTypeName = javaType.getName();
            this.sqlType = sqlType;
            this.hasPrecision = hasPrecision;
            this.hasScale = hasScale;
            this.setter = setter;
        }

        /** */
        String getJavaTypeName() {
            return javaTypeName;
        }

        /** */
        String getSqlType() {
            return sqlType;
        }

        /** */
        boolean hasPrecision() {
            return hasPrecision;
        }

        /** */
        boolean hasScale() {
            return hasScale;
        }

        /** */
        TriConsumer<PreparedStatement, Integer, Object> setter() {
            return setter;
        }

        /** */
        static JavaToSqlType get(String clsName) {
            return LOOKUP.getOrDefault(clsName, OBJECT);
        }
    }
}


package org.chdb.jdbc;

import java.sql.*;

/**
 * jdbc param
 *
 * @param sqlType    sql type from Types.xxx
 * @param valueClass value class
 * @param value      value
 */
public record JdbcParam(int sqlType, Class<?> valueClass, Object value) {

    public String getLiteralValue() {
        if (value == null) {
            return "NULL";
        } else if (sqlType == Types.DOUBLE
                || sqlType == Types.INTEGER
                || sqlType == Types.BIGINT
                || sqlType == Types.DECIMAL
                || sqlType == Types.FLOAT
                || sqlType == Types.SMALLINT
        ) {
            return value.toString();
        } else if (sqlType == Types.DATE || sqlType == Types.TIME || sqlType == Types.TIMESTAMP) {
            return "'" + value + "'";
        } else {
            return escapeSQL(value.toString());
        }
    }

    public String escapeSQL(String value) {
        if (value.contains("'")) {
            return "'" + value.replace("'", "''") + "'";
        } else {
            return "'" + value + "'";
        }
    }
}
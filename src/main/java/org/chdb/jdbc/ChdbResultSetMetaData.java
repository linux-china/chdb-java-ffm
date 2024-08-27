package org.chdb.jdbc;

import java.sql.*;

public class ChdbResultSetMetaData implements ResultSetMetaData {
    public String[] cols;
    public String[] colsMeta;

    public ChdbResultSetMetaData(String[] cols, String[] colsMeta) {
        this.cols = cols;
        this.colsMeta = colsMeta;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return cols.length;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return 0;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return cols[column - 1];
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return cols[column - 1];
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        // TODO add more types
        String typeName = colsMeta[column - 1].toLowerCase();
        if (typeName.contains("(")) {
            typeName = typeName.substring(typeName.indexOf("("), typeName.length() - 2);
        }
        if (typeName.startsWith("string")) {
            return Types.VARCHAR;
        } else if (typeName.startsWith("int64")) {
            return Types.BIGINT;
        } else if (typeName.contains("int")) {
            return Types.INTEGER;
        } else if (typeName.startsWith("datetime")) {
            return Types.TIMESTAMP;
        } else if (typeName.startsWith("date")) {
            return Types.DATE;
        } else if (typeName.startsWith("time")) {
            return Types.TIME;
        } else if (typeName.startsWith("decimal")) {
            return Types.DECIMAL;
        } else if (typeName.startsWith("float")) {
            return Types.DOUBLE;
        }
        return Types.VARCHAR;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return colsMeta[column - 1];
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        int columnType = getColumnType(column);
        return switch (columnType) {
            case Types.BIGINT -> "java.lang.Long";
            case Types.INTEGER -> "java.lang.Integer";
            case Types.DOUBLE -> "java.lang.Double";
            case Types.FLOAT -> "java.lang.Float";
            case Types.VARCHAR -> "java.lang.String";
            default -> "java.lang.Object";
        };
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws ClassCastException {
        return iface.cast(this);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

}

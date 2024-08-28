package org.chdb.jdbc;

import org.chdb.result.*;

import java.io.*;
import java.math.*;
import java.net.*;
import java.sql.*;
import java.sql.Date;
import java.time.*;
import java.util.*;


public class ChdbResultSet implements ResultSet {
    private final Statement statement;
    private final String sql;
    private final String[] cols;
    private final String[] colsMeta;
    private final List<Map<String, Object>> rows;
    private final int maxRows;
    private int row = -1;

    public ChdbResultSet(Statement statement, String sql, QueryJsonResult queryResult) {
        this.statement = statement;
        this.sql = sql;
        this.rows = queryResult.data();
        this.maxRows = this.rows.size();
        List<ColumnInfo> columnInfoList = queryResult.meta();
        int columnNum = columnInfoList.size();
        cols = new String[columnNum];
        colsMeta = new String[columnNum];
        for (int i = 0; i < columnNum; i++) {
            cols[i] = columnInfoList.get(i).name();
            colsMeta[i] = columnInfoList.get(i).type();
        }
    }

    public Map<String, Object> getCurrentRow() {
        return rows.get(row);
    }

    public String getSql() {
        return sql;
    }

    @Override
    public boolean next() throws SQLException {
        row = row + 1;
        return row < maxRows;
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public boolean wasNull() throws SQLException {
        String columName = cols[cols.length - 1];
        return getCurrentRow().containsKey(columName);
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return getString(cols[columnIndex - 1]);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return getBoolean(cols[columnIndex - 1]);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return getByte(cols[columnIndex - 1]);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return getShort(cols[columnIndex - 1]);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return getInt(cols[columnIndex - 1]);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return getLong(cols[columnIndex - 1]);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return getFloat(cols[columnIndex - 1]);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return getDouble(cols[columnIndex - 1]);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getBigDecimal(cols[columnIndex - 1]);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return new byte[0];
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return getDate(cols[columnIndex - 1]);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return getTime(cols[columnIndex - 1]);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getTimestamp(cols[columnIndex - 1]);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        Object object = getCurrentRow().get(columnLabel);
        if (object == null) {
            return null;
        } else if (object instanceof String) {
            return (String) object;
        } else {
            return object.toString();
        }
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        Object object = getCurrentRow().get(columnLabel);
        return switch (object) {
            case null -> false;
            case Boolean b -> b;
            case Integer i -> i == 0;
            case Long l -> l == 0;
            case Double d -> d == 0;
            default -> Boolean.parseBoolean(object.toString());
        };
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        Object object = getCurrentRow().get(columnLabel);
        return switch (object) {
            case null -> 0;
            case Integer i -> i.byteValue();
            case Long l -> l.byteValue();
            case Double d -> d.byteValue();
            default -> Byte.parseByte(object.toString());
        };
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        Object object = getCurrentRow().get(columnLabel);
        return switch (object) {
            case null -> 0;
            case Integer i -> i.shortValue();
            case Long l -> l.shortValue();
            case Double d -> d.shortValue();
            default -> Short.parseShort(object.toString());
        };
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        Object object = getCurrentRow().get(columnLabel);
        return switch (object) {
            case null -> 0;
            case Integer i -> i;
            case Long l -> l.intValue();
            case Double d -> d.intValue();
            default -> Integer.parseInt(object.toString());
        };
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        Object object = getCurrentRow().get(columnLabel);
        return switch (object) {
            case null -> 0;
            case Integer i -> i;
            case Long l -> l;
            case Double d -> d.longValue();
            default -> Long.parseLong(object.toString());
        };
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        Object object = getCurrentRow().get(columnLabel);
        return switch (object) {
            case null -> 0;
            case Integer i -> i;
            case Long l -> l;
            case Double d -> d.floatValue();
            default -> Float.parseFloat(object.toString());
        };
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        Object object = getCurrentRow().get(columnLabel);
        return switch (object) {
            case null -> 0;
            case Integer i -> i;
            case Long l -> l;
            case Double d -> d;
            default -> Double.parseDouble(object.toString());
        };
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(columnLabel).setScale(scale, RoundingMode.UP);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return new byte[0];
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        String text = getString(columnLabel);
        return java.sql.Date.valueOf(text);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        String text = getString(columnLabel);
        return java.sql.Time.valueOf(text);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        String text = getString(columnLabel);
        return java.sql.Timestamp.valueOf(text);
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public String getCursorName() throws SQLException {
        return "";
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new ChdbResultSetMetaData(this.cols, this.colsMeta);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getObject(cols[columnIndex - 1]);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getCurrentRow().get(columnLabel);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        for (int i = 0; i < cols.length; i++) {
            if (Objects.equals(cols[i], columnLabel)) {
                return i + 1;
            }
        }
        throw new SQLException("column label not found: " + columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return getBigDecimal(cols[columnIndex - 1]);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        Object object = getCurrentRow().get(columnLabel);
        return switch (object) {
            case null -> null;
            case Integer i -> BigDecimal.valueOf(i);
            case Long l -> BigDecimal.valueOf(l);
            case Double d -> BigDecimal.valueOf(d);
            case BigDecimal b -> b;
            default -> BigDecimal.valueOf(Double.parseDouble(object.toString()));
        };
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return row == -1;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return row >= maxRows;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return row == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        return row == maxRows - 1;
    }

    @Override
    public void beforeFirst() throws SQLException {
        row = -1;
    }

    @Override
    public void afterLast() throws SQLException {
        row = maxRows;
    }

    @Override
    public boolean first() throws SQLException {
        return row == 0;
    }

    @Override
    public boolean last() throws SQLException {
        return row == maxRows - 1;
    }

    @Override
    public int getRow() throws SQLException {
        return row + 1;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        this.row = row - 1;
        return true;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        this.row = this.row + rows;
        return false;
    }

    @Override
    public boolean previous() throws SQLException {
        if (row >= 1) {
            this.row = row - 1;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLException("Not support");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {

    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {

    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {

    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {

    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {

    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {

    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {

    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {

    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {

    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {

    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {

    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {

    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {

    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {

    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {

    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {

    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {

    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {

    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {

    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {

    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {

    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {

    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {

    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {

    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {

    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {

    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {

    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {

    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {

    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {

    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {

    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {

    }

    @Override
    public void insertRow() throws SQLException {

    }

    @Override
    public void updateRow() throws SQLException {

    }

    @Override
    public void deleteRow() throws SQLException {

    }

    @Override
    public void refreshRow() throws SQLException {

    }

    @Override
    public void cancelRowUpdates() throws SQLException {

    }

    @Override
    public void moveToInsertRow() throws SQLException {

    }

    @Override
    public void moveToCurrentRow() throws SQLException {

    }

    @Override
    public Statement getStatement() throws SQLException {
        return this.statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {

    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {

    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {

    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {

    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {

    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {

    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {

    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return "";
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return "";
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return getObject(cols[columnIndex - 1], type);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        if (type == null) {
            throw new SQLException("requested type cannot be null");
        } else if (type == String.class) {
            return type.cast(this.getString(columnLabel));
        } else if (type == Boolean.class) {
            return type.cast(this.getBoolean(columnLabel));
        } else if (type == BigDecimal.class) {
            return type.cast(this.getBigDecimal(columnLabel));
        } else if (type == byte[].class) {
            return type.cast(this.getBytes(columnLabel));
        } else if (type == Date.class) {
            return type.cast(this.getDate(columnLabel));
        } else if (type == Time.class) {
            return type.cast(this.getTime(columnLabel));
        } else if (type == Timestamp.class) {
            return type.cast(this.getTimestamp(columnLabel));
        } else if (type == LocalDate.class) {
            try {
                return type.cast(LocalDate.parse(this.getString(columnLabel)));
            } catch (SQLException var4) {
                return null;
            }
        } else if (type == LocalTime.class) {
            try {
                return type.cast(LocalTime.parse(this.getString(columnLabel)));
            } catch (SQLException var5) {
                return null;
            }
        } else if (type == LocalDateTime.class) {
            try {
                return type.cast(LocalDateTime.parse(this.getString(columnLabel)));
            } catch (SQLException var6) {
                return null;
            }
        } else {
            throw new SQLException("type not support: " + type.getName());
        }
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

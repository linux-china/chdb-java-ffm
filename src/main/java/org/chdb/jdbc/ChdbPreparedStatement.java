package org.chdb.jdbc;

import org.chdb.*;
import org.chdb.result.*;

import java.io.*;
import java.math.*;
import java.net.*;
import java.sql.Date;
import java.sql.*;
import java.util.*;


/**
 * fake one, but friendly for most libraries and frameworks
 *
 * @author linux_china
 */
public class ChdbPreparedStatement implements PreparedStatement {
    private final ChdbConnection connection;
    private final String sql;
    private final List<JdbcParam> params = new ArrayList<>();

    public ChdbPreparedStatement(ChdbConnection conn, String sql) {
        this.connection = conn;
        this.sql = sql;
    }

    /**
     * replace `?` with real param value.
     *
     * @return SQL
     */
    public String getRealSQL() {
        StringBuilder builder = new StringBuilder();
        String[] parts = (sql + " ").split("\\?", params.size() + 1);
        for (int i = 0; i < params.size(); i++) {
            builder.append(parts[i]);
            String literalValue = params.get(i).getLiteralValue();
            builder.append(literalValue);
        }
        if (!parts[parts.length - 1].isEmpty()) {
            builder.append(parts[parts.length - 1]);
        }
        return builder.toString();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        String realSQL = getRealSQL();
        QueryResultV2 resultV2 = Chdb.query(realSQL);
        String sqlError = resultV2.sqlError();
        if (sqlError != null) {
            throw new SQLException(sqlError);
        }
        return new ChdbResultSet(this, realSQL, resultV2.toJsonResult());
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw new SQLException("Not support");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        params.add(parameterIndex - 1, new JdbcParam(sqlType, Object.class, null));
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        params.add(parameterIndex - 1, new JdbcParam(Types.BOOLEAN, Boolean.class, x));
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        params.add(parameterIndex - 1, new JdbcParam(Types.SMALLINT, Byte.class, x));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        params.add(parameterIndex - 1, new JdbcParam(Types.SMALLINT, Short.class, x));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        params.add(parameterIndex - 1, new JdbcParam(Types.INTEGER, Integer.class, x));
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        params.add(parameterIndex - 1, new JdbcParam(Types.BIGINT, Long.class, x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        params.add(parameterIndex - 1, new JdbcParam(Types.FLOAT, Float.class, x));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        params.add(parameterIndex - 1, new JdbcParam(Types.DOUBLE, Double.class, x));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        params.add(parameterIndex - 1, new JdbcParam(Types.DECIMAL, BigDecimal.class, x));
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        params.add(parameterIndex - 1, new JdbcParam(Types.VARCHAR, String.class, x));
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new SQLException("Not Support");
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        params.add(parameterIndex - 1, new JdbcParam(Types.DATE, Date.class, x));
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        params.add(parameterIndex - 1, new JdbcParam(Types.TIME, Time.class, x));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        params.add(parameterIndex - 1, new JdbcParam(Types.TIMESTAMP, Timestamp.class, x));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void clearParameters() throws SQLException {
        this.params.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        params.add(parameterIndex - 1, new JdbcParam(targetSqlType, Object.class, x));
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        switch (x) {
            case Byte num -> setByte(parameterIndex, num);
            case Short num -> setShort(parameterIndex, num);
            case Integer num -> setInt(parameterIndex, num);
            case Long num -> setLong(parameterIndex, num);
            case Float num -> setFloat(parameterIndex, num);
            case Double num -> setDouble(parameterIndex, num);
            case BigDecimal num -> setBigDecimal(parameterIndex, num);
            case Date o -> setDate(parameterIndex, o);
            case Time o -> setTime(parameterIndex, o);
            case Timestamp o -> setTimestamp(parameterIndex, o);
            default -> throw new SQLException("Not support");
        }
    }

    @Override
    public boolean execute() throws SQLException {
        throw new SQLException("Not support");
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLException("Not support");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {

    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {

    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {

    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLException("Not support");
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {

    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {

    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {

    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {

    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {

    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {

    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        throw new SQLException("Not support");
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLException("Not support");
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {

    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {

    }

    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public boolean execute(String sql) throws SQLException {
        throw new SQLException("Not support");
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new SQLException("Not support");
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

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
    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
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

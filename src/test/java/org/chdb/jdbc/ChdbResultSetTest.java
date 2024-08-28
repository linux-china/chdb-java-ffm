package org.chdb.jdbc;

import org.junit.jupiter.api.*;

import java.sql.*;

public class ChdbResultSetTest {

    @Test
    public void testEmptyResultSet() throws Exception {
        String columns = "TABLE_CAT Nullable(String), TABLE_SCHEM Nullable(String), TABLE_NAME String, GRANTOR Nullable(String), GRANTEE String, PRIVILEGE String, IS_GRANTABLE Nullable(String)";
        try (ChdbResultSet resultSet = new ChdbResultSet(null, "", columns, null)) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            System.out.println(metaData.getColumnCount());
        }
    }
}

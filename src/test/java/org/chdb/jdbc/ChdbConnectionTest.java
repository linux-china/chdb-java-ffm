package org.chdb.jdbc;

import org.junit.jupiter.api.*;

import java.sql.*;

public class ChdbConnectionTest {

    @AutoClose
    private static Connection conn;

    @BeforeAll
    public static void setUp() throws Exception {
        String url = "jdbc:chdb::memory:";
        conn = DriverManager.getConnection(url);
    }

    @Test
    public void testStatement() throws SQLException {
        String sql = "select * from file('src/test/resources/logs.csv','CSV')";
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
            System.out.println(resultSet.getString("level"));
        }
    }

    @Test
    public void testPreparedStatement() throws SQLException {
        String sql = "select * from file('src/test/resources/logs.csv','CSV') where level = ? and id > 0";
        ChdbPreparedStatement statement = (ChdbPreparedStatement) conn.prepareStatement(sql);
        statement.setString(1, "error");
        System.out.println(statement.getRealSQL());
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            System.out.println(resultSet.getString("level"));
        }
    }
}

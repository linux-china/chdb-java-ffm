package org.chdb.jdbc;

import org.junit.jupiter.api.*;

import java.sql.*;

public class ChdbConnectionTest {

    @AutoClose
    private static Connection conn;

    @BeforeAll
    public static void setUp() throws Exception {
        String url = "jdbc:chdb:memory:";
        conn = DriverManager.getConnection(url);
    }

    @Test
    public void testConnection() throws SQLException {
        String sql = "select * from file('src/test/resources/logs.csv','CSV')";
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
            System.out.println(resultSet.getString("level"));
        }
    }
}

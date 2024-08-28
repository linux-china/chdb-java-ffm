package org.chdb.jdbc;

import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.*;
import org.springframework.jdbc.datasource.*;

import javax.sql.*;
import java.sql.*;

public class JdbcTemplateTest {
    @AutoClose
    private static JdbcTemplate jdbcTemplate;

    @BeforeAll
    public static void setUp() throws Exception {
        String url = "jdbc:chdb:memory:";
        DataSource ds = new SimpleDriverDataSource(new ChdbDriver(), url);
        jdbcTemplate = new JdbcTemplate(ds);
    }

    @Test
    public void testStatement() throws SQLException {
        String sql = "select * from file('src/test/resources/logs.csv','CSV')";
        jdbcTemplate.query(sql, new RowCallbackHandler() {
            @Override
            public void processRow(ResultSet rs) throws SQLException {
                System.out.println(rs.getString(2));
            }
        });
    }

    @Test
    public void testPreparedStatement() throws SQLException {
        String sql = "select * from file('src/test/resources/logs.csv','CSV') where level = ?";
        jdbcTemplate.query(sql, rs -> {
            System.out.println(rs.getString(2));
        }, "error");
    }

}

package org.chdb;

import org.chdb.result.*;
import org.junit.jupiter.api.*;

public class ChdbTest {

    @Test
    public void testQuery() {
        String sql = "select * from file('src/test/resources/logs.csv','CSV')";
        QueryResultV2 result = Chdb.query(sql);
        System.out.println(result.getOutput());
        QueryJsonResult jsonResult = result.toJsonResult();
        System.out.println(jsonResult.meta());
    }
}

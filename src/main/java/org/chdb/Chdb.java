package org.chdb;

import org.chdb.ffm.LibChdb;
import org.chdb.ffm.local_result_v2;
import org.chdb.result.QueryResultV2;
import org.jspecify.annotations.NonNull;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * chdb facade for FFM
 *
 * @author linux_china
 */
public class Chdb {

    public static QueryResultV2 query(String sql) {
        MemorySegment result = null;
        try (Arena arena = Arena.ofShared()) {
            MemorySegment parameters = arena.allocate(ValueLayout.ADDRESS, 3);
            parameters.setAtIndex(ValueLayout.ADDRESS, 0, arena.allocateFrom("clickhouse"));
            parameters.setAtIndex(ValueLayout.ADDRESS, 1, arena.allocateFrom("--format=JSON"));
            parameters.setAtIndex(ValueLayout.ADDRESS, 2, arena.allocateFrom("--query=" + sql));
            result = LibChdb.query_stable_v2(3, parameters);
            return parseResult(result);
        } finally {
            if (result != null) {
                LibChdb.free_result_v2(result);
            }
        }
    }

    public static QueryResultV2 query(@NonNull String dbPath, String sql) {
        MemorySegment result = null;
        try (Arena arena = Arena.ofShared()) {
            MemorySegment parameters = arena.allocate(ValueLayout.ADDRESS, 4);
            parameters.setAtIndex(ValueLayout.ADDRESS, 0, arena.allocateFrom("clickhouse"));
            parameters.setAtIndex(ValueLayout.ADDRESS, 1, arena.allocateFrom("--path=" + dbPath));
            parameters.setAtIndex(ValueLayout.ADDRESS, 2, arena.allocateFrom("--format=JSON"));
            parameters.setAtIndex(ValueLayout.ADDRESS, 3, arena.allocateFrom("--query=" + sql));
            result = LibChdb.query_stable_v2(4, parameters);
            return parseResult(result);
        } finally {
            if (result != null) {
                LibChdb.free_result_v2(result);
            }
        }
    }

    private static QueryResultV2 parseResult(MemorySegment result) {
        MemorySegment errorMessage = local_result_v2.error_message(result);
        if (errorMessage.address() > 0) {
            String errorMessageText = errorMessage.getString(0);
            return QueryResultV2.error(errorMessageText);
        }
        long byteCount = local_result_v2.len(result);
        long rowCount = local_result_v2.rows_read(result);
        double elapsed = local_result_v2.elapsed(result);
        MemorySegment buf = local_result_v2.buf(result);
        if (buf.address() > 0) {
            String output = buf.getString(0).trim();
            return new QueryResultV2(byteCount, rowCount, elapsed, output);
        }
        return QueryResultV2.error("No output found!");
    }
}

package org.chdb.result;

import com.fasterxml.jackson.annotation.*;

import java.util.*;

/**
 * clickhouse json output object
 *
 * @param meta meta data
 * @param data data
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record QueryJsonResult(List<ColumnInfo> meta, List<Map<String, Object>> data) {
}

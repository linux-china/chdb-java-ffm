package org.chdb.result;

import com.fasterxml.jackson.annotation.*;

/**
 * column info
 *
 * @param name column name
 * @param type clickhouse column type
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ColumnInfo(String name, String type) {
}

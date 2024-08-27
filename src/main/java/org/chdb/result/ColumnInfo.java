package org.chdb.result;

import com.fasterxml.jackson.annotation.*;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ColumnInfo(String name, String type) {
}

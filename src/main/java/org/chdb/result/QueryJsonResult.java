package org.chdb.result;

import com.fasterxml.jackson.annotation.*;

import java.util.*;

@JsonIgnoreProperties(ignoreUnknown = true)
public record QueryJsonResult(List<ColumnInfo> meta, List<Map<String, Object>> data) {
}

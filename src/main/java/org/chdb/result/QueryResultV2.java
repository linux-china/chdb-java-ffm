package org.chdb.result;

import com.fasterxml.jackson.databind.*;

public class QueryResultV2 {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private long byteCount;
    private long rowCount;
    private double elapsed;
    private String errorMessage;
    private String output; // csv format

    public QueryResultV2(long byteCount, long rowCount, double elapsed, String output) {
        this.byteCount = byteCount;
        this.rowCount = rowCount;
        this.elapsed = elapsed;
        this.output = output;
    }

    public static QueryResultV2 error(String errorMessage) {
        return new QueryResultV2(0, 0, 0, errorMessage);
    }

    public long getByteCount() {
        return byteCount;
    }

    public void setByteCount(long byteCount) {
        this.byteCount = byteCount;
    }

    public long getRowCount() {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public double getElapsed() {
        return elapsed;
    }

    public void setElapsed(double elapsed) {
        this.elapsed = elapsed;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public String sqlError() {
        if (errorMessage != null) {
            return errorMessage;
        } else if (output != null && !output.startsWith("{")) {
            return output;
        }
        return null;
    }

    public QueryJsonResult toJsonResult() {
        try {
            return OBJECT_MAPPER.readValue(output, QueryJsonResult.class);
        } catch (Exception e) {
            return null;
        }
    }
}

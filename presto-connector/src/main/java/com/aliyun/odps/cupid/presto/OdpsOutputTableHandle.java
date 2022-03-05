package com.aliyun.odps.cupid.presto;

import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class OdpsOutputTableHandle implements ConnectorOutputTableHandle {
    private final String schemaName;
    private final String tableName;
    private final List<OdpsColumnHandle> inputColumns;
    private final List<String> partitionedBy;

    @JsonCreator
    public OdpsOutputTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("inputColumns") List<OdpsColumnHandle> inputColumns,
            @JsonProperty("partitionedBy") List<String> partitionedBy) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.inputColumns = inputColumns;
        this.partitionedBy = partitionedBy;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public List<OdpsColumnHandle> getInputColumns() {
        return inputColumns;
    }

    @JsonProperty
    public List<String> getPartitionedBy() {
        return partitionedBy;
    }
}
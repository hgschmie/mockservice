package com.zuora.mockservice.resources;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchColumnType;
import io.airlift.tpch.TpchEntity;

import java.util.Optional;

public class ColumnDescriptor {

    private final String name;
    private final TpchColumn<? extends TpchEntity> column;
    private final boolean hidden;

    public ColumnDescriptor(String name, TpchColumn<? extends TpchEntity> column, boolean hidden) {
        this.name = checkNotNull(name, "name is null");
        this.column = checkNotNull(column, "column is null");
        this.hidden = hidden;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public String getColumnName() {
        return column.getSimplifiedColumnName();
    }

    @JsonProperty
    public String getColumnType() {
        return column.getType().getBase().toString();
    }

    @JsonProperty
    public Optional<Long> getPrecision() {
        return column.getType().getPrecision();
    }

    @JsonProperty
    public boolean isHidden() {
        return hidden;
    }
}

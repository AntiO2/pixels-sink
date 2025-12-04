/*
 * Copyright 2025 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
 
package io.pixelsdb.pixels.sink.metadata;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.core.TypeDescription;
import lombok.Getter;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
public class TableMetadata
{
    private final Table table;
    private final SinglePointIndex index;
    private final TypeDescription typeDescription;
    private final List<Column> columns;
    private final List<String> keyColumnNames;

    public TableMetadata(Table table, SinglePointIndex index, List<Column> columns) throws MetadataException
    {
        this.table = table;
        this.index = index;
        this.columns = columns;
        this.keyColumnNames = new LinkedList<>();
        List<String> columnNames = columns.stream().map(Column::getName).collect(Collectors.toList());
        List<String> columnTypes = columns.stream().map(Column::getType).collect(Collectors.toList());
        typeDescription = TypeDescription.createSchemaFromStrings(columnNames, columnTypes);
        if (index != null)
        {
            Map<Long, Column> columnMap = new HashMap<>();
            for (Column column : columns)
            {
                columnMap.put(column.getId(), column);
            }

            for (Integer keyColumnId : index.getKeyColumns().getKeyColumnIds())
            {
                Column column = columnMap.get(keyColumnId.longValue());
                if (column != null)
                {
                    keyColumnNames.add(column.getName());
                } else
                {
                    throw new MetadataException("Cant find key column id: " + keyColumnId + " in table "
                            + table.getName() + " schema id is " + table.getSchemaId());
                }
            }
        }
    }

    public boolean hasPrimaryIndex()
    {
        return index != null;
    }

    public long getPrimaryIndexKeyId()
    {
        return index.getId();
    }

    public long getTableId()
    {
        return table.getId();
    }

    public long getSchemaId()
    {
        return table.getSchemaId();
    }
}
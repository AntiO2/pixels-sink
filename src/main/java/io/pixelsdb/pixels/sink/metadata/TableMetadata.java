/*
 * Copyright 2025 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.pixelsdb.pixels.sink.metadata;

import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.core.TypeDescription;
import lombok.Getter;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

@Getter
public class TableMetadata {
    private final Table table;
    private final SinglePointIndex index;
    private final TypeDescription typeDescription;
    private final List<Column> columns;
    private final List<String> keyColumnNames;

    public TableMetadata(Table table, SinglePointIndex index, List<Column> columns) {
        this.table = table;
        this.index = index;
        this.columns = columns;
        this.keyColumnNames = new LinkedList<>();
        List<String> columnNames = columns.stream().map(Column::getName).collect(Collectors.toList());
        List<String> columnTypes = columns.stream().map(Column::getType).collect(Collectors.toList());
        typeDescription = TypeDescription.createSchemaFromStrings(columnNames, columnTypes);
        if(index != null) {
            List<Integer> keyColumnIds = index.getKeyColumns().getKeyColumnIds();
            for (Integer keyColumnId : keyColumnIds) {
                keyColumnNames.add(columns.get(keyColumnId).getName());
            }
        }
    }

    public boolean hasPrimaryIndex() {
        return index != null;
    }
    public int getPkId() {
        return index.getKeyColumns().getKeyColumnIds().get(0);
    }
}
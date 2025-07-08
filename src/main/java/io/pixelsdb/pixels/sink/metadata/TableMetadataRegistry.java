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

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.SecondaryIndex;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.deserializer.SchemaDeserializer;
import io.pixelsdb.pixels.sink.exception.SinkException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class TableMetadataRegistry {
    private static final MetadataService metadataService = MetadataService.Instance();
    private static volatile TableMetadataRegistry instance;
    private final ConcurrentMap<TableMetadataKey, TableMetadata> registry = new ConcurrentHashMap<>();
    private final ConcurrentMap<TableMetadataKey, TypeDescription> typeDescriptionConcurrentMap = new ConcurrentHashMap<>();
    private final SchemaCache schemaCache = SchemaCache.getInstance();

    private TableMetadataRegistry() {
    }

    public static TableMetadataRegistry Instance() {
        if (instance == null) {
            synchronized (TableMetadataRegistry.class) {
                if (instance == null) {
                    instance = new TableMetadataRegistry();
                }
            }
        }
        return instance;
    }

    public TableMetadata getMetadata(String schema, String table) throws SinkException {
        TableMetadataKey key = new TableMetadataKey(schema, table);
        if (!registry.containsKey(key)) {
            TableMetadata metadata = loadTableMetadata(schema, table);
            registry.put(key, metadata);
        }
        return registry.get(key);
    }



    public TypeDescription getTypeDescription(String schema, String table) {
        TableMetadataKey key = new TableMetadataKey(schema, table);
        return typeDescriptionConcurrentMap.computeIfAbsent(key, k -> {
            try {
                return loadTypeDescription(schema, table);
            } catch (MetadataException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public TableMetadata loadTableMetadata(String schemaName, String tableName) throws SinkException {
        try {
            Table table = metadataService.getTable(schemaName, tableName);
            SecondaryIndex index = null;
            try {
                index = metadataService.getSecondaryIndex(table.getId());
                /*
                  TODO(Lizn): just test;
                 */
                if (!index.isUnique()) {
                    throw new MetadataException("Non Unique Index is not supported");
                }
            } catch (MetadataException ignored) {

            }

            List<Column> tableColumns = metadataService.getColumns(schemaName, tableName, false);
            return new TableMetadata(table, index, tableColumns);
        } catch (MetadataException e) {
            throw new SinkException(e);
        }
    }

    /**
     * parse typeDescription from avro record and cache it.
     *
     * @param record
     * @return
     */
    public TypeDescription parseTypeDescription(GenericRecord record, String sourceSchema, String sourceTable) {
        Schema schema = ((GenericData.Record) record).getSchema().getField("before").schema().getTypes().get(1);
        TableMetadataKey tableMetadataKey = new TableMetadataKey(sourceSchema, sourceTable);
        TypeDescription typeDescription = typeDescriptionConcurrentMap.computeIfAbsent(
                tableMetadataKey,
                key -> SchemaDeserializer.parseFromAvroSchema(schema)
        );
        return typeDescription;
    }

    private TypeDescription loadTypeDescription(String schemaName, String tableName) throws MetadataException {
        List<Column> columns = metadataService.getColumns(schemaName, tableName, false);
        List<String> columnNames = columns.stream().map(Column::getName).collect(Collectors.toList());
        List<String> columnTypes = columns.stream().map(Column::getType).collect(Collectors.toList());
        return TypeDescription.createSchemaFromStrings(columnNames, columnTypes);
    }
}

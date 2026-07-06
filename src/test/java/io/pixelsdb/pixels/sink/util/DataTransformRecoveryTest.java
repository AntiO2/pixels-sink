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

package io.pixelsdb.pixels.sink.util;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.KeyColumns;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import io.pixelsdb.pixels.sink.provider.ProtoType;
import io.pixelsdb.pixels.sink.source.storage.StorageSourceOffset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.*;

class DataTransformRecoveryTest
{
    private static final String SCHEMA = "unit_test";
    private static final String TABLE = "recovery_row";

    @BeforeEach
    void setUp() throws Exception
    {
        ConfigFactory configFactory = ConfigFactory.Instance();
        configFactory.addProperty("sink.mode", "retina");
        configFactory.addProperty("sink.monitor.enable", "false");
        configFactory.addProperty("sink.monitor.report.enable", "false");
        configFactory.addProperty("sink.monitor.freshness.level", "row");
        PixelsSinkConfigFactory.initialize(configFactory);
        installMetadata();
    }

    @AfterEach
    void tearDown() throws Exception
    {
        clearMetadata();
        PixelsSinkConfigFactory.reset();
    }

    @Test
    void shouldRewriteInsertIntoUpdateWithBeforeImageCopied() throws Exception
    {
        TableMetadata metadata = buildMetadata();
        TypeDescription schema = metadata.getTypeDescription();
        RowChangeEvent insertEvent = new RowChangeEvent(insertRowRecord(), schema);
        insertEvent.setTimeStamp(123456789L);
        StorageSourceOffset offset = new StorageSourceOffset(2, 128L, 7, ProtoType.ROW);
        insertEvent.setSourceOffset(offset);

        RowChangeEvent transformed = DataTransform.transformInsertToRecoveryUpdate(insertEvent);

        assertEquals(SinkProto.OperationType.UPDATE, transformed.getOp());
        assertEquals(insertEvent.getTimeStamp(), transformed.getTimeStamp());
        assertEquals(offset, transformed.getSourceOffset());
        assertTrue(transformed.hasBeforeData());
        assertTrue(transformed.hasAfterData());
        assertEquals(transformed.getRowRecord().getAfter(), transformed.getRowRecord().getBefore());

        IndexProto.IndexKey beforeKey = transformed.getBeforeKey();
        IndexProto.IndexKey afterKey = transformed.getAfterKey();
        assertNotNull(beforeKey);
        assertNotNull(afterKey);
        assertEquals(beforeKey.getKey(), afterKey.getKey());
    }

    @Test
    void shouldLeaveNonInsertEventUnchanged() throws Exception
    {
        TableMetadata metadata = buildMetadata();
        RowChangeEvent updateEvent = new RowChangeEvent(updateRowRecord(), metadata.getTypeDescription());

        RowChangeEvent transformed = DataTransform.transformInsertToRecoveryUpdate(updateEvent);

        assertSame(updateEvent, transformed);
    }

    private void installMetadata() throws Exception
    {
        TableMetadata metadata = buildMetadata();
        Field registryField = TableMetadataRegistry.class.getDeclaredField("registry");
        registryField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<SchemaTableName, TableMetadata> registry =
                (ConcurrentMap<SchemaTableName, TableMetadata>) registryField.get(TableMetadataRegistry.Instance());
        registry.clear();
        registry.put(new SchemaTableName(SCHEMA, TABLE), metadata);

        Field tableIdField = TableMetadataRegistry.class.getDeclaredField("tableId2SchemaTableName");
        tableIdField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<Long, SchemaTableName> tableIdMap =
                (ConcurrentMap<Long, SchemaTableName>) tableIdField.get(TableMetadataRegistry.Instance());
        tableIdMap.clear();
        tableIdMap.put(metadata.getTableId(), new SchemaTableName(SCHEMA, TABLE));
    }

    private void clearMetadata() throws Exception
    {
        Field registryField = TableMetadataRegistry.class.getDeclaredField("registry");
        registryField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<SchemaTableName, TableMetadata> registry =
                (ConcurrentMap<SchemaTableName, TableMetadata>) registryField.get(TableMetadataRegistry.Instance());
        registry.clear();

        Field tableIdField = TableMetadataRegistry.class.getDeclaredField("tableId2SchemaTableName");
        tableIdField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<Long, SchemaTableName> tableIdMap =
                (ConcurrentMap<Long, SchemaTableName>) tableIdField.get(TableMetadataRegistry.Instance());
        tableIdMap.clear();
    }

    private TableMetadata buildMetadata() throws Exception
    {
        Table table = new Table();
        table.setId(1001L);
        table.setName(TABLE);
        table.setSchemaId(2001L);
        table.setStorageScheme(Storage.Scheme.file);

        Column id = new Column();
        id.setId(1L);
        id.setName("id");
        id.setType("int");
        id.setTableId(table.getId());

        Column age = new Column();
        age.setId(2L);
        age.setName("age");
        age.setType("int");
        age.setTableId(table.getId());

        KeyColumns keyColumns = new KeyColumns();
        keyColumns.addKeyColumnIds(1);

        SinglePointIndex index = new SinglePointIndex();
        index.setId(3001L);
        index.setPrimary(true);
        index.setUnique(true);
        index.setTableId(table.getId());
        index.setSchemaVersionId(4001L);
        index.setKeyColumns(keyColumns);

        return new TableMetadata(table, index, List.of(id, age));
    }

    private SinkProto.RowRecord insertRowRecord()
    {
        return SinkProto.RowRecord.newBuilder()
                .setSource(sourceInfo())
                .setTransaction(transactionInfo())
                .setOp(SinkProto.OperationType.INSERT)
                .setAfter(rowValue(42, 18))
                .build();
    }

    private SinkProto.RowRecord updateRowRecord()
    {
        return SinkProto.RowRecord.newBuilder()
                .setSource(sourceInfo())
                .setTransaction(transactionInfo())
                .setOp(SinkProto.OperationType.UPDATE)
                .setBefore(rowValue(42, 18))
                .setAfter(rowValue(42, 19))
                .build();
    }

    private SinkProto.SourceInfo sourceInfo()
    {
        return SinkProto.SourceInfo.newBuilder()
                .setDb(SCHEMA)
                .setSchema(SCHEMA)
                .setTable(TABLE)
                .build();
    }

    private SinkProto.TransactionInfo transactionInfo()
    {
        return SinkProto.TransactionInfo.newBuilder()
                .setId("tx-recovery-test")
                .build();
    }

    private SinkProto.RowValue rowValue(int id, int age)
    {
        return SinkProto.RowValue.newBuilder()
                .addValues(intValue(id))
                .addValues(intValue(age))
                .build();
    }

    private SinkProto.ColumnValue intValue(int value)
    {
        return SinkProto.ColumnValue.newBuilder()
                .setValue(ByteString.copyFrom(ByteBuffer.allocate(Integer.BYTES).putInt(value).array()))
                .build();
    }
}

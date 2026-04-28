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

package io.pixelsdb.pixels.sink.integration;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.index.SinglePointIndex;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import io.pixelsdb.pixels.sink.writer.proto.ProtoWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtoCdcLogIntegrationTest
{
    private static final String TEST_SCHEMA = "pixels_sink_it";
    private static final String TEST_TABLE = "recovery_account";
    private static final String DATASET_NAME = "recovery-cdc-it";
    private static final String TX_ID = "it-tx-1";
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @AfterEach
    void tearDown()
    {
        PixelsSinkConfigFactory.reset();
    }

    @Test
    void shouldCreateSchemaAndGenerateLocalProtoCdcLog() throws Exception
    {
        Path outputRoot = Paths.get("target", "integration-tests", "proto-cdc");
        recreateDirectory(outputRoot);
        initializeProtoWriterConfig(outputRoot);

        TableSpec tableSpec = resolveTableSpec();

        try (ProtoWriter writer = new ProtoWriter())
        {
            writer.writeTrans(beginTransaction());
            writer.writeRow(insertRow(tableSpec));
            writer.writeRow(updateRow(tableSpec));
            writer.writeRow(deleteRow(tableSpec));
            writer.writeTrans(endTransaction(tableSpec, 3));
        }

        Path datasetDir = outputRoot.resolve(DATASET_NAME);
        assertTrue(Files.isDirectory(datasetDir), "Proto dataset directory was not created");

        List<Path> protoFiles;
        try (var pathStream = Files.list(datasetDir))
        {
            protoFiles = pathStream
                    .filter(path -> path.getFileName().toString().endsWith(".proto"))
                    .sorted()
                    .toList();
        }

        assertFalse(protoFiles.isEmpty(), "No proto CDC files were generated");

        int framedRecordCount = countFramedRecords(protoFiles.get(0));
        assertEquals(5, framedRecordCount, "Expected BEGIN + 3 row events + END in the proto CDC file");
    }

    private void initializeProtoWriterConfig(Path outputRoot)
    {
        ConfigFactory configFactory = ConfigFactory.Instance();
        configFactory.addProperty("sink.proto.dir", outputRoot.toUri().toString());
        configFactory.addProperty("sink.proto.data", DATASET_NAME);
        configFactory.addProperty("sink.proto.maxRecords", "100");
        PixelsSinkConfigFactory.initialize(configFactory);
    }

    private TableSpec resolveTableSpec() throws MetadataException
    {
        MetadataService metadataService = MetadataService.Instance();
        String configuredSchema = System.getProperty("pixels.it.schema");
        String configuredTable = System.getProperty("pixels.it.table");
        if (configuredSchema != null && configuredTable != null &&
                metadataService.existTable(configuredSchema, configuredTable))
        {
            return buildTableSpecFromMetadata(configuredSchema, configuredTable, metadataService);
        }

        try
        {
            ensureSchemaAndTable(metadataService);
            return TableSpec.minimalTestTable();
        } catch (MetadataException createFailure)
        {
            if (metadataService.existTable("pixels_bench_sf1x", "savingaccount"))
            {
                return buildTableSpecFromMetadata("pixels_bench_sf1x", "savingaccount", metadataService);
            }
            if (metadataService.existTable("pixels_bench_sf10x", "savingaccount"))
            {
                return buildTableSpecFromMetadata("pixels_bench_sf10x", "savingaccount", metadataService);
            }
            throw new TestAbortedException(
                    "No reusable integration-test table found. Start with an existing table via "
                            + "-Dpixels.it.schema=<schema> -Dpixels.it.table=<table>, "
                            + "or provide a metadata environment that allows createTable().",
                    createFailure
            );
        }
    }

    private void ensureSchemaAndTable(MetadataService metadataService) throws MetadataException
    {
        if (!metadataService.existSchema(TEST_SCHEMA))
        {
            metadataService.createSchema(TEST_SCHEMA);
        }
        if (!metadataService.existTable(TEST_SCHEMA, TEST_TABLE))
        {
            metadataService.createTable(
                    TEST_SCHEMA,
                    TEST_TABLE,
                    Storage.Scheme.file,
                    List.of("account_id"),
                    List.of(
                            column("account_id", "bigint"),
                            column("balance", "bigint"),
                            column("owner", "varchar(32)")
                    )
            );
        }

        ensurePrimaryIndex(metadataService);
    }

    private void ensurePrimaryIndex(MetadataService metadataService) throws MetadataException
    {
        Table table = metadataService.getTable(TEST_SCHEMA, TEST_TABLE);
        try
        {
            metadataService.getPrimaryIndex(table.getId());
            return;
        } catch (MetadataException ignored)
        {
            // Fall through and create the primary index.
        }

        List<Column> columns = metadataService.getColumns(TEST_SCHEMA, TEST_TABLE, false);
        long accountIdColumnId = columns.stream()
                .filter(column -> "account_id".equals(column.getName()))
                .findFirst()
                .orElseThrow(() -> new MetadataException("Missing account_id column in metadata"))
                .getId();
        Layout layout = metadataService.getLatestLayout(TEST_SCHEMA, TEST_TABLE);

        io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex index =
                new io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex();
        index.setPrimary(true);
        index.setUnique(true);
        index.setIndexScheme(SinglePointIndex.Scheme.rocksdb);
        index.setTableId(table.getId());
        index.setSchemaVersionId(layout.getSchemaVersionId());
        index.setKeyColumnsJson("{\"keyColumnIds\":[" + accountIdColumnId + "]}");
        metadataService.createSinglePointIndex(index);
    }

    private Column column(String name, String type)
    {
        Column column = new Column();
        column.setName(name);
        column.setType(type);
        return column;
    }

    private TableSpec buildTableSpecFromMetadata(String schemaName, String tableName, MetadataService metadataService)
            throws MetadataException
    {
        List<Column> columns = metadataService.getColumns(schemaName, tableName, false);
        List<String> keyColumnNames;
        try
        {
            keyColumnNames = TableMetadataRegistry.Instance().getKeyColumnsName(schemaName, tableName);
        } catch (SinkException e)
        {
            throw new MetadataException("Failed to load primary key metadata for " + schemaName + "." + tableName, e);
        }
        List<String> insertValues = new ArrayList<>(columns.size());
        List<String> updateAfterValues = new ArrayList<>(columns.size());
        int updateColumnIndex = -1;

        for (int i = 0; i < columns.size(); i++)
        {
            Column column = columns.get(i);
            String value = sampleValue(column.getType(), i, false);
            insertValues.add(value);
            updateAfterValues.add(value);
            if (updateColumnIndex < 0 && !keyColumnNames.contains(column.getName()) && isUpdatableType(column.getType()))
            {
                updateColumnIndex = i;
            }
        }

        if (updateColumnIndex >= 0)
        {
            updateAfterValues.set(
                    updateColumnIndex,
                    sampleValue(columns.get(updateColumnIndex).getType(), updateColumnIndex, true)
            );
        }

        return new TableSpec(
                schemaName,
                tableName,
                insertValues,
                new ArrayList<>(insertValues),
                updateAfterValues,
                new ArrayList<>(updateAfterValues)
        );
    }

    private SinkProto.TransactionMetadata beginTransaction()
    {
        return SinkProto.TransactionMetadata.newBuilder()
                .setId(TX_ID)
                .setStatus(SinkProto.TransactionStatus.BEGIN)
                .setTimestamp(System.currentTimeMillis())
                .build();
    }

    private SinkProto.TransactionMetadata endTransaction(TableSpec tableSpec, int rowCount)
    {
        return SinkProto.TransactionMetadata.newBuilder()
                .setId(TX_ID)
                .setStatus(SinkProto.TransactionStatus.END)
                .setTimestamp(System.currentTimeMillis())
                .addDataCollections(
                        SinkProto.DataCollection.newBuilder()
                                .setDataCollection(tableSpec.schemaName + "." + tableSpec.tableName)
                                .setEventCount(rowCount)
                                .build()
                )
                .build();
    }

    private RowChangeEvent insertRow(TableSpec tableSpec) throws SinkException
    {
        return new RowChangeEvent(SinkProto.RowRecord.newBuilder()
                .setSource(sourceInfo(tableSpec))
                .setTransaction(transactionInfo())
                .setOp(SinkProto.OperationType.INSERT)
                .setAfter(buildRowValue(tableSpec.insertValues))
                .build());
    }

    private RowChangeEvent updateRow(TableSpec tableSpec) throws SinkException
    {
        return new RowChangeEvent(SinkProto.RowRecord.newBuilder()
                .setSource(sourceInfo(tableSpec))
                .setTransaction(transactionInfo())
                .setOp(SinkProto.OperationType.UPDATE)
                .setBefore(buildRowValue(tableSpec.updateBeforeValues))
                .setAfter(buildRowValue(tableSpec.updateAfterValues))
                .build());
    }

    private RowChangeEvent deleteRow(TableSpec tableSpec) throws SinkException
    {
        return new RowChangeEvent(SinkProto.RowRecord.newBuilder()
                .setSource(sourceInfo(tableSpec))
                .setTransaction(transactionInfo())
                .setOp(SinkProto.OperationType.DELETE)
                .setBefore(buildRowValue(tableSpec.deleteBeforeValues))
                .build());
    }

    private SinkProto.SourceInfo sourceInfo(TableSpec tableSpec)
    {
        return SinkProto.SourceInfo.newBuilder()
                .setDb(tableSpec.schemaName)
                .setTable(tableSpec.tableName)
                .build();
    }

    private SinkProto.TransactionInfo transactionInfo()
    {
        return SinkProto.TransactionInfo.newBuilder()
                .setId(TX_ID)
                .build();
    }

    private SinkProto.ColumnValue columnValue(String value)
    {
        return SinkProto.ColumnValue.newBuilder()
                .setValue(ByteString.copyFrom(value.getBytes(StandardCharsets.UTF_8)))
                .build();
    }

    private SinkProto.RowValue buildRowValue(List<String> values)
    {
        SinkProto.RowValue.Builder builder = SinkProto.RowValue.newBuilder();
        for (String value : values)
        {
            builder.addValues(columnValue(value));
        }
        return builder.build();
    }

    private String sampleValue(String rawType, int columnIndex, boolean updated)
    {
        String type = rawType.toLowerCase();
        if (type.contains("bigint") || type.contains("long"))
        {
            return updated ? Long.toString(2000L + columnIndex) : Long.toString(1000L + columnIndex);
        }
        if (type.contains("int") || type.contains("smallint") || type.contains("tinyint"))
        {
            return updated ? Integer.toString(200 + columnIndex) : Integer.toString(100 + columnIndex);
        }
        if (type.contains("float") || type.contains("double") || type.contains("decimal"))
        {
            return updated ? (2000 + columnIndex) + ".5" : (1000 + columnIndex) + ".0";
        }
        if (type.contains("bool"))
        {
            return updated ? "false" : "true";
        }
        if (type.contains("date") && !type.contains("timestamp"))
        {
            return LocalDate.of(2026, 4, updated ? 28 : 27).toString();
        }
        if (type.contains("timestamp") || type.contains("datetime"))
        {
            return LocalDateTime.of(2026, 4, updated ? 28 : 27, 20, 0, 0).format(TIMESTAMP_FORMATTER);
        }
        if (type.contains("time"))
        {
            return updated ? "20:05:00" : "20:00:00";
        }
        return updated ? "updated_" + columnIndex : "value_" + columnIndex;
    }

    private boolean isUpdatableType(String rawType)
    {
        String type = rawType.toLowerCase();
        return !type.contains("binary") && !type.contains("blob") && !type.contains("vector");
    }

    private int countFramedRecords(Path protoFile) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.wrap(Files.readAllBytes(protoFile)).order(ByteOrder.BIG_ENDIAN);
        int count = 0;
        while (buffer.remaining() > 0)
        {
            int key = buffer.getInt();
            int length = buffer.getInt();
            assertTrue(length >= 0, "Invalid framed record length for key " + key);
            assertTrue(buffer.remaining() >= length, "Truncated framed record for key " + key);
            buffer.position(buffer.position() + length);
            count++;
        }
        return count;
    }

    private void recreateDirectory(Path path) throws IOException
    {
        if (Files.exists(path))
        {
            try (var stream = Files.walk(path))
            {
                stream.sorted(Comparator.reverseOrder())
                        .forEach(current ->
                        {
                            try
                            {
                                Files.delete(current);
                            } catch (IOException e)
                            {
                                throw new RuntimeException(e);
                            }
                        });
            }
        }
        Files.createDirectories(path);
    }

    private static final class TableSpec
    {
        private final String schemaName;
        private final String tableName;
        private final List<String> insertValues;
        private final List<String> updateBeforeValues;
        private final List<String> updateAfterValues;
        private final List<String> deleteBeforeValues;

        private TableSpec(
                String schemaName,
                String tableName,
                List<String> insertValues,
                List<String> updateBeforeValues,
                List<String> updateAfterValues,
                List<String> deleteBeforeValues)
        {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.insertValues = insertValues;
            this.updateBeforeValues = updateBeforeValues;
            this.updateAfterValues = updateAfterValues;
            this.deleteBeforeValues = deleteBeforeValues;
        }

        private static TableSpec minimalTestTable()
        {
            return new TableSpec(
                    TEST_SCHEMA,
                    TEST_TABLE,
                    List.of("1001", "10000", "alice"),
                    List.of("1001", "10000", "alice"),
                    List.of("1001", "12000", "alice"),
                    List.of("1001", "12000", "alice")
            );
        }

    }
}

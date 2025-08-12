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
import io.pixelsdb.pixels.common.index.IndexService;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.index.IndexProto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * @package: io.pixelsdb.pixels.sink.metadata
 * @className: TestIndexService
 * @author: AntiO2
 * @date: 2025/8/5 04:34
 */
public class TestIndexService {

    private final MetadataService metadataService = MetadataService.Instance();
    private final IndexService indexService = IndexService.Instance();

    @Test
    public void testCreateFreshnessIndex() throws MetadataException {
        String testSchemaName = "pixels_bench_sf1x";
        String testTblName = "freshness";
        String keyColumn = "{\"keyColumnIds\":[15]}";
        Table table = metadataService.getTable(testSchemaName, testTblName);
        Layout layout = metadataService.getLatestLayout(testSchemaName, testTblName);

        MetadataProto.SinglePointIndex.Builder singlePointIndexbuilder = MetadataProto.SinglePointIndex.newBuilder();
        singlePointIndexbuilder.setId(0L)
                .setKeyColumns(keyColumn)
                .setPrimary(true)
                .setUnique(true)
                .setIndexScheme("rocksdb")
                .setTableId(table.getId())
                .setSchemaVersionId(layout.getSchemaVersionId());

        SinglePointIndex index = new SinglePointIndex(singlePointIndexbuilder.build());
        boolean result = metadataService.createSinglePointIndex(index);
        Assertions.assertTrue(result);
        boolean pause = true;
    }

    @Test
    public void testCreateIndex() throws MetadataException {
        String testSchemaName = "pixels_index";
        String testTblName = "ray_index";
        String keyColumn = "{\"keyColumnIds\":[11]}";
        Table table = metadataService.getTable(testSchemaName, testTblName);
        long id = table.getId();
        long schemaId = table.getSchemaId();
        Layout layout = metadataService.getLatestLayout(testSchemaName, testTblName);

        MetadataProto.SinglePointIndex.Builder singlePointIndexbuilder = MetadataProto.SinglePointIndex.newBuilder();
        singlePointIndexbuilder.setId(0L)
                .setKeyColumns(keyColumn)
                .setPrimary(true)
                .setUnique(true)
                .setIndexScheme("rocksdb")
                .setTableId(table.getId())
                .setSchemaVersionId(layout.getSchemaVersionId());

        SinglePointIndex index = new SinglePointIndex(singlePointIndexbuilder.build());
        boolean result = metadataService.createSinglePointIndex(index);
        Assertions.assertTrue(result);
        boolean pause = true;
    }

    @Test
    public void testGetIndex() throws MetadataException {
        String testSchemaName = "pixels_index";
        String testTblName = "ray_index";
        Table table = metadataService.getTable(testSchemaName, testTblName);
        long id = table.getId();
        SinglePointIndex index = metadataService.getPrimaryIndex(id);

        Assertions.assertNotNull(index);
        boolean pause = true;
    }

    @Test
    public void testGetRowID() throws MetadataException {
        int numRowIds = 10000;
        IndexProto.RowIdBatch rowIdBatch = indexService.allocateRowIdBatch(4, numRowIds);
        Assertions.assertEquals(rowIdBatch.getLength(), numRowIds);
        boolean pause = true;
    }
}

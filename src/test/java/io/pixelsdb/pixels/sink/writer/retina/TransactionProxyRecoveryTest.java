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

package io.pixelsdb.pixels.sink.writer.retina;

import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.writer.retina.recovery.RecoveryManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TransactionProxyRecoveryTest
{
    @TempDir
    Path tempDir;

    @AfterEach
    void tearDown()
    {
        RecoveryManager.closeInstance();
        PixelsSinkConfigFactory.reset();
        TransactionProxy.staticClose();
    }

    @Test
    void shouldConsumeRecoveredPreallocatedTransactionsBeforeRequestingNewOnes()
    {
        initializeRecoveryConfig(tempDir.resolve("rocksdb"));

        RecoveryManager manager = RecoveryManager.getInstance();
        manager.recordPreallocatedTransContexts(List.of(
                new TransContext(8101L, 8201L, 8301L, 8401L, false),
                new TransContext(8102L, 8202L, 8302L, 8402L, false)
        ));
        manager.initializeForSourceStart();

        TransactionProxy proxy = new TransactionProxy(
                null,
                manager,
                true,
                10,
                10,
                1,
                10,
                "row",
                false
        );

        TransContext first = proxy.getNewTransContext("source-tx-1");
        TransContext second = proxy.getNewTransContext("source-tx-2");

        assertEquals(8101L, first.getTransId());
        assertEquals(8201L, first.getTimestamp());
        assertEquals(8102L, second.getTransId());
        assertEquals(8202L, second.getTimestamp());
    }

    private void initializeRecoveryConfig(Path rocksdbDir)
    {
        ConfigFactory configFactory = ConfigFactory.Instance();
        configFactory.addProperty("sink.datasource", "storage");
        configFactory.addProperty("sink.mode", "retina");
        configFactory.addProperty("sink.trans.mode", "batch");
        configFactory.addProperty("sink.recovery.enable", "true");
        configFactory.addProperty("sink.recovery.mode", "recovery");
        configFactory.addProperty("sink.recovery.bootstrap.force_overwrite", "false");
        configFactory.addProperty("sink.recovery.rocksdb.dir", rocksdbDir.toString());
        configFactory.addProperty("sink.recovery.insert_as_update", "true");
        configFactory.addProperty("sink.retina.trans.request.batch", "true");
        configFactory.addProperty("sink.retina.trans.request.batch.size", "10");
        configFactory.addProperty("sink.commit.batch.size", "10");
        configFactory.addProperty("sink.commit.batch.worker", "1");
        configFactory.addProperty("sink.commit.batch.delay", "10");
        configFactory.addProperty("sink.monitor.enable", "false");
        configFactory.addProperty("sink.monitor.report.enable", "false");
        configFactory.addProperty("sink.monitor.freshness.level", "row");
        PixelsSinkConfigFactory.initialize(configFactory);
    }
}

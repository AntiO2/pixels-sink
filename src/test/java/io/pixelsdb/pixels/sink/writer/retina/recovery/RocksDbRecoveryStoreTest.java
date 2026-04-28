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

package io.pixelsdb.pixels.sink.writer.retina.recovery;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.provider.ProtoType;
import io.pixelsdb.pixels.sink.source.storage.StorageSourceOffset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RocksDbRecoveryStoreTest
{
    @TempDir
    Path tempDir;

    @AfterEach
    void tearDown()
    {
        RecoveryManager.closeInstance();
        PixelsSinkConfigFactory.reset();
    }

    @Test
    void shouldPersistBindingAdvanceOffsetAndCommit()
    {
        initializeRecoveryConfig(tempDir.resolve("rocksdb"), "recovery", false);
        StorageSourceOffset beginOffset = new StorageSourceOffset(0, 128L, 0, ProtoType.TRANS);
        StorageSourceOffset rowOffset = new StorageSourceOffset(0, 256L, 0, ProtoType.ROW);
        StorageSourceOffset earlierRowOffset = new StorageSourceOffset(0, 192L, 0, ProtoType.ROW);
        RecoveryBinding binding = new RecoveryBinding(
                "tx-1",
                101L,
                10001L,
                20001L,
                30001L,
                beginOffset,
                null,
                RecoveryState.ACTIVE
        );

        try (RocksDbRecoveryStore store = new RocksDbRecoveryStore())
        {
            assertFalse(store.hasRecoveryState());

            store.saveNewBinding(binding);
            assertTrue(store.hasRecoveryState());

            RecoveryBinding stored = store.getBinding("tx-1").orElseThrow();
            assertEquals(101L, stored.getPixelsTransId());
            assertEquals(beginOffset, stored.getBeginOffset());
            assertEquals(RecoveryState.ACTIVE, stored.getState());

            List<RecoveryBinding> activeBindings = store.loadActiveBindings();
            assertEquals(1, activeBindings.size());
            assertEquals("tx-1", activeBindings.get(0).getDataSourceTxId());

            store.saveTimestampReplayIndex(new TimestampReplayIndexEntry(10001L, "tx-1", 101L, beginOffset));
            assertEquals(beginOffset, store.findReplayIndexAtOrBefore(10001L).orElseThrow().getSourceOffset());
            assertEquals(beginOffset, store.findReplayIndexAtOrBefore(10002L).orElseThrow().getSourceOffset());

            store.advanceLastSafeOffset("tx-1", rowOffset);
            RecoveryBinding advanced = store.getBinding("tx-1").orElseThrow();
            assertEquals(rowOffset, advanced.getLastSafeOffset());

            store.advanceLastSafeOffset("tx-1", earlierRowOffset);
            RecoveryBinding regressed = store.getBinding("tx-1").orElseThrow();
            assertEquals(earlierRowOffset, regressed.getLastSafeOffset());

            store.markCommitting("tx-1");
            RecoveryBinding committing = store.getBinding("tx-1").orElseThrow();
            assertEquals(RecoveryState.COMMITTING, committing.getState());

            store.markCommitted("tx-1");
            assertTrue(store.hasCommitMarker("tx-1"));
            RecoveryBinding committed = store.getBinding("tx-1").orElseThrow();
            assertEquals(RecoveryState.COMMITTED, committed.getState());
            assertTrue(store.loadActiveBindings().isEmpty());
        }
    }

    @Test
    void shouldResetAllRecoveryState()
    {
        initializeRecoveryConfig(tempDir.resolve("rocksdb"), "bootstrap", false);
        RecoveryBinding binding = new RecoveryBinding(
                "tx-2",
                202L,
                20002L,
                21000L,
                31000L,
                new StorageSourceOffset(1, 64L, 0, ProtoType.TRANS),
                null,
                RecoveryState.ACTIVE
        );

        try (RocksDbRecoveryStore store = new RocksDbRecoveryStore())
        {
            store.saveNewBinding(binding);
            assertTrue(store.hasRecoveryState());

            store.reset();
            assertFalse(store.hasRecoveryState());
            assertTrue(store.getBinding("tx-2").isEmpty());
            assertFalse(store.hasCommitMarker("tx-2"));
        }
    }

    private void initializeRecoveryConfig(Path rocksdbDir, String mode, boolean forceOverwrite)
    {
        ConfigFactory configFactory = ConfigFactory.Instance();
        configFactory.addProperty("sink.datasource", "storage");
        configFactory.addProperty("sink.mode", "retina");
        configFactory.addProperty("sink.trans.mode", "batch");
        configFactory.addProperty("sink.recovery.enable", "true");
        configFactory.addProperty("sink.recovery.mode", mode);
        configFactory.addProperty("sink.recovery.bootstrap.force_overwrite", Boolean.toString(forceOverwrite));
        configFactory.addProperty("sink.recovery.rocksdb.dir", rocksdbDir.toString());
        configFactory.addProperty("sink.recovery.insert_as_update", "true");
        PixelsSinkConfigFactory.initialize(configFactory);
    }
}

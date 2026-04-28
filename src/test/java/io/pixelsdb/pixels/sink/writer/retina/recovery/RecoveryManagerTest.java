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

import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.provider.ProtoType;
import io.pixelsdb.pixels.sink.source.storage.StorageSourceOffset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class RecoveryManagerTest
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
    void shouldRecordBindingAndRestoreExistingContext()
    {
        Path rocksdbDir = tempDir.resolve("restore-existing");
        initializeRecoveryConfig(rocksdbDir, "recovery", false);

        RecoveryManager manager = RecoveryManager.getInstance();
        String txId = "tx-restore";
        StorageSourceOffset beginOffset = new StorageSourceOffset(0, 10L, 0, ProtoType.TRANS);
        manager.observeTransactionMetadata(beginTx(txId), beginOffset);

        TransContext original = new TransContext(501L, 601L, 701L, 801L, false);
        manager.recordNewBinding(txId, original);

        RecoveryBinding binding = manager.getBinding(txId).orElseThrow();
        assertEquals(original.getTransId(), binding.getPixelsTransId());
        assertEquals(original.getTimestamp(), binding.getTimestamp());
        assertEquals(beginOffset, binding.getBeginOffset());

        TransContext restored = manager.restoreTransContext(txId, transId ->
        {
            assertEquals(original.getTransId(), transId);
            return original;
        }).orElseThrow();
        assertSame(original, restored);
    }

    @Test
    void shouldPickEarliestReplayStartOffsetInRecoveryMode()
    {
        Path rocksdbDir = tempDir.resolve("replay-start");
        initializeRecoveryConfig(rocksdbDir, "recovery", false);

        try (RocksDbRecoveryStore store = new RocksDbRecoveryStore())
        {
            store.saveNewBinding(new RecoveryBinding(
                    "tx-late",
                    1001L,
                    1101L,
                    1201L,
                    1301L,
                    new StorageSourceOffset(1, 200L, 1, ProtoType.TRANS),
                    null,
                    RecoveryState.ACTIVE
            ));
            store.saveNewBinding(new RecoveryBinding(
                    "tx-early",
                    1002L,
                    1102L,
                    1202L,
                    1302L,
                    new StorageSourceOffset(0, 100L, 0, ProtoType.TRANS),
                    null,
                    RecoveryState.ACTIVE
            ));
        }

        RecoveryManager.closeInstance();
        PixelsSinkConfigFactory.reset();
        initializeRecoveryConfig(rocksdbDir, "recovery", false);

        RecoveryManager manager = RecoveryManager.getInstance();
        manager.initializeForSourceStart();

        StorageSourceOffset replayStart = manager.getReplayStartOffset();
        assertNotNull(replayStart);
        assertEquals(new StorageSourceOffset(0, 100L, 0, ProtoType.TRANS), replayStart);
        assertEquals(RecoveryReplayRole.ACTIVE_RECOVERY, manager.resolveReplayRole("tx-early"));
        assertEquals(RecoveryReplayRole.ACTIVE_RECOVERY, manager.resolveReplayRole("tx-late"));
        assertTrue(manager.shouldRewriteInsert("tx-early"));
        assertTrue(manager.shouldRewriteInsert("tx-late"));
    }

    @Test
    void shouldSkipDuplicateCommitButStillTransformCommittedReplay()
    {
        Path rocksdbDir = tempDir.resolve("commit-marker");
        initializeRecoveryConfig(rocksdbDir, "recovery", false);

        RecoveryManager manager = RecoveryManager.getInstance();
        String txId = "tx-commit";
        manager.observeTransactionMetadata(beginTx(txId), new StorageSourceOffset(0, 15L, 0, ProtoType.TRANS));
        manager.recordNewBinding(txId, new TransContext(9001L, 9002L, 9003L, 9004L, false));

        assertEquals(RecoveryReplayRole.FORWARD_CONSUMPTION, manager.resolveReplayRole(txId));
        assertFalse(manager.shouldSuppressCommit(txId));
        assertFalse(manager.shouldRewriteInsert(txId));

        manager.markCommitted(txId);

        assertEquals(RecoveryReplayRole.CHECKPOINT_REPLAY, manager.resolveReplayRole(txId));
        assertTrue(manager.shouldSuppressCommit(txId));
        assertTrue(manager.shouldRewriteInsert(txId));
        assertTrue(manager.shouldTrackCommitProgress("fresh-tx"));
    }

    @Test
    void shouldClearExistingStateInBootstrapForceOverwriteMode()
    {
        Path rocksdbDir = tempDir.resolve("bootstrap-overwrite");
        initializeRecoveryConfig(rocksdbDir, "recovery", false);
        try (RocksDbRecoveryStore store = new RocksDbRecoveryStore())
        {
            store.saveNewBinding(new RecoveryBinding(
                    "tx-bootstrap",
                    2001L,
                    2101L,
                    2201L,
                    2301L,
                    new StorageSourceOffset(0, 1L, 0, ProtoType.TRANS),
                    null,
                    RecoveryState.ACTIVE
            ));
        }

        RecoveryManager.closeInstance();
        PixelsSinkConfigFactory.reset();
        initializeRecoveryConfig(rocksdbDir, "bootstrap", true);

        RecoveryManager manager = RecoveryManager.getInstance();
        manager.initializeForSourceStart();
        assertNull(manager.getReplayStartOffset());
        assertTrue(manager.getBinding("tx-bootstrap").isEmpty());

        RecoveryManager.closeInstance();
        try (RocksDbRecoveryStore store = new RocksDbRecoveryStore())
        {
            assertFalse(store.hasRecoveryState());
        }
    }

    @Test
    void shouldRejectForceOverwriteInRecoveryMode()
    {
        initializeRecoveryConfig(tempDir.resolve("invalid-config"), "recovery", true);

        RecoveryManager manager = RecoveryManager.getInstance();
        IllegalStateException exception = assertThrows(IllegalStateException.class, manager::initializeForSourceStart);
        assertTrue(exception.getMessage().contains("force_overwrite"));
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

    private SinkProto.TransactionMetadata beginTx(String txId)
    {
        return SinkProto.TransactionMetadata.newBuilder()
                .setId(txId)
                .setStatus(SinkProto.TransactionStatus.BEGIN)
                .build();
    }
}

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
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.provider.ProtoType;
import io.pixelsdb.pixels.sink.source.storage.StorageSourceOffset;
import io.pixelsdb.pixels.sink.writer.retina.recovery.RecoveryManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SinkContextManagerRecoveryTest
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
    void shouldSuppressCommitForRecoveredCommittedTransaction()
    {
        initializeRecoveryConfig(tempDir.resolve("suppress-commit"));
        RecoveryManager manager = RecoveryManager.getInstance();
        String txId = "tx-commit-suppressed";
        manager.observeTransactionMetadata(
                SinkProto.TransactionMetadata.newBuilder()
                        .setId(txId)
                        .setStatus(SinkProto.TransactionStatus.BEGIN)
                        .build(),
                new StorageSourceOffset(0, 10L, 0, ProtoType.TRANS)
        );
        manager.recordNewBinding(txId, new TransContext(9001L, 9002L, 9003L, 9004L, false));
        manager.markCommitted(txId);

        RecordingTransactionProxy proxy = new RecordingTransactionProxy(manager);
        SinkContextManager contextManager = new SinkContextManager(proxy, manager, "sync", "row", 16);
        SinkContext context = new SinkContext(txId, new TransContext(9001L, 9002L, 9003L, 9004L, false));
        context.setEndTx(SinkProto.TransactionMetadata.newBuilder()
                .setId(txId)
                .setStatus(SinkProto.TransactionStatus.END)
                .build());

        contextManager.endTransaction(context);

        assertEquals(0, proxy.syncCommitCount);
        assertEquals(0, proxy.asyncCommitCount);
    }

    @Test
    void shouldCommitForwardTransactionNormally()
    {
        initializeRecoveryConfig(tempDir.resolve("forward-commit"));
        RecoveryManager manager = RecoveryManager.getInstance();
        RecordingTransactionProxy proxy = new RecordingTransactionProxy(manager);
        SinkContextManager contextManager = new SinkContextManager(proxy, manager, "sync", "row", 16);
        SinkContext context = new SinkContext("tx-forward", new TransContext(9101L, 9102L, 9103L, 9104L, false));
        context.setEndTx(SinkProto.TransactionMetadata.newBuilder()
                .setId("tx-forward")
                .setStatus(SinkProto.TransactionStatus.END)
                .build());

        contextManager.endTransaction(context);

        assertEquals(1, proxy.syncCommitCount);
        assertEquals(0, proxy.asyncCommitCount);
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
        configFactory.addProperty("sink.commit.method", "sync");
        configFactory.addProperty("sink.monitor.enable", "false");
        configFactory.addProperty("sink.monitor.report.enable", "false");
        configFactory.addProperty("sink.monitor.freshness.level", "row");
        PixelsSinkConfigFactory.initialize(configFactory);
    }

    private static final class RecordingTransactionProxy extends TransactionProxy
    {
        private int syncCommitCount;
        private int asyncCommitCount;

        private RecordingTransactionProxy(RecoveryManager recoveryManager)
        {
            super(null, recoveryManager, false, 1, 1, 1, 1, "row", false);
        }

        @Override
        public void commitTransSync(SinkContext transContext)
        {
            syncCommitCount++;
        }

        @Override
        public void commitTransAsync(SinkContext transContext)
        {
            asyncCommitCount++;
        }
    }
}

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
import io.pixelsdb.pixels.common.transaction.TransService;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.source.storage.StorageSourceOffset;
import io.pixelsdb.pixels.sink.writer.PixelsSinkMode;
import io.pixelsdb.pixels.sink.writer.retina.TransactionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RecoveryManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RecoveryManager.class);
    private static volatile RecoveryManager instance;

    private final PixelsSinkConfig config;
    private final boolean enabled;
    private final RecoveryMode mode;
    private final RecoveryStore store;
    private final TransService transService;
    private final Set<String> recoveredTransactionIds = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<String, StorageSourceOffset> pendingBeginOffsets = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, StorageSourceOffset> pendingRowOffsets = new ConcurrentHashMap<>();
    private volatile boolean initializedForSourceStart = false;
    private volatile StorageSourceOffset replayStartOffset;

    private RecoveryManager()
    {
        this.config = PixelsSinkConfigFactory.getInstance();
        this.enabled = config.isRecoveryEnabled()
                && "storage".equalsIgnoreCase(config.getDataSource())
                && config.getPixelsSinkMode() == PixelsSinkMode.RETINA
                && config.getTransactionMode() == TransactionMode.BATCH;
        this.mode = RecoveryMode.fromValue(config.getRecoveryMode());
        this.store = enabled ? new RocksDbRecoveryStore() : null;
        this.transService = enabled ? TransService.Instance() : null;
    }

    public static RecoveryManager getInstance()
    {
        if (instance == null)
        {
            synchronized (RecoveryManager.class)
            {
                if (instance == null)
                {
                    instance = new RecoveryManager();
                }
            }
        }
        return instance;
    }

    public static void closeInstance()
    {
        if (instance != null)
        {
            try
            {
                instance.close();
            } catch (IOException e)
            {
                throw new RuntimeException("Failed to close recovery manager", e);
            } finally
            {
                instance = null;
            }
        }
    }

    public boolean isEnabled()
    {
        return enabled;
    }

    public RecoveryMode getMode()
    {
        return mode;
    }

    public synchronized void initializeForSourceStart()
    {
        if (!enabled || initializedForSourceStart)
        {
            return;
        }
        if (mode == RecoveryMode.BOOTSTRAP)
        {
            if (store.hasRecoveryState())
            {
                if (config.isRecoveryBootstrapForceOverwrite())
                {
                    LOGGER.warn("Recovery bootstrap force overwrite enabled; clearing previous recovery state");
                    store.reset();
                } else
                {
                    throw new IllegalStateException("Recovery state exists. Set sink.recovery.bootstrap.force_overwrite=true to discard it.");
                }
            }
            replayStartOffset = null;
        } else
        {
            if (config.isRecoveryBootstrapForceOverwrite())
            {
                throw new IllegalStateException("sink.recovery.bootstrap.force_overwrite can only be used in bootstrap mode");
            }
            List<RecoveryBinding> bindings = store.loadActiveBindings();
            bindings.stream()
                    .map(RecoveryBinding::getDataSourceTxId)
                    .forEach(recoveredTransactionIds::add);
            StorageSourceOffset earliestActiveOffset = bindings.stream()
                    .map(RecoveryBinding::getBeginOffset)
                    .min(Comparator.naturalOrder())
                    .orElse(null);
            StorageSourceOffset checkpointOffset = loadCheckpointReplayOffset();
            replayStartOffset = minOffset(earliestActiveOffset, checkpointOffset);
        }
        initializedForSourceStart = true;
    }

    public StorageSourceOffset getReplayStartOffset()
    {
        return replayStartOffset;
    }

    public void observeTransactionMetadata(SinkProto.TransactionMetadata transactionMetadata, StorageSourceOffset offset)
    {
        if (!enabled || transactionMetadata == null || offset == null)
        {
            return;
        }
        if (transactionMetadata.getStatus() == SinkProto.TransactionStatus.BEGIN)
        {
            pendingBeginOffsets.putIfAbsent(transactionMetadata.getId(), offset);
        }
    }

    public void observeRowEvent(RowChangeEvent event)
    {
        if (!enabled || event == null || event.getSourceOffset() == null || event.getTransaction() == null)
        {
            return;
        }
        pendingRowOffsets.putIfAbsent(event.getTransaction().getId(), event.getSourceOffset());
    }

    public Optional<RecoveryBinding> getBinding(String dataSourceTxId)
    {
        if (!enabled)
        {
            return Optional.empty();
        }
        return store.getBinding(dataSourceTxId);
    }

    public RecoveryReplayRole resolveReplayRole(String dataSourceTxId)
    {
        if (!enabled || mode != RecoveryMode.RECOVERY || dataSourceTxId == null)
        {
            return RecoveryReplayRole.FORWARD_CONSUMPTION;
        }
        if (store.hasCommitMarker(dataSourceTxId))
        {
            return RecoveryReplayRole.CHECKPOINT_REPLAY;
        }
        if (recoveredTransactionIds.contains(dataSourceTxId))
        {
            return RecoveryReplayRole.ACTIVE_RECOVERY;
        }
        return RecoveryReplayRole.FORWARD_CONSUMPTION;
    }

    public boolean shouldSuppressCommit(String dataSourceTxId)
    {
        return resolveReplayRole(dataSourceTxId) == RecoveryReplayRole.CHECKPOINT_REPLAY;
    }

    @Deprecated
    public boolean shouldSkipTransaction(String dataSourceTxId)
    {
        return shouldSuppressCommit(dataSourceTxId);
    }

    public boolean shouldReplayRow(RowChangeEvent event)
    {
        return event != null;
    }

    @Deprecated
    public boolean shouldSkipRow(RowChangeEvent event)
    {
        return !shouldReplayRow(event);
    }

    public boolean shouldRewriteInsert(String dataSourceTxId)
    {
        return enabled
                && mode == RecoveryMode.RECOVERY
                && config.isRecoveryInsertAsUpdate()
                && dataSourceTxId != null
                && resolveReplayRole(dataSourceTxId) != RecoveryReplayRole.FORWARD_CONSUMPTION;
    }

    @Deprecated
    public boolean shouldTransformInsert(String dataSourceTxId)
    {
        return shouldRewriteInsert(dataSourceTxId);
    }

    public boolean shouldTrackCommitProgress(String dataSourceTxId)
    {
        return !shouldSuppressCommit(dataSourceTxId);
    }

    public void recordNewBinding(String dataSourceTxId, TransContext transContext)
    {
        if (!enabled || transContext == null || dataSourceTxId == null || store.hasCommitMarker(dataSourceTxId) || store.getBinding(dataSourceTxId).isPresent())
        {
            return;
        }
        StorageSourceOffset beginOffset = pendingBeginOffsets.remove(dataSourceTxId);
        if (beginOffset == null)
        {
            beginOffset = pendingRowOffsets.get(dataSourceTxId);
        }
        if (beginOffset == null)
        {
            LOGGER.warn("No source offset found for transaction {}, recovery metadata will not be recorded", dataSourceTxId);
            return;
        }
        RecoveryBinding binding = new RecoveryBinding(
                dataSourceTxId,
                transContext.getTransId(),
                transContext.getTimestamp(),
                transContext.getLease().getStartMs(),
                transContext.getLease().getPeriodMs(),
                beginOffset,
                null,
                RecoveryState.ACTIVE
        );
        store.saveNewBinding(binding);
        store.saveTimestampReplayIndex(new TimestampReplayIndexEntry(
                transContext.getTimestamp(),
                dataSourceTxId,
                transContext.getTransId(),
                beginOffset
        ));
    }

    public void markCommitting(String dataSourceTxId)
    {
        if (!enabled)
        {
            return;
        }
        store.markCommitting(dataSourceTxId);
    }

    public void markCommitted(String dataSourceTxId)
    {
        if (!enabled)
        {
            return;
        }
        store.markCommitted(dataSourceTxId);
        recoveredTransactionIds.remove(dataSourceTxId);
        pendingBeginOffsets.remove(dataSourceTxId);
        pendingRowOffsets.remove(dataSourceTxId);
    }

    public void advanceLastSafeOffset(String dataSourceTxId, StorageSourceOffset offset)
    {
        if (!enabled || dataSourceTxId == null || offset == null)
        {
            return;
        }
        store.advanceLastSafeOffset(dataSourceTxId, offset);
    }

    public Optional<TransContext> restoreTransContext(String dataSourceTxId, TransactionContextLoader loader)
    {
        if (!enabled)
        {
            return Optional.empty();
        }
        return store.getBinding(dataSourceTxId).map(binding ->
        {
            try
            {
                if (shouldSuppressCommit(dataSourceTxId))
                {
                    return buildReplayOnlyTransContext(binding);
                }
                return loader.load(binding.getPixelsTransId());
            } catch (Exception e)
            {
                throw new RuntimeException("Failed to restore transaction context for " + dataSourceTxId, e);
            }
        });
    }

    private TransContext buildReplayOnlyTransContext(RecoveryBinding binding)
    {
        return new TransContext(
                binding.getPixelsTransId(),
                binding.getTimestamp(),
                binding.getLeaseStartMs(),
                binding.getLeasePeriodMs(),
                false
        );
    }

    public void close() throws IOException
    {
        if (store != null)
        {
            store.close();
        }
    }

    private StorageSourceOffset loadCheckpointReplayOffset()
    {
        try
        {
            long safeGcTimestamp = transService.getSafeGcTimestamp();
            return store.findReplayIndexAtOrBefore(safeGcTimestamp)
                    .map(TimestampReplayIndexEntry::getSourceOffset)
                    .orElse(null);
        } catch (Exception e)
        {
            LOGGER.warn("Failed to resolve recovery checkpoint from safe gc timestamp", e);
            return null;
        }
    }

    private StorageSourceOffset minOffset(StorageSourceOffset left, StorageSourceOffset right)
    {
        if (left == null)
        {
            return right;
        }
        if (right == null)
        {
            return left;
        }
        return left.compareTo(right) <= 0 ? left : right;
    }

    @FunctionalInterface
    public interface TransactionContextLoader
    {
        TransContext load(long transId) throws Exception;
    }
}

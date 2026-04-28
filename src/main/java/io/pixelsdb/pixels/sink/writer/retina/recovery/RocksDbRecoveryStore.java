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

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.source.storage.StorageSourceOffset;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class RocksDbRecoveryStore implements RecoveryStore
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDbRecoveryStore.class);
    private static final byte[] TXN_BINDING_CF = "txn_binding_cf".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ACTIVE_TX_ORDER_CF = "active_tx_order_cf".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COMMIT_MARKER_CF = "commit_marker_cf".getBytes(StandardCharsets.UTF_8);
    private static final byte[] TS_REPLAY_INDEX_CF = "ts_replay_index_cf".getBytes(StandardCharsets.UTF_8);

    static
    {
        RocksDB.loadLibrary();
    }

    private final RocksDB db;
    private final ColumnFamilyHandle txnBindingHandle;
    private final ColumnFamilyHandle activeTxOrderHandle;
    private final ColumnFamilyHandle commitMarkerHandle;
    private final ColumnFamilyHandle tsReplayIndexHandle;
    private final DBOptions dbOptions;
    private final List<ColumnFamilyHandle> handles;
    private final WriteOptions writeOptions = new WriteOptions();

    public RocksDbRecoveryStore()
    {
        PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
        try
        {
            Path dbPath = Path.of(config.getRecoveryRocksdbDir());
            Files.createDirectories(dbPath);
            List<ColumnFamilyDescriptor> descriptors = List.of(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()),
                    new ColumnFamilyDescriptor(TXN_BINDING_CF, new ColumnFamilyOptions()),
                    new ColumnFamilyDescriptor(ACTIVE_TX_ORDER_CF, new ColumnFamilyOptions()),
                    new ColumnFamilyDescriptor(COMMIT_MARKER_CF, new ColumnFamilyOptions()),
                    new ColumnFamilyDescriptor(TS_REPLAY_INDEX_CF, new ColumnFamilyOptions())
            );
            this.dbOptions = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true);
            this.handles = new ArrayList<>();
            this.db = RocksDB.open(dbOptions, dbPath.toString(), descriptors, handles);
            Map<String, ColumnFamilyHandle> handleMap = new HashMap<>();
            for (int i = 0; i < descriptors.size(); i++)
            {
                handleMap.put(new String(descriptors.get(i).getName(), StandardCharsets.UTF_8), handles.get(i));
            }
            this.txnBindingHandle = handleMap.get(new String(TXN_BINDING_CF, StandardCharsets.UTF_8));
            this.activeTxOrderHandle = handleMap.get(new String(ACTIVE_TX_ORDER_CF, StandardCharsets.UTF_8));
            this.commitMarkerHandle = handleMap.get(new String(COMMIT_MARKER_CF, StandardCharsets.UTF_8));
            this.tsReplayIndexHandle = handleMap.get(new String(TS_REPLAY_INDEX_CF, StandardCharsets.UTF_8));
        } catch (IOException | RocksDBException e)
        {
            throw new RuntimeException("Failed to initialize RocksDB recovery store", e);
        }
    }

    @Override
    public boolean hasRecoveryState()
    {
        return hasAnyKey(txnBindingHandle) || hasAnyKey(commitMarkerHandle) || hasAnyKey(activeTxOrderHandle)
                || hasAnyKey(tsReplayIndexHandle);
    }

    @Override
    public void reset()
    {
        clearColumnFamily(txnBindingHandle);
        clearColumnFamily(activeTxOrderHandle);
        clearColumnFamily(commitMarkerHandle);
        clearColumnFamily(tsReplayIndexHandle);
    }

    @Override
    public Optional<RecoveryBinding> getBinding(String dataSourceTxId)
    {
        try
        {
            byte[] raw = db.get(txnBindingHandle, key(dataSourceTxId));
            if (raw == null)
            {
                return Optional.empty();
            }
            return Optional.of(deserialize(raw, RecoveryBinding.class));
        } catch (RocksDBException e)
        {
            throw new RuntimeException("Failed to read recovery binding: " + dataSourceTxId, e);
        }
    }

    @Override
    public List<RecoveryBinding> loadActiveBindings()
    {
        List<RecoveryBinding> bindings = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator(activeTxOrderHandle))
        {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next())
            {
                String txId = new String(iterator.value(), StandardCharsets.UTF_8);
                getBinding(txId)
                        .filter(binding -> binding.getState() != RecoveryState.COMMITTED)
                        .ifPresent(bindings::add);
            }
        }
        return bindings;
    }

    @Override
    public boolean hasCommitMarker(String dataSourceTxId)
    {
        try
        {
            return db.get(commitMarkerHandle, key(dataSourceTxId)) != null;
        } catch (RocksDBException e)
        {
            throw new RuntimeException("Failed to query commit marker: " + dataSourceTxId, e);
        }
    }

    @Override
    public void saveNewBinding(RecoveryBinding binding)
    {
        try (WriteBatch batch = new WriteBatch())
        {
            batch.put(txnBindingHandle, key(binding.getDataSourceTxId()), serialize(binding));
            batch.put(activeTxOrderHandle, activeOrderKey(binding.getBeginOffset(), binding.getDataSourceTxId()), key(binding.getDataSourceTxId()));
            db.write(writeOptions, batch);
        } catch (RocksDBException e)
        {
            throw new RuntimeException("Failed to save new recovery binding: " + binding.getDataSourceTxId(), e);
        }
    }

    @Override
    public void saveTimestampReplayIndex(TimestampReplayIndexEntry entry)
    {
        try
        {
            db.put(tsReplayIndexHandle, writeOptions, timestampKey(entry.getPixelsTimestamp()), serialize(entry));
        } catch (RocksDBException e)
        {
            throw new RuntimeException("Failed to persist timestamp replay index: " + entry.getPixelsTimestamp(), e);
        }
    }

    @Override
    public Optional<TimestampReplayIndexEntry> findReplayIndexAtOrBefore(long pixelsTimestamp)
    {
        try (RocksIterator iterator = db.newIterator(tsReplayIndexHandle))
        {
            byte[] seekKey = timestampKey(pixelsTimestamp);
            iterator.seekForPrev(seekKey);
            if (!iterator.isValid())
            {
                return Optional.empty();
            }
            return Optional.of(deserialize(iterator.value(), TimestampReplayIndexEntry.class));
        }
    }

    @Override
    public void advanceLastSafeOffset(String dataSourceTxId, StorageSourceOffset offset)
    {
        getBinding(dataSourceTxId).ifPresent(binding ->
        {
            binding.setLastSafeOffset(offset);
            persistBinding(binding);
        });
    }

    @Override
    public void markCommitting(String dataSourceTxId)
    {
        getBinding(dataSourceTxId).ifPresent(binding ->
        {
            if (binding.getState() == RecoveryState.COMMITTED || hasCommitMarker(dataSourceTxId))
            {
                return;
            }
            binding.setState(RecoveryState.COMMITTING);
            persistBinding(binding);
        });
    }

    @Override
    public void markCommitted(String dataSourceTxId)
    {
        getBinding(dataSourceTxId).ifPresent(binding ->
        {
            binding.setState(RecoveryState.COMMITTED);
            CommitMarker marker = new CommitMarker(dataSourceTxId, binding.getPixelsTransId(), System.currentTimeMillis());
            try (WriteBatch batch = new WriteBatch())
            {
                batch.put(txnBindingHandle, key(dataSourceTxId), serialize(binding));
                batch.put(commitMarkerHandle, key(dataSourceTxId), serialize(marker));
                batch.delete(activeTxOrderHandle, activeOrderKey(binding.getBeginOffset(), dataSourceTxId));
                db.write(writeOptions, batch);
            } catch (RocksDBException e)
            {
                throw new RuntimeException("Failed to mark committed: " + dataSourceTxId, e);
            }
        });
    }

    @Override
    public void close()
    {
        writeOptions.close();
        handles.forEach(ColumnFamilyHandle::close);
        db.close();
        dbOptions.close();
    }

    private void persistBinding(RecoveryBinding binding)
    {
        try
        {
            db.put(txnBindingHandle, writeOptions, key(binding.getDataSourceTxId()), serialize(binding));
        } catch (RocksDBException e)
        {
            throw new RuntimeException("Failed to persist recovery binding: " + binding.getDataSourceTxId(), e);
        }
    }

    private boolean hasAnyKey(ColumnFamilyHandle handle)
    {
        try (RocksIterator iterator = db.newIterator(handle))
        {
            iterator.seekToFirst();
            return iterator.isValid();
        }
    }

    private void clearColumnFamily(ColumnFamilyHandle handle)
    {
        List<byte[]> keys = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator(handle))
        {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next())
            {
                keys.add(Arrays.copyOf(iterator.key(), iterator.key().length));
            }
        }
        try (WriteBatch batch = new WriteBatch())
        {
            for (byte[] key : keys)
            {
                batch.delete(handle, key);
            }
            db.write(writeOptions, batch);
        } catch (RocksDBException e)
        {
            throw new RuntimeException("Failed to clear recovery column family", e);
        }
    }

    private static byte[] key(String value)
    {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] timestampKey(long pixelsTimestamp)
    {
        return ByteBuffer.allocate(Long.BYTES).putLong(pixelsTimestamp).array();
    }

    private static byte[] activeOrderKey(StorageSourceOffset offset, String dataSourceTxId)
    {
        byte[] txIdBytes = dataSourceTxId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES * 2 + Long.BYTES + txIdBytes.length);
        buffer.putInt(offset.getEpoch());
        buffer.putInt(offset.getFileId());
        buffer.putLong(offset.getByteOffset());
        buffer.put(txIdBytes);
        return buffer.array();
    }

    private static byte[] serialize(Serializable value)
    {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream))
        {
            objectOutputStream.writeObject(value);
            objectOutputStream.flush();
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e)
        {
            throw new RuntimeException("Failed to serialize recovery metadata", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T deserialize(byte[] value, Class<T> type)
    {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(value);
             ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream))
        {
            Object object = objectInputStream.readObject();
            return (T) type.cast(object);
        } catch (IOException | ClassNotFoundException e)
        {
            throw new RuntimeException("Failed to deserialize recovery metadata", e);
        }
    }
}

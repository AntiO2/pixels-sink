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

package io.pixelsdb.pixels.sink.provider;

import com.google.protobuf.InvalidProtocolBufferException;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.source.storage.StorageSourceRecord;
import io.pixelsdb.pixels.sink.writer.retina.recovery.RecoveryManager;

import java.nio.ByteBuffer;

public class TransactionEventStorageLoopProvider<T> extends TransactionEventProvider<T>
{
    @Override
    SinkProto.TransactionMetadata convertToTargetRecord(T record)
    {
        StorageSourceRecord<ByteBuffer> buffer = (StorageSourceRecord<ByteBuffer>) record;
        try
        {
            ByteBuffer payload = buffer.getPayload().duplicate();
            payload.rewind();
            SinkProto.TransactionMetadata tx = SinkProto.TransactionMetadata.parseFrom(payload);
            SinkProto.TransactionMetadata.Builder builder = tx.toBuilder();
            builder.setId(builder.getId() + "_" + buffer.getOffset().getEpoch());
            SinkProto.TransactionMetadata transactionMetadata = builder.build();
            RecoveryManager.getInstance().observeTransactionMetadata(transactionMetadata, buffer.getOffset());
            return transactionMetadata;
        } catch (InvalidProtocolBufferException e)
        {
            throw new RuntimeException(e);
        }
    }
}

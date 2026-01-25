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
import io.pixelsdb.pixels.core.utils.Pair;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.event.deserializer.RowChangeEventStructDeserializer;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.util.DataTransform;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

public class TableEventStorageLoopProvider<T> extends TableEventProvider<T> {
    private final Logger LOGGER = Logger.getLogger(TableEventStorageProvider.class.getName());
    private final boolean freshness_embed;
    private final boolean freshness_timestamp;

    protected TableEventStorageLoopProvider() {
        super();
        PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
        String sinkMonitorFreshnessLevel = config.getSinkMonitorFreshnessLevel();
        if (sinkMonitorFreshnessLevel.equals("embed")) {
            freshness_embed = true;
        } else {
            freshness_embed = false;
        }
        freshness_timestamp = config.isSinkMonitorFreshnessTimestamp();
    }

    @Override
    RowChangeEvent convertToTargetRecord(T record) {
        Pair<ByteBuffer, Integer> pairRecord = (Pair<ByteBuffer, Integer>) record;
        ByteBuffer sourceRecord = pairRecord.getLeft();
        sourceRecord.rewind();
        Integer loopId = pairRecord.getRight();
        try {
            SinkProto.RowRecord rowRecord = SinkProto.RowRecord.parseFrom(sourceRecord);

            SinkProto.RowRecord.Builder rowRecordBuilder = rowRecord.toBuilder();
            if (freshness_timestamp) {
                DataTransform.updateRecordTimestamp(rowRecordBuilder, System.currentTimeMillis() * 1000);
            }

//            if(rowRecord.getSource().getTable().equals("transfer"))
//            {
//                DataTransform.transIdToBigint(rowRecordBuilder);
//            }

            SinkProto.TransactionInfo.Builder transactionBuilder = rowRecordBuilder.getTransactionBuilder();
            String id = transactionBuilder.getId();
            transactionBuilder.setId(id + "_" + loopId);
            rowRecordBuilder.setTransaction(transactionBuilder);
            return RowChangeEventStructDeserializer.convertToRowChangeEvent(rowRecordBuilder.build());
        } catch (InvalidProtocolBufferException | SinkException e) {
            LOGGER.warning(e.getMessage());
            return null;
        }
    }
}

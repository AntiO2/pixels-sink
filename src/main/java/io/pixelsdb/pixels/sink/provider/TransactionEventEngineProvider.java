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


import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.deserializer.TransactionStructMessageDeserializer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * @package: io.pixelsdb.pixels.sink.provider
 * @className: TransactionEventEngineProvider
 * @author: AntiO2
 * @date: 2025/9/25 13:20
 */
public class TransactionEventEngineProvider<T> extends TransactionEventProvider<T>
{

    public static final TransactionEventEngineProvider<SourceRecord> INSTANCE = new TransactionEventEngineProvider<>();

    public static TransactionEventEngineProvider<SourceRecord> getInstance()
    {
        return INSTANCE;
    }

    @Override
    SinkProto.TransactionMetadata convertToTargetRecord(T record)
    {
        SourceRecord sourceRecord = (SourceRecord) record;
        Struct value = (Struct) sourceRecord.value();
        return TransactionStructMessageDeserializer.convertToTransactionMetadata(value);
    }
}

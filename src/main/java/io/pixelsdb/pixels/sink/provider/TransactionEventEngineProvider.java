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


package io.pixelsdb.pixels.sink.provider;


import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.deserializer.TransactionStructMessageDeserializer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @package: io.pixelsdb.pixels.sink.event
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
        metricsFacade.recordSerdTxChange();
        return TransactionStructMessageDeserializer.convertToTransactionMetadata(value);
    }


}

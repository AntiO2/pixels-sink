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


package io.pixelsdb.pixels.sink.source;


import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.utils.Pair;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import io.pixelsdb.pixels.sink.processor.TransactionProcessor;
import io.pixelsdb.pixels.sink.provider.ProtoType;
import io.pixelsdb.pixels.sink.provider.TableProviderAndProcessorPipelineManager;
import io.pixelsdb.pixels.sink.provider.TransactionEventEngineProvider;
import io.pixelsdb.pixels.sink.provider.TransactionEventStorageProvider;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @package: io.pixelsdb.pixels.sink.processor
 * @className: LegacySinkStorageSource
 * @author: AntiO2
 * @date: 2025/10/5 11:43
 */
public class FasterSinkStorageSource extends AbstractSinkStorageSource implements SinkSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FasterSinkStorageSource.class);
    static SchemaTableName transactionSchemaTableName = new SchemaTableName("freak", "transaction");

    public FasterSinkStorageSource()
    {
        super();
    }

    private static String readString(ByteBuffer buffer, int len)
    {
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new String(bytes);
    }

    @Override
    ProtoType getProtoType(int i)
    {
        if (i == -1)
        {
            return ProtoType.TRANS;
        }
        return ProtoType.ROW;
    }

}

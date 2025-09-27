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

package io.pixelsdb.pixels.sink.sink;

import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.retina.RetinaService;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.concurrent.TransactionMode;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.processor.MetricsFacade;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class RetinaWriter implements PixelsSinkWriter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RetinaWriter.class);
    @Getter
    private static final PixelsSinkMode pixelsSinkMode = PixelsSinkMode.RETINA;
    private static final PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
    // private static final IndexService indexService = IndexService.Instance();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final RetinaService retinaService = RetinaService.Instance();
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private RetinaService.StreamHandle retinaStream = null;
    public RetinaWriter()
    {
        if (config.getTransactionMode() == TransactionMode.BATCH && config.getRetinaWriteMode() == RetinaWriteMode.STREAM)
        {
            retinaStream = retinaService.startUpdateStream();
        } else
        {
            retinaStream = null;
        }
    }

    @Override
    public void flush()
    {
    }

    @Override
    public boolean write(RowChangeEvent event)
    {
        if (isClosed.get())
        {
            LOGGER.warn("Attempted to write to closed writer");
            return false;
        }

        try
        {
            switch (event.getOp())
            {
                case INSERT:
                case SNAPSHOT:
                    return sendInsertRequest(event);
                case UPDATE:
                    return sendUpdateRequest(event);
                case DELETE:
                    return sendDeleteRequest(event);
                case UNRECOGNIZED:
                    break;
            }
        } catch (RetinaException e)
        {
            LOGGER.error("Retina write failed for event {}", event);
            return false;
        }
        // TODO: error handle
        return false;
    }

    @Override
    public boolean writeTrans(String schemaName, List<RetinaProto.TableUpdateData> tableUpdateData, long timestamp)
    {
        if (config.getRetinaWriteMode() == RetinaWriteMode.STUB)
        {
            try
            {
                LOGGER.debug("Retina Writer update record {}, TS: {}", schemaName, timestamp);
                retinaService.updateRecord(schemaName, tableUpdateData);
            } catch (RetinaException e)
            {
                e.printStackTrace();
                return false;
            }
        } else
        {
            retinaStream.updateRecord(schemaName, tableUpdateData);
        }
        return true;
    }

    @Override
    public boolean writeBatch(String schemaName, List<RetinaProto.TableUpdateData> tableUpdateData)
    {
        if (config.getRetinaWriteMode() == RetinaWriteMode.STUB)
        {
            try
            {
                retinaService.updateRecord(schemaName, tableUpdateData);
            } catch (RetinaException e)
            {
                e.printStackTrace();
                return false;
            }
        } else
        {
            retinaStream.updateRecord(schemaName, tableUpdateData);
        }
        return true;
    }

    @Deprecated
    private boolean sendInsertRequest(RowChangeEvent event) throws RetinaException
    {
        // Insert retina
        // boolean retinaServiceResult = retinaService.insertRecord(event.getSchemaName(), event.getTable(), event.getAfterData(), event.getTimeStamp());

        return false;
    }

    private boolean sendDeleteRequest(RowChangeEvent event)
    {
        return false;
//        RetinaProto.DeleteRecordResponse deleteRecordResponse = blockingStub.deleteRecord(getDeleteRecordRequest(event));
//        return deleteRecordResponse.getHeader().getErrorCode() == 0;
    }

    @Override
    public void close() throws IOException
    {
        if (isClosed.compareAndSet(false, true))
        {
//            try {
//                channel.shutdown();
//                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
//                    channel.shutdownNow();
//                }
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//                throw new IOException("Channel shutdown interrupted", e);
//            }
        }

        if (config.getTransactionMode() == TransactionMode.BATCH && config.getRetinaWriteMode() == RetinaWriteMode.STREAM)
        {
            retinaStream.close();
        }
    }

    private boolean sendUpdateRequest(RowChangeEvent event)
    {
        // Delete & Insert
//        RetinaProto.DeleteRecordResponse deleteRecordResponse = blockingStub.deleteRecord(getDeleteRecordRequest(event));
//        if (deleteRecordResponse.getHeader().getErrorCode() != 0) {
//            return false;
//        }
//
//        RetinaProto.InsertRecordResponse insertRecordResponse = blockingStub.insertRecord(getInsertRecordRequest(event));
//        return insertRecordResponse.getHeader().getErrorCode() == 0;
        return false;
    }

    public enum RetinaWriteMode
    {
        STREAM,
        STUB;

        public static RetinaWriteMode fromValue(String value)
        {
            for (RetinaWriteMode mode : values())
            {
                if (mode.name().equalsIgnoreCase(value))
                {
                    return mode;
                }
            }
            throw new RuntimeException(String.format("Can't convert %s to sink type", value));
        }
    }
}

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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.index.IndexService;
import io.pixelsdb.pixels.common.retina.RetinaService;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.retina.RetinaWorkerServiceGrpc;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.monitor.MetricsFacade;
import io.pixelsdb.pixels.sink.util.LatencySimulator;
import io.prometheus.client.Summary;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RetinaWriter implements PixelsSinkWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetinaWriter.class);
    @Getter
    private static final PixelsSinkMode pixelsSinkMode = PixelsSinkMode.RETINA;
    // private static final IndexService indexService = IndexService.Instance();


    private static final PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final RetinaService retinaService = RetinaService.Instance();

    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();

    public RetinaWriter() {
    }

    @Override
    public void flush() {
    }

    @Override
    public boolean write(RowChangeEvent event) {
        if (isClosed.get()) {
            LOGGER.warn("Attempted to write to closed writer");
            return false;
        }

        try {
            switch (event.getOp()) {
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
        } catch (RetinaException e) {
            LOGGER.error("Retina write failed for event {}", event.toString());
            return false;
        } finally {

        }
        // TODO: error handle
        return false;
    }


    @Deprecated
    private boolean sendInsertRequest(RowChangeEvent event) throws RetinaException {
        // Insert retina
        // boolean retinaServiceResult = retinaService.insertRecord(event.getSchemaName(), event.getTable(), event.getAfterData(), event.getTimeStamp());

        return false;
    }


    private boolean sendDeleteRequest(RowChangeEvent event) {
         return false;
//        RetinaProto.DeleteRecordResponse deleteRecordResponse = blockingStub.deleteRecord(getDeleteRecordRequest(event));
//        return deleteRecordResponse.getHeader().getErrorCode() == 0;
    }

    @Override
    public void close() throws IOException {
        if (isClosed.compareAndSet(false, true)) {
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
    }

    private boolean sendUpdateRequest(RowChangeEvent event) {
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

//    private RetinaProto.TransInfo getTransinfo(RowChangeEvent event) {
//        return RetinaProto.TransInfo.newBuilder()
//                .setOrder(event.getTransaction().getTotalOrder())
//                .setTransId(event.getTransaction().getId().hashCode()).build();
//    }
}

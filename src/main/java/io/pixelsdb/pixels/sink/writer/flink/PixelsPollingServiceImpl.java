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

package io.pixelsdb.pixels.sink.writer.flink;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.sink.PixelsPollingServiceGrpc;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.freshness.FreshnessClient;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import io.pixelsdb.pixels.sink.util.rateLimiter.FlushRateLimiter;
import io.pixelsdb.pixels.sink.util.rateLimiter.FlushRateLimiterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PixelsPollingServiceImpl extends PixelsPollingServiceGrpc.PixelsPollingServiceImplBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PixelsPollingServiceImpl.class);
    private final FlinkPollingWriter writer;
    private final int pollBatchSize;
    private final long pollTimeoutMs;
    private final FlushRateLimiter flushRateLimiter;
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private final String freshnessLevel;

    public PixelsPollingServiceImpl(FlinkPollingWriter writer)
    {
        if (writer == null)
        {
            throw new IllegalArgumentException("FlinkPollingWriter cannot be null.");
        }
        this.writer = writer;
        PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
        this.pollBatchSize = config.getCommitBatchSize();
        this.pollTimeoutMs = config.getTimeoutMs();
        this.flushRateLimiter = FlushRateLimiterFactory.getNewInstance();
        this.freshnessLevel = config.getSinkMonitorFreshnessLevel();
        LOGGER.info("PixelsPollingServiceImpl initialized. Using 'sink.commit.batch.size' for pollBatchSize ({}) " +
                        "and 'sink.timeout.ms' for pollTimeoutMs ({}).",
                this.pollBatchSize, this.pollTimeoutMs);
    }

    @Override
    public void pollEvents(SinkProto.PollRequest request, StreamObserver<SinkProto.PollResponse> responseObserver)
    {
        SchemaTableName schemaTableName = new SchemaTableName(request.getSchemaName(), request.getTableName());
        LOGGER.debug("Received poll request for table '{}'", schemaTableName);
        List<SinkProto.RowRecord> records = new ArrayList<>(pollBatchSize);

        try
        {
            for (int bucketId : request.getBucketsList())
            {
                if (records.size() >= pollBatchSize)
                {
                    break;
                }

                List<SinkProto.RowRecord> polled =
                        writer.pollRecords(
                                schemaTableName,
                                bucketId,
                                pollBatchSize - records.size(),
                                0,
                                TimeUnit.MILLISECONDS
                        );

                if (polled != null && !polled.isEmpty())
                {
                    records.addAll(polled);
                }
            }

            SinkProto.PollResponse.Builder responseBuilder = SinkProto.PollResponse.newBuilder();
            if (records != null && !records.isEmpty())
            {
                responseBuilder.addAllRecords(records);
                metricsFacade.recordRowEvent(records.size());
                metricsFacade.recordTransaction();
//                this.flushRateLimiter.acquire(records.size());

                if (freshnessLevel.equals("embed"))
                {
                    FreshnessClient.getInstance().addMonitoredTable(request.getTableName());
                }
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            LOGGER.error("Polling thread was interrupted for table: " + schemaTableName, e);
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Server polling was interrupted")
                    .asRuntimeException());
        } catch (Exception e)
        {
            LOGGER.error("An unexpected error occurred while polling for table: " + schemaTableName, e);
            responseObserver.onError(io.grpc.Status.UNKNOWN
                    .withDescription("An unexpected error occurred: " + e.getMessage())
                    .asRuntimeException());
        }
    }
}
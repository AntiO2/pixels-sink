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

package io.pixelsdb.pixels.sink.processor;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class MetricsFacade
{
    private static final Logger LOGGER =  LoggerFactory.getLogger(MetricsFacade.class);

    private static MetricsFacade instance;
    private final boolean enabled;
    private final Counter tableChangeCounter;
    private final Counter rowChangeCounter;
    private final Counter transactionCounter;
    private final Summary processingLatency;
    private final Counter rawDataThroughputCounter;
    private final Counter debeziumEventCounter;
    private final Counter rowEventCounter;

    private final Summary transServiceLatency;
    private final Summary indexServiceLatency;
    private final Summary retinaServiceLatency;
    private final Summary writerLatency;
    private final Summary totalLatency;

    private static final PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();

    private final boolean monitorReportEnabled;
    private final int monitorReportInterval;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread reportThread;

    private long lastRowChangeCount = 0;
    private long lastTransactionCount = 0;

    private MetricsFacade(boolean enabled)
    {
        this.enabled = enabled;
        if (enabled)
        {
            this.debeziumEventCounter = Counter.build()
                    .name("debezium_event_total")
                    .help("Debezium Event Total")
                    .register();

            this.rowEventCounter = Counter.build()
                    .name("row_event_total")
                    .help("Debezium Row Event Total")
                    .register();


            this.tableChangeCounter = Counter.build()
                    .name("sink_table_changes_total")
                    .help("Total processed table changes")
                    .labelNames("table")
                    .register();

            this.rowChangeCounter = Counter.build()
                    .name("sink_row_changes_total")
                    .help("Total processed row changes")
                    .labelNames("table", "operation")
                    .register();

            this.transactionCounter = Counter.build()
                    .name("sink_transactions_total")
                    .help("Total committed transactions")
                    .register();

            this.processingLatency = Summary.build()
                    .name("sink_processing_latency_seconds")
                    .help("End-to-end processing latency")
                    .quantile(0.5, 0.05)
                    .quantile(0.75, 0.01)
                    .quantile(0.95, 0.005)
                    .quantile(0.99, 0.001)
                    .register();

            this.rawDataThroughputCounter = Counter.build()
                    .name("sink_data_throughput_counter")
                    .help("Data throughput")
                    .register();

            this.transServiceLatency = Summary.build()
                    .name("trans_service_latency_seconds")
                    .help("End-to-end processing latency")
                    .quantile(0.5, 0.05)
                    .quantile(0.75, 0.01)
                    .quantile(0.95, 0.005)
                    .quantile(0.99, 0.001)
                    .register();

            this.indexServiceLatency = Summary.build()
                    .name("index_service_latency_seconds")
                    .help("End-to-end processing latency")
                    .quantile(0.5, 0.05)
                    .quantile(0.75, 0.01)
                    .quantile(0.95, 0.005)
                    .quantile(0.99, 0.001)
                    .register();

            this.retinaServiceLatency = Summary.build()
                    .name("retina_service_latency_seconds")
                    .help("End-to-end processing latency")
                    .quantile(0.5, 0.05)
                    .quantile(0.75, 0.01)
                    .quantile(0.95, 0.005)
                    .quantile(0.99, 0.001)
                    .register();

            this.writerLatency = Summary.build()
                    .name("write_latency_seconds")
                    .help("Write latency")
                    .quantile(0.5, 0.05)
                    .quantile(0.75, 0.01)
                    .quantile(0.95, 0.005)
                    .quantile(0.99, 0.001)
                    .register();

            this.totalLatency = Summary.build()
                    .name("total_latency_seconds")
                    .help("total latency to ETL a row change event")
                    .labelNames("table", "operation")
                    .quantile(0.5, 0.05)
                    .quantile(0.75, 0.01)
                    .quantile(0.95, 0.005)
                    .quantile(0.99, 0.001)
                    .register();

        } else
        {
            this.debeziumEventCounter = null;
            this.rowEventCounter = null;
            this.rowChangeCounter = null;
            this.transactionCounter = null;
            this.processingLatency = null;
            this.tableChangeCounter = null;
            this.rawDataThroughputCounter = null;
            this.transServiceLatency = null;
            this.indexServiceLatency = null;
            this.retinaServiceLatency = null;
            this.writerLatency = null;
            this.totalLatency = null;
        }

        monitorReportEnabled = config.isMonitorReportEnabled();
        monitorReportInterval = config.getMonitorReportInterval();

        if(monitorReportEnabled)
        {
            running.set(true);
            reportThread = new Thread(this::run, "Metrics Report Thread");
            LOGGER.info("Metrics Report Thread Started");
            reportThread.start();
        } else {
            reportThread = null;
        }
    }
    public void stop()
    {
        running.set(false);
        if (reportThread != null)
        {
            reportThread.interrupt();
        }
        LOGGER.info("Monitor report thread stopped.");
    }

    private static synchronized void initialize()
    {
        if (instance == null)
        {
            instance = new MetricsFacade(config.isMonitorEnabled());
        }
    }

    public static MetricsFacade getInstance()
    {
        if (instance == null)
        {
            initialize();
        }
        return instance;
    }

    public void recordDebeziumEvent()
    {
        if(enabled && debeziumEventCounter != null)
        {
            debeziumEventCounter.inc();
        }
    }

    public void recordRowChange(String table, SinkProto.OperationType operation)
    {
        recordRowChange(table, operation, 1);
    }

    public void recordRowChange(String table, SinkProto.OperationType operation, int rows)
    {
        if (enabled && rowChangeCounter != null)
        {
            tableChangeCounter.labels(table).inc(rows);
            rowChangeCounter.labels(table, operation.toString()).inc(rows);
        }
    }

    public void recordTransaction(int i)
    {
        if (enabled && transactionCounter != null)
        {
            transactionCounter.inc(i);
        }
    }

    public void recordTransaction()
    {
        recordTransaction(1);
    }

    public Summary.Timer startProcessLatencyTimer()
    {
        return enabled ? processingLatency.startTimer() : null;
    }

    public Summary.Timer startIndexLatencyTimer()
    {
        return enabled ? indexServiceLatency.startTimer() : null;
    }

    public Summary.Timer startTransLatencyTimer()
    {
        return enabled ? transServiceLatency.startTimer() : null;
    }

    public Summary.Timer startRetinaLatencyTimer()
    {
        return enabled ? retinaServiceLatency.startTimer() : null;
    }

    public Summary.Timer startWriteLatencyTimer()
    {
        return enabled ? writerLatency.startTimer() : null;
    }

    public void addRawData(double data)
    {
        rawDataThroughputCounter.inc(data);
    }

    public void recordTotalLatency(RowChangeEvent event)
    {
        if (event.getTimeStamp() != 0)
        {
            long recordLatency = System.currentTimeMillis() - event.getTimeStamp();
            totalLatency.labels(event.getFullTableName(), event.getOp().toString()).observe(recordLatency);
        }
    }

    public void recordRowEvent()
    {
        recordRowEvent(1);
    }

    public void recordRowEvent(int i)
    {
        if (enabled && rowEventCounter != null)
        {
            rowEventCounter.inc(i);
        }
    }

    public int getRecordRowEvent()
    {
        return (int) rowEventCounter.get();
    }

    public int getTransactionEvent()
    {
        return (int) transactionCounter.get();
    }


    public void run()
    {
        while (running.get())
        {
            try
            {
                logPerformance();
                Thread.sleep(monitorReportInterval);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                break;
            }
            catch (Throwable t)
            {
                LOGGER.warn("Error while reporting performance.", t);
            }
        }
    }


    public void logPerformance()
    {
        long currentRows = (long) rowEventCounter.get();
        long currentTxns = (long) transactionCounter.get();

        long deltaRows = currentRows - lastRowChangeCount;
        long deltaTxns = currentTxns - lastTransactionCount;

        lastRowChangeCount = currentRows;
        lastTransactionCount = currentTxns;

        double seconds = monitorReportInterval / 1000.0;
        double rowOips = deltaRows / seconds;
        double txnOips = deltaTxns / seconds;

        LOGGER.info(
                "Performance report: +{} rows (+{}/s), +{} transactions (+{}/s) in {} ms",
                deltaRows, String.format("%.2f", rowOips),
                deltaTxns, String.format("%.2f", txnOips),
                monitorReportInterval
        );
    }
}
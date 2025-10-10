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


package io.pixelsdb.pixels.sink;


import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @package: io.pixelsdb.pixels.sink
 * @className: DebeziumEngineTest
 * @author: AntiO2
 * @date: 2025/9/25 12:16
 */
public class DebeziumEngineTest
{
    @Test
    public void testPostgresCDC()
    {
        final Properties props = new Properties();

        props.setProperty("name", "testEngine");
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        props.setProperty("provide.transaction.metadata", "true");

        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", "/tmp/offsets.dat");
        props.setProperty("offset.flush.interval.ms", "60000");

        props.setProperty("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
        props.setProperty("schema.history.internal.file.filename", "/tmp/schemahistory.dat");

        props.setProperty("database.hostname", "localhost");
        props.setProperty("database.port", "5432");
        props.setProperty("database.user", "pixels");
        props.setProperty("database.password", "pixels_realtime_crud");
        props.setProperty("database.dbname", "pixels_bench_sf1x");
        props.setProperty("plugin.name", "pgoutput");
        props.setProperty("database.server.id", "1");
        props.setProperty("schema.include.list", "public");
        props.setProperty("snapshot.mode", "never");

        props.setProperty("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.setProperty("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.setProperty("topic.prefix", "postgres.cdc");

        props.setProperty("transforms", "topicRouting");
        props.setProperty("transforms.topicRouting.type", "org.apache.kafka.connect.transforms.RegexRouter");
        props.setProperty("transforms.topicRouting.regex", "postgresql\\.oltp_server\\.public\\.(.*)");
        props.setProperty("transforms.topicRouting.replacement", "postgresql.oltp_server.pixels_bench_sf1x.$1");

        DebeziumEngine<RecordChangeEvent<SourceRecord>> engine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(props)
                .notifying(new MyChangeConsumer())
                .build();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);

        while (true)
        {

        }
    }

    class MyChangeConsumer implements DebeziumEngine.ChangeConsumer<RecordChangeEvent<SourceRecord>>
    {
        public void handleBatch(List<RecordChangeEvent<SourceRecord>> event, DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>> committer) throws InterruptedException
        {
            committer.markBatchFinished();
        }
    }

}

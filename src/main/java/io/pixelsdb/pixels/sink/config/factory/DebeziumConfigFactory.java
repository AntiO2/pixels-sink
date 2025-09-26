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


package io.pixelsdb.pixels.sink.config.factory;


import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;

import java.util.Map;
import java.util.Properties;

/**
 * @package: io.pixelsdb.pixels.sink.config.factory
 * @className: DebeziumConfigFactory
 * @author: AntiO2
 * @date: 2025/9/26 13:48
 */
public class DebeziumConfigFactory {

    private static final String PREFIX = "debezium.";

    public static Properties createDebeziumProperties(PixelsSinkConfig config) {
        Properties debeziumProps = new Properties();

        for (Map.Entry<Object, Object> entry : config.getConfig().getProperties().entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();

            if (key.startsWith(PREFIX)) {
                String newKey = key.substring(PREFIX.length());
                debeziumProps.setProperty(newKey, value);
            }
        }

        return debeziumProps;
    }
}
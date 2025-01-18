/*
 * Copyright 2018-2025 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pixelsdb.pixels.sink.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class PixelsSinkConfig {
    private Properties properties;

    public PixelsSinkConfig(String configFilePath) throws IOException {
        properties = new Properties();
        try (FileInputStream input = new FileInputStream(configFilePath)) {
            properties.load(input);
        }
    }

    public String getTopicPrefix() {
        return properties.getProperty("topic.prefix");
    }

    public String getCaptureDatabase() {
        return properties.getProperty("consumer.capture_database");
    }

    public String[] getIncludeTables() {
        String includeTables = properties.getProperty("consumer.include_tables", "");
        return includeTables.isEmpty() ? new String[0] : includeTables.split(",");
    }

    public String getBootstrapServers() {
        return properties.getProperty("bootstrap.servers");
    }

    public String getGroupId() {
        return properties.getProperty("group.id");
    }
}
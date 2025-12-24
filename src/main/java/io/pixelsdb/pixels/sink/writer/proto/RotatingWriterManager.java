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

package io.pixelsdb.pixels.sink.writer.proto;


import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import io.pixelsdb.pixels.common.physical.PhysicalWriterUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.util.EtcdFileRegistry;

import java.io.IOException;

/**
 * @package: io.pixelsdb.pixels.sink.writer
 * @className: RotatingWriterManager
 * @author: AntiO2
 * @date: 2025/10/5 07:34
 */
public class RotatingWriterManager {
    private final String baseDir;
    private final String topic;
    private final int maxRecordsPerFile;
    private final Storage.Scheme scheme;
    private final EtcdFileRegistry registry;
    private int currentCount = 0;
    private PhysicalWriter currentWriter;
    private String currentFileName;

    public RotatingWriterManager(String topic) throws IOException {
        PixelsSinkConfig sinkConfig = PixelsSinkConfigFactory.getInstance();
        this.baseDir = sinkConfig.getSinkProtoDir();
        this.topic = topic;
        this.maxRecordsPerFile = sinkConfig.getMaxRecordsPerFile();
        this.registry = new EtcdFileRegistry(topic, baseDir);
        this.scheme = Storage.Scheme.fromPath(this.baseDir);
        rotate();
    }

    private void rotate() throws IOException {
        if (currentWriter != null) {
            currentWriter.close();
            registry.markFileCompleted(registry.getCurrentFileKey());
        }

        currentFileName = registry.createNewFile();
        currentWriter = PhysicalWriterUtil.newPhysicalWriter(scheme, currentFileName);

        currentCount = 0;
    }

    public PhysicalWriter current() throws IOException {
        if (currentCount >= maxRecordsPerFile) {
            rotate();
        }
        currentCount++;
        return currentWriter;
    }

    public void close() throws IOException {
        if (currentWriter != null) {
            currentWriter.close();
            registry.markFileCompleted(registry.getCurrentFileKey());
        }
    }
}
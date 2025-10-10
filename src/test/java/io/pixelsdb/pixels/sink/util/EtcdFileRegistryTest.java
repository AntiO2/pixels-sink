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


package io.pixelsdb.pixels.sink.util;


import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @package: io.pixelsdb.pixels.sink.util
 * @className: EtcdFileRegistryTest
 * @author: AntiO2
 * @date: 2025/10/5 08:54
 */
public class EtcdFileRegistryTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(EtcdFileRegistryTest.class);

    @Test
    public void testCreateFile()
    {
        EtcdFileRegistry etcdFileRegistry = new EtcdFileRegistry("test", "file:///tmp/test/ray");
        for (int i = 0; i < 10; i++)
        {
            String newFile = etcdFileRegistry.createNewFile();
            etcdFileRegistry.markFileCompleted(newFile);
        }
        List<String> files = etcdFileRegistry.listAllFiles();
        for (String file : files)
        {
            LOGGER.info(file);
        }
        etcdFileRegistry.cleanData();
    }
}

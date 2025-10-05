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

package io.pixelsdb.pixels.sink.deserializer;

import io.pixelsdb.pixels.sink.util.DateUtil;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;

public class RowDataParserTest
{
    // BqQ= 17
    // JbR7 24710.35
//    @ParameterizedTest
//    @CsvSource({
//            // encodedValue, expectedValue, precision, scale
//            "BqQ=, 17.00, 15, 2",
//            "JbR7, 24710.35, 15, 2",
//    })
//    void testParseDecimalValid(String encodedValue, String expectedValue, int precision, int scale) {
//        JsonNode node = new TextNode(encodedValue);
//        TypeDescription type = TypeDescription.createDecimal(precision, scale);
//        RowDataParser rowDataParser = new RowDataParser(type);
//        BigDecimal result = rowDataParser.parseDecimal(node, type);
//        assertEquals(new BigDecimal(expectedValue), result);
//    }

    @Test
    void testParseDate()
    {
        int day = 17059;
        Date debeziumDate = DateUtil.fromDebeziumDate(day);
        String dayString = DateUtil.convertDateToDayString(debeziumDate);
        long ts = 1473927308302000L;
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts / 1000), ZoneOffset.UTC);
        ZonedDateTime zonedDateTime = Instant.ofEpochMilli(ts).atZone(ZoneOffset.UTC);
        boolean pause = true;
    }
}

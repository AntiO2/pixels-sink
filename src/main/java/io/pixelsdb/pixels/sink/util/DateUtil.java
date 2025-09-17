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


import io.pixelsdb.pixels.core.utils.DatetimeUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;

/**
 * @package: io.pixelsdb.pixels.sink.util
 * @className: DateUtil
 * @author: AntiO2
 * @date: 2025/8/21 17:31
 */
public class DateUtil
{

    public static Date fromDebeziumDate(int epochDay)
    {
        Calendar cal = Calendar.getInstance();
        cal.clear();
        cal.set(1970, Calendar.JANUARY, 1); // epoch 起点
        cal.add(Calendar.DAY_OF_MONTH, epochDay); // 加上天数
        return cal.getTime();
    }

    // TIMESTAMP(1), TIMESTAMP(2), TIMESTAMP(3)
    public static Date fromDebeziumTimestamp(long epochTs)
    {
        return new Date(epochTs / 1000);
    }

    public static String convertDateToDayString(Date date)
    {
        // "yyyy-MM-dd HH:mm:ss.SSS"
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String dateToString = df.format(date);
        return (dateToString);
    }

    public static String convertDateToString(Date date)
    {
        // "yyyy-MM-dd HH:mm:ss.SSS"
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String dateToString = df.format(date);
        return (dateToString);
    }
    public static String convertTimestampToString(Date date) {
        if (date == null) {
            return null;
        }
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
        return sdf.format(date);
    }

    public static String convertDebeziumTimestampToString(long epochTs)
    {
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochTs), ZoneId.systemDefault());
        DateTimeFormatter formatter = DatetimeUtils.SQL_LOCAL_DATE_TIME;
        return dateTime.format(formatter);
    }
}

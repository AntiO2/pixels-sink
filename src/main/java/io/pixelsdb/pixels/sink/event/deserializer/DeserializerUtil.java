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

package io.pixelsdb.pixels.sink.event.deserializer;

import io.pixelsdb.pixels.sink.SinkProto;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class DeserializerUtil
{
    static public <T> SinkProto.TransactionStatus getStatusSafely(T record, String field)
    {
        String statusString = getStringSafely(record, field);
        if (statusString.equals("BEGIN"))
        {
            return SinkProto.TransactionStatus.BEGIN;
        }
        if (statusString.equals("END"))
        {
            return SinkProto.TransactionStatus.END;
        }

        return SinkProto.TransactionStatus.UNRECOGNIZED;
    }

    public static <T> Object getFieldSafely(T record, String field)
    {
        try
        {
            if (record instanceof GenericRecord avro)
            {
                return avro.get(field);
            } else if (record instanceof Struct struct)
            {
                return struct.get(field);
            } else if (record instanceof SourceRecord sourceRecord)
            {
                return ((Struct) sourceRecord.value()).get(field);
            }
        } catch (Exception e)
        {
            return null;
        }
        return null;
    }

    public static <T> String getStringSafely(T record, String field)
    {
        Object value = getFieldSafely(record, field);
        return value != null ? value.toString() : "";
    }

    public static <T> Long getLongSafely(T record, String field)
    {
        Object value = getFieldSafely(record, field);
        return value instanceof Number ? ((Number) value).longValue() : 0L;
    }

    public static <T> Integer getIntSafely(T record, String field)
    {
        Object value = getFieldSafely(record, field);
        return value instanceof Number ? ((Number) value).intValue() : 0;
    }

    static public SinkProto.OperationType getOperationType(String op)
    {
        op = op.toLowerCase();
        return switch (op)
        {
            case "c" -> SinkProto.OperationType.INSERT;
            case "u" -> SinkProto.OperationType.UPDATE;
            case "d" -> SinkProto.OperationType.DELETE;
            case "r" -> SinkProto.OperationType.SNAPSHOT;
            default -> throw new IllegalArgumentException(String.format("Can't convert %s to operation type", op));
        };
    }

    static public boolean hasBeforeValue(SinkProto.OperationType op)
    {
        return op == SinkProto.OperationType.DELETE || op == SinkProto.OperationType.UPDATE;
    }

    static public boolean hasAfterValue(SinkProto.OperationType op)
    {
        return op != SinkProto.OperationType.DELETE;
    }

    static public String getTransIdPrefix(String originTransID)
    {
        return originTransID.contains(":")
                ? originTransID.substring(0, originTransID.indexOf(":"))
                : originTransID;
    }

}

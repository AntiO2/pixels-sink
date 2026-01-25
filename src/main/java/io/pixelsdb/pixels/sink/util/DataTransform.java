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

package io.pixelsdb.pixels.sink.util;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.SinkProto;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DataTransform
{
    private static ByteString longToByteString(long value)
    {
        byte[] bytes = ByteBuffer.allocate(Long.BYTES).putLong(value).array();
        return ByteString.copyFrom(bytes);
    }

    private static int byteStringToInt(ByteString bytes)
    {
        return ByteBuffer.wrap(bytes.toByteArray()).getInt();
    }

    @Deprecated
    public static void updateTimeStamp(List<RetinaProto.TableUpdateData.Builder> updateData, long txStartTime)
    {
        ByteString timestampBytes = longToByteString(txStartTime);

        for (RetinaProto.TableUpdateData.Builder tableUpdateDataBuilder : updateData)
        {
            int insertDataCount = tableUpdateDataBuilder.getInsertDataCount();
            for (int i = 0; i < insertDataCount; i++)
            {
                RetinaProto.InsertData.Builder insertBuilder = tableUpdateDataBuilder.getInsertDataBuilder(i);
                int colValueCount = insertBuilder.getColValuesCount();
                if (colValueCount > 0)
                {
                    insertBuilder.setColValues(colValueCount - 1, timestampBytes);
                }
            }

            int updateDataCount = tableUpdateDataBuilder.getUpdateDataCount();
            for (int i = 0; i < updateDataCount; i++)
            {
                RetinaProto.UpdateData.Builder updateBuilder = tableUpdateDataBuilder.getUpdateDataBuilder(i);

                int colValueCount = updateBuilder.getColValuesCount();
                if (colValueCount > 0)
                {
                    updateBuilder.setColValues(colValueCount - 1, timestampBytes);
                }
            }
        }
    }


    public static List<SinkProto.RowRecord> updateRecordTimestamp(List<SinkProto.RowRecord> records, long timestamp)
    {
        if (records == null || records.isEmpty())
        {
            return records;
        }
        SinkProto.ColumnValue timestampColumn = getTimestampColumn(timestamp);
        List<SinkProto.RowRecord> updatedRecords = new ArrayList<>(records.size());
        for (SinkProto.RowRecord record : records)
        {

            updatedRecords.add(updateRecordTimestamp(record, timestampColumn));
        }
        return updatedRecords;
    }

    private static SinkProto.ColumnValue getTimestampColumn(long timestamp)
    {
        ByteString timestampBytes = longToByteString(timestamp);
        return SinkProto.ColumnValue.newBuilder().setValue(timestampBytes).build();
    }

    public static SinkProto.RowRecord updateRecordTimestamp(SinkProto.RowRecord record, long timestamp)
    {
        if (record == null)
        {
            return null;
        }
        SinkProto.ColumnValue timestampColumn = getTimestampColumn(timestamp);
        return updateRecordTimestamp(record, timestampColumn);
    }

    public static void updateRecordTimestamp(SinkProto.RowRecord.Builder recordBuilder, long timestamp)
    {
        switch (recordBuilder.getOp())
        {
            case INSERT:
            case UPDATE:
            case SNAPSHOT:
                if (recordBuilder.hasAfter())
                {
                    SinkProto.RowValue.Builder afterBuilder = recordBuilder.getAfterBuilder();
                    int colCount = afterBuilder.getValuesCount();
                    if (colCount > 0)
                    {
                        afterBuilder.setValues(colCount - 1, getTimestampColumn(timestamp));
                    }
                }
                break;
            case DELETE:
            default:
                break;
        }
    }

    public static void transIdToBigint(SinkProto.RowRecord.Builder recordBuilder)
    {

        if (recordBuilder.hasAfter())
        {
            SinkProto.RowValue.Builder afterBuilder = recordBuilder.getAfterBuilder();
            afterBuilder.setValues(0, getTimestampColumn(byteStringToInt(afterBuilder.getValues(0).getValue())));
        }
        if (recordBuilder.hasBefore())
        {
            SinkProto.RowValue.Builder beforeBuilder = recordBuilder.getBeforeBuilder();
            beforeBuilder.setValues(0, getTimestampColumn(byteStringToInt(beforeBuilder.getValues(0).getValue())));
        }
    }


    private static SinkProto.RowRecord updateRecordTimestamp(SinkProto.RowRecord.Builder recordBuilder, SinkProto.ColumnValue timestampColumn)
    {
        switch (recordBuilder.getOp())
        {
            case INSERT:
            case UPDATE:
            case SNAPSHOT:
                if (recordBuilder.hasAfter())
                {
                    SinkProto.RowValue.Builder afterBuilder = recordBuilder.getAfterBuilder();
                    int colCount = afterBuilder.getValuesCount();
                    if (colCount > 0)
                    {
                        afterBuilder.setValues(colCount - 1, timestampColumn);
                    }
                }
                break;
            case DELETE:
            default:
                break;
        }
        return recordBuilder.build();
    }

    private static SinkProto.RowRecord updateRecordTimestamp(SinkProto.RowRecord record, SinkProto.ColumnValue timestampColumn)
    {
        SinkProto.RowRecord.Builder recordBuilder = record.toBuilder();
        return updateRecordTimestamp(recordBuilder, timestampColumn);
    }

    public static String extractTableName(String topic)
    {
        String[] parts = topic.split("\\.");
        return parts[parts.length - 1];
    }
}

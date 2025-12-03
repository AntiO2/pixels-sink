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
 import java.util.ArrayList;

 import java.nio.ByteBuffer;
 import java.util.List;

 public class DataTransform
 {
     public static ByteString longToByteString(long value) {
         byte[] bytes = ByteBuffer.allocate(Long.BYTES).putLong(value).array();
         return ByteString.copyFrom(bytes);
     }

     public static void updateTimeStamp(List<RetinaProto.TableUpdateData.Builder> updateData, long txStartTime) {
         ByteString timestampBytes = longToByteString(txStartTime);

         for (RetinaProto.TableUpdateData.Builder tableUpdateDataBuilder : updateData) {
             int insertDataCount = tableUpdateDataBuilder.getInsertDataCount();
             for (int i = 0; i < insertDataCount; i++) {
                 RetinaProto.InsertData.Builder insertBuilder = tableUpdateDataBuilder.getInsertDataBuilder(i);
                 int colValueCount = insertBuilder.getColValuesCount();
                 if (colValueCount > 0) {
                     insertBuilder.setColValues(colValueCount - 1, timestampBytes);
                 }
             }

             int updateDataCount = tableUpdateDataBuilder.getUpdateDataCount();
             for (int i = 0; i < updateDataCount; i++) {
                 RetinaProto.UpdateData.Builder updateBuilder = tableUpdateDataBuilder.getUpdateDataBuilder(i);

                 int colValueCount = updateBuilder.getColValuesCount();
                 if (colValueCount > 0) {
                     updateBuilder.setColValues(colValueCount - 1, timestampBytes);
                 }
             }
         }
     }
    /**
     * 遍历 RowRecord 列表，为每个记录的 'after' 镜像的最后一列更新时间戳。
     * 由于 RowRecord 是不可变的，此方法会返回一个包含已修改记录的新列表。
     *
     * @param records 原始的 RowRecord 列表。
     * @param timestamp 要设置的时间戳 (long 类型)。
     * @return 包含更新后时间戳的 RowRecord 新列表。
     */
    public static List<SinkProto.RowRecord> updateRecordTimestamp(List<SinkProto.RowRecord> records, long timestamp) {
        // 处理空或 null 列表的边界情况
        if (records == null || records.isEmpty()) {
            return records;
        }
        // 1. 一次性将 long 转换为 ByteString，提高效率
        ByteString timestampBytes = longToByteString(timestamp);
        SinkProto.ColumnValue timestampColumn = SinkProto.ColumnValue.newBuilder().setValue(timestampBytes).build();
        // 2. 创建一个新列表来存储修改后的记录
        List<SinkProto.RowRecord> updatedRecords = new ArrayList<>(records.size());
        // 3. 遍历所有记录
        for (SinkProto.RowRecord record : records) {
            SinkProto.RowRecord.Builder recordBuilder = record.toBuilder();
            // 4. 只处理包含 'after' 镜像的操作类型 (INSERT, UPDATE, SNAPSHOT)
            switch (record.getOp()) {
                case INSERT:
                case UPDATE:
                case SNAPSHOT:
                    if (recordBuilder.hasAfter()) {
                        SinkProto.RowValue.Builder afterBuilder = recordBuilder.getAfterBuilder();
                        int colCount = afterBuilder.getValuesCount();
                        if (colCount > 0) {
                            // 5. 设置最后一列的值
                            afterBuilder.setValues(colCount - 1, timestampColumn);
                        }
                    }
                    break;
                case DELETE:
                default:
                    // 对于 DELETE 或其他未知操作，我们不修改记录
                    break;
            }
            // 6. 将构建好的记录（无论是否修改）添加到新列表中
            updatedRecords.add(recordBuilder.build());
        }
        return updatedRecords;
    }
 }

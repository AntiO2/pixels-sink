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
             tableUpdateDataBuilder.setTimestamp(txStartTime);
         }
     }
 }

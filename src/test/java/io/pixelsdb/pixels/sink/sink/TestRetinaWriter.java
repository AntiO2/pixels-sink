package io.pixelsdb.pixels.sink.sink;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.retina.RetinaService;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class TestRetinaWriter {

    static Logger logger = Logger.getLogger(TestRetinaWriter.class.getName());
    static RetinaService retinaService;
    static MetadataService metadataService;

    @BeforeAll
    public static void setUp() throws IOException {
        PixelsSinkConfigFactory.initialize("/home/pixels/projects/pixels-sink/src/main/resources/pixels-sink.local.properties");
        retinaService = RetinaService.Instance();
        metadataService = MetadataService.Instance();
    }

    @Test
    public  void insertSingleRecord() throws RetinaException, SinkException {
        long timeStamp=0;
        String schemaName = "pixels_index";
        String tableName = "ray_index";


        List<RetinaProto.UpdateData> updateData = new ArrayList<>();
        // <int, long, string>
        for(int i = 0; i < 10; ++i) {
            byte[][] cols = new byte[3][];

            cols[0] = Integer.toString(i).getBytes(StandardCharsets.UTF_8);
            cols[1] = Long.toString(i * 1000L).getBytes(StandardCharsets.UTF_8);
            cols[2] = ("row_" + i).getBytes(StandardCharsets.UTF_8);
            SinkProto.RowValue.Builder afterValueBuilder = SinkProto.RowValue.newBuilder();
            afterValueBuilder
                    .addValues(
                    SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom((cols[0]))).setName("id").build())
                    .addValues(
                            SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom((cols[1]))).setName("age").build())
                    .addValues(
                            SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom((cols[2]))).setName("name").build());


            SinkProto.RowRecord.Builder builder = SinkProto.RowRecord.newBuilder();
            builder.setOp(SinkProto.OperationType.INSERT)
                    .setAfter(afterValueBuilder)
                    .setSource(
                            SinkProto.SourceInfo.newBuilder()
                                    .setDb(schemaName)
                                    .setTable(tableName)
                                    .build()
                    );
            RowChangeEvent rowChangeEvent = new RowChangeEvent(builder.build());
            rowChangeEvent.setTimeStamp(timeStamp);
            IndexProto.IndexKey indexKey = rowChangeEvent.getAfterKey();
            RetinaProto.UpdateData.Builder updateDataBuilder = RetinaProto.UpdateData.newBuilder();
            updateDataBuilder.setInsert(RetinaProto.InsertData.newBuilder()
                    .addColValues(ByteString.copyFrom((cols[0]))).addColValues(ByteString.copyFrom((cols[1])))
                    .addColValues(ByteString.copyFrom((cols[2])))
                    .setTableName(tableName)
                    .addIndexKeys(indexKey));
            updateData.add(updateDataBuilder.build());

        }
        retinaService.updateRecord(schemaName, updateData, timeStamp);
    }

    @Test
    public  void insertSimpleRecord() throws RetinaException {
        long timeStamp=100;
        String tableName = "simple2";
        // <int, long, string>
        int start = 20000;
        int end = 30000;
        for(int i = 20000; i < 30000; ++i) {
            byte[][] cols = new byte[2][];

            cols[0] = Integer.toString(i).getBytes(StandardCharsets.UTF_8);
            cols[1] = Long.toString(i * 1000L).getBytes(StandardCharsets.UTF_8);
//            boolean result = retinaService.insertRecord(schemaName, tableName, cols, timeStamp);
//            logger.info("inserted row No." + i + "\tresult: " + result);
        }
    }
}

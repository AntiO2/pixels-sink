package io.pixelsdb.pixels.sink.sink;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.retina.RetinaService;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.transaction.TransService;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
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
    static TableMetadataRegistry metadataRegistry;
    static  TransService transService;
    @BeforeAll
    public static void setUp() throws IOException {
        PixelsSinkConfigFactory.initialize("/home/pixels/projects/pixels-sink/src/main/resources/pixels-sink.local.properties");
        retinaService = RetinaService.Instance();
        metadataRegistry = TableMetadataRegistry.Instance();
        transService = TransService.Instance();
    }

    @Test
    public  void insertSingleRecord() throws RetinaException, SinkException, TransException
    {

        TransContext ctx = transService.beginTrans(false);
        long timeStamp=ctx.getTimestamp();
        String schemaName = "pixels_index";
        String tableName = "ray_index";
        List<RetinaProto.TableUpdateData> tableUpdateData = new ArrayList<>();
        RetinaProto.TableUpdateData.Builder tableUpdateDataBuilder = RetinaProto.TableUpdateData.newBuilder();
        tableUpdateDataBuilder.setTableName(tableName);
        tableUpdateDataBuilder.setPrimaryIndexId(metadataRegistry.getPrimaryIndexKeyId(schemaName, tableName));
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
            RetinaProto.InsertData.Builder insertDataBuilder = RetinaProto.InsertData.newBuilder();
            insertDataBuilder.
                    addColValues(ByteString.copyFrom((cols[0]))).addColValues(ByteString.copyFrom((cols[1])))
                    .addColValues(ByteString.copyFrom((cols[2])))
                    .addIndexKeys(indexKey);
            tableUpdateDataBuilder.addInsertData(insertDataBuilder.build());
        }
        tableUpdateData.add(tableUpdateDataBuilder.build());
        retinaService.updateRecord(schemaName, tableUpdateData, timeStamp);

        transService.commitTrans(ctx.getTransId(), timeStamp);
    }

    @Test
    public  void updateSingleRecord() throws RetinaException, SinkException, TransException
    {
        TransContext ctx = transService.beginTrans(false);
        long timeStamp=ctx.getTimestamp();
        String schemaName = "pixels_index";
        String tableName = "ray_index";


        List<RetinaProto.TableUpdateData> tableUpdateData = new ArrayList<>();
        RetinaProto.TableUpdateData.Builder tableUpdateDataBuilder = RetinaProto.TableUpdateData.newBuilder();
        tableUpdateDataBuilder.setTableName(tableName);
        tableUpdateDataBuilder.setPrimaryIndexId(metadataRegistry.getPrimaryIndexKeyId(schemaName, tableName));
        // <int, long, string>
        for(int i = 0; i < 10; ++i) {
            byte[][] cols = new byte[4][];
            cols[0] = Integer.toString(i).getBytes(StandardCharsets.UTF_8);
            cols[1] = Long.toString(i * 1000L).getBytes(StandardCharsets.UTF_8);
            cols[2] = ("row_" + i).getBytes(StandardCharsets.UTF_8);
            cols[3] = ("updated_row_" + i).getBytes(StandardCharsets.UTF_8);
            SinkProto.RowValue.Builder beforeValueBuilder = SinkProto.RowValue.newBuilder();
            beforeValueBuilder
                    .addValues(
                            SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom((cols[0]))).setName("id").build())
                    .addValues(
                            SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom((cols[1]))).setName("age").build())
                    .addValues(
                            SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom((cols[2]))).setName("name").build());


            SinkProto.RowValue.Builder afterValueBuilder = SinkProto.RowValue.newBuilder();
            afterValueBuilder
                    .addValues(
                            SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom((cols[0]))).setName("id").build())
                    .addValues(
                            SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom((cols[1]))).setName("age").build())
                    .addValues(
                            SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom((cols[3]))).setName("name").build());
            SinkProto.RowRecord.Builder builder = SinkProto.RowRecord.newBuilder();
            builder.setOp(SinkProto.OperationType.UPDATE)
                    .setBefore(beforeValueBuilder)
                    .setAfter(afterValueBuilder)
                    .setSource(
                            SinkProto.SourceInfo.newBuilder()
                                    .setDb(schemaName)
                                    .setTable(tableName)
                                    .build()
                    );

            RowChangeEvent rowChangeEvent = new RowChangeEvent(builder.build());
            rowChangeEvent.setTimeStamp(timeStamp);
            RetinaProto.DeleteData.Builder deleteDataBuilder = RetinaProto.DeleteData.newBuilder();
            deleteDataBuilder
                    .addIndexKeys(rowChangeEvent.getBeforeKey());
            tableUpdateDataBuilder.addDeleteData(deleteDataBuilder.build());

            RetinaProto.InsertData.Builder insertDataBuilder = RetinaProto.InsertData.newBuilder();
            insertDataBuilder
                    .addColValues(ByteString.copyFrom((cols[0]))).addColValues(ByteString.copyFrom((cols[1])))
                    .addColValues(ByteString.copyFrom((cols[3])))
                    .addIndexKeys(rowChangeEvent.getAfterKey());
            tableUpdateDataBuilder.addInsertData(insertDataBuilder.build());
        }
        tableUpdateData.add(tableUpdateDataBuilder.build());
        retinaService.updateRecord(schemaName, tableUpdateData, timeStamp);
        transService.commitTrans(ctx.getTransId(), timeStamp);
    }
}

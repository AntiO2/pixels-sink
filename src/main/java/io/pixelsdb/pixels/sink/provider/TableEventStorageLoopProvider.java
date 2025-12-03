package io.pixelsdb.pixels.sink.provider;

import com.google.protobuf.InvalidProtocolBufferException;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.event.deserializer.RowChangeEventStructDeserializer;
import io.pixelsdb.pixels.sink.exception.SinkException;

import java.nio.ByteBuffer;
import java.util.logging.Logger;
import io.pixelsdb.pixels.core.utils.Pair;
public class TableEventStorageLoopProvider<T> extends TableEventProvider<T>
{
    private final Logger LOGGER = Logger.getLogger(TableEventStorageProvider.class.getName());

    protected TableEventStorageLoopProvider()
    {
        super();
    }

    @Override
    RowChangeEvent convertToTargetRecord(T record)
    {
        Pair<ByteBuffer, Integer> pairRecord = (Pair<ByteBuffer, Integer>) record;
        ByteBuffer sourceRecord = pairRecord.getLeft();
        Integer loopId = pairRecord.getRight();
        try
        {
            SinkProto.RowRecord rowRecord = SinkProto.RowRecord.parseFrom(sourceRecord);

            SinkProto.RowRecord.Builder rowRecordBuilder = rowRecord.toBuilder();
            SinkProto.TransactionInfo.Builder transactionBuilder = rowRecordBuilder.getTransactionBuilder();
            String id = transactionBuilder.getId();
            transactionBuilder.setId(id + "_" + Integer.toString(loopId));
            rowRecordBuilder.setTransaction(transactionBuilder);
            return RowChangeEventStructDeserializer.convertToRowChangeEvent(rowRecordBuilder.build());
        } catch (InvalidProtocolBufferException | SinkException e)
        {
            LOGGER.warning(e.getMessage());
            return null;
        }
    }
}

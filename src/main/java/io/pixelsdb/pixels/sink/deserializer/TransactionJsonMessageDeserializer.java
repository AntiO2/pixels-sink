package io.pixelsdb.pixels.sink.deserializer;

import com.alibaba.fastjson.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import io.pixelsdb.pixels.sink.proto.TransactionMetadataValue;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class TransactionJsonMessageDeserializer implements Deserializer<TransactionMetadataValue.TransactionMetadata> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionJsonMessageDeserializer.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonFormat.Parser PROTO_PARSER = JsonFormat.parser().ignoringUnknownFields();

    @Override
    public TransactionMetadataValue.TransactionMetadata deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        try {
            Map<String, Object> rawMessage = OBJECT_MAPPER.readValue(data, Map.class);
            return parseTransactionMetadata(rawMessage);
        } catch (IOException e) {
            LOGGER.error("Failed to deserialize transaction message", e);
            throw new RuntimeException("Deserialization error", e);
        }
    }

    private TransactionMetadataValue.TransactionMetadata parseTransactionMetadata(Map<String, Object> rawMessage) throws IOException {
        TransactionMetadataValue.TransactionMetadata.Builder builder = TransactionMetadataValue.TransactionMetadata.newBuilder();
        String json = OBJECT_MAPPER.writeValueAsString(rawMessage.get("payload"));
        //TODO optimize
        PROTO_PARSER.merge(json, builder);

        return builder.build();
    }
}
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

package io.pixelsdb.pixels.sink.event.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class TransactionJsonMessageDeserializer implements Deserializer<SinkProto.TransactionMetadata>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionJsonMessageDeserializer.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonFormat.Parser PROTO_PARSER = JsonFormat.parser().ignoringUnknownFields();

    @Override
    public SinkProto.TransactionMetadata deserialize(String topic, byte[] data)
    {
        if (data == null || data.length == 0)
        {
            return null;
        }
        MetricsFacade.getInstance().addRawData(data.length);
        try
        {
            Map<String, Object> rawMessage = OBJECT_MAPPER.readValue(data, Map.class);
            return parseTransactionMetadata(rawMessage);
        } catch (IOException e)
        {
            LOGGER.error("Failed to deserialize transaction message", e);
            throw new RuntimeException("Deserialization error", e);
        }
    }

    private SinkProto.TransactionMetadata parseTransactionMetadata(Map<String, Object> rawMessage) throws IOException
    {
        SinkProto.TransactionMetadata.Builder builder = SinkProto.TransactionMetadata.newBuilder();
        String json = OBJECT_MAPPER.writeValueAsString(rawMessage.get("payload"));
        PROTO_PARSER.merge(json, builder);

        builder.setId(DeserializerUtil.getTransIdPrefix(builder.getId()));

        return builder.build();
    }
}
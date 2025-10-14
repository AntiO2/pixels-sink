package io.pixelsdb.pixels.sink.event;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RowChangeEventTest
{
    private static Logger LOGGER = LoggerFactory.getLogger(RowChangeEventTest.class);

    @BeforeAll
    public static void init() throws IOException
    {
        PixelsSinkConfigFactory.initialize("/home/ubuntu/pixels-sink/conf/pixels-sink.aws.properties");
    }


    @Test
    public void testSameHash()
    {
        for(int i = 0; i < 10; ++i)
        {
            ByteString indexKey = getIndexKey(0);
            int bucket = RowChangeEvent.getBucketIdFromByteBuffer(indexKey);
            LOGGER.info("Bucket: {}", bucket);
        }
    }

    private ByteString getIndexKey(int key)
    {
        int keySize = Integer.BYTES;
        ByteBuffer byteBuffer = ByteBuffer.allocate(keySize);
        byteBuffer.putInt(key);
        return ByteString.copyFrom(byteBuffer.rewind());
    }
}

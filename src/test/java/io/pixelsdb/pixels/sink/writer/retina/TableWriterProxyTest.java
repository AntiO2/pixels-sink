package io.pixelsdb.pixels.sink.writer.retina;

import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEventTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TableWriterProxyTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TableWriterProxyTest.class);


    String tableName = "test";

    @BeforeAll
    public static void init() throws IOException
    {
        PixelsSinkConfigFactory.initialize("/home/ubuntu/pixels-sink/conf/pixels-sink.aws.properties");
    }

    @Test
    public void testGetSameTableWriter() throws IOException
    {
        TableWriterProxy tableWriterProxy = TableWriterProxy.getInstance();

        for(int i = 0; i < 10 ; i++)
        {
            TableWriter tableWriter = tableWriterProxy.getTableWriter(tableName, 0, 0);
        }
    }
}

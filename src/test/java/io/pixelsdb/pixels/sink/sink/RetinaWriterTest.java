package io.pixelsdb.pixels.sink.sink;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.common.retina.RetinaService;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.retina.PixelsWriterBuffer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class RetinaWriterTest {

    static Logger logger = Logger.getLogger(RetinaWriterTest.class.getName());
    static RetinaService retinaService;
    static MetadataService metadataService;
    static String schemaName = "pixels";
    static String tableName = "ray4";


    @BeforeAll
    public static void setUp() {
        retinaService = RetinaService.Instance();
        metadataService = MetadataService.Instance();
    }

    @Test
    public  void insertSingleRecord() throws RetinaException {
        long timeStamp=100;
        // <int, long, string>
        for(int i = 0; i < 100; ++i) {
            byte[][] cols = new byte[3][];

            cols[0] = Integer.toString(i).getBytes(StandardCharsets.UTF_8);
            cols[1] = Long.toString(i * 1000L).getBytes(StandardCharsets.UTF_8);
            cols[2] = ("row_" + i).getBytes(StandardCharsets.UTF_8);

            boolean result = retinaService.insertRecord(schemaName, tableName, cols, timeStamp);
            logger.info("inserted row No." + i + "\tresult: " + result);
        }
    }

    @Test
    public  void insertSimpleRecord() throws RetinaException {
        long timeStamp=100;
        tableName = "simple2";
        // <int, long, string>
        int start = 20000;
        int end = 30000;
        for(int i = 20000; i < 30000; ++i) {
            byte[][] cols = new byte[2][];

            cols[0] = Integer.toString(i).getBytes(StandardCharsets.UTF_8);
            cols[1] = Long.toString(i * 1000L).getBytes(StandardCharsets.UTF_8);
            boolean result = retinaService.insertRecord(schemaName, tableName, cols, timeStamp);
            logger.info("inserted row No." + i + "\tresult: " + result);
        }
    }
}

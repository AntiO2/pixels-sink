package io.pixelsdb.pixels.sink.concurrent;


import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.transaction.TransService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;
import java.util.List;

@Slf4j
public class TransactionServiceTest {
    private static final Logger logger = LoggerFactory.getLogger(TransactionServiceTest.class);
    @Test
    public void testTransactionService() {
        int numTransactions = 10;

        TransService transService = TransService.CreateInstance("localhost", 18889);
        try {
            List<TransContext> transContexts =  transService.beginTransBatch(numTransactions, false);
            assertEquals(numTransactions, transContexts.size());
            TransContext prevTransContext = transContexts.get(0);
            for(int i = 1; i < numTransactions; i++) {
                TransContext transContext = transContexts.get(i);
                assertTrue(transContext.getTransId() > prevTransContext.getTransId());
                assertTrue(transContext.getTimestamp() > prevTransContext.getTimestamp());
                prevTransContext = transContext;
            }
        } catch (TransException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testBatchRequest() {
        int numTransactions = 1000;

        TransService transService = TransService.CreateInstance("localhost", 18889);
        try {
            List<TransContext> transContexts =  transService.beginTransBatch(numTransactions, false);
            assertEquals(numTransactions, transContexts.size());
            TransContext prevTransContext = transContexts.get(0);
            for(int i = 1; i < numTransactions; i++) {
                TransContext transContext = transContexts.get(i);
                assertTrue(transContext.getTransId() > prevTransContext.getTransId());
                assertTrue(transContext.getTimestamp() > prevTransContext.getTimestamp());
                prevTransContext = transContext;
            }
        } catch (TransException e) {
            throw new RuntimeException(e);
        }
    }
    @Test
    public void testAbort() throws TransException
    {
        TransService transService = TransService.Instance();
        TransContext transContext = transService.beginTrans(true);

        logger.info("ID {}, TS {}", transContext.getTransId(), transContext.getTimestamp());
        TransContext transContext1 = transService.beginTrans(false);
        TransContext transContext2 = transService.beginTrans(false);

        logger.info("ID {}, TS {}", transContext1.getTransId(), transContext1.getTimestamp());
        logger.info("ID {}, TS {}", transContext2.getTransId(), transContext2.getTimestamp());
        transService.commitTrans(transContext2.getTransId(), transContext2.getTimestamp());

        transContext = transService.beginTrans(true);
        logger.info("ID {}, TS {}", transContext.getTransId(), transContext.getTimestamp());

        
    }
}

package io.pixelsdb.pixels.sink.freshness;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

// We extend FreshnessClient to access the protected queryAndCalculateFreshness method
public class TestFreshnessClient
{

    // Mocks for JDBC dependencies
    private Connection mockConnection;
    private Statement mockStatement;
    private ResultSet mockResultSet;

    // Mocks for utility/config dependencies
    private PixelsSinkConfig mockConfig;
    private MetricsFacade mockMetricsFacade;
    private FreshnessClient client; // The instance of the client to test

    @BeforeAll
    public static void setUp() throws IOException
    {
        // Initialization as per the user's template
        PixelsSinkConfigFactory.initialize("/home/ubuntu/pixels-sink/conf/pixels-sink.aws.properties");
    }
    @Test
    public void testFreshnessCalculationSuccess() throws Exception {

        FreshnessClient freshnessClient = FreshnessClient.getInstance();
        freshnessClient.addMonitoredTable("customer");
        freshnessClient.start();
        while(true){}
    }

    @Test
    public void testSnapshotTs() throws SQLException
    {
        FreshnessClient freshnessClient = FreshnessClient.getInstance();
        Connection connection = freshnessClient.createNewConnection(123456L);
        String query = String.format("SELECT max(freshness_ts) FROM customer");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        resultSet.next();
    }
}
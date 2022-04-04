import nikolaeva.lab1.CounterType;
import nikolaeva.lab1.HW1Mapper;
import nikolaeva.lab1.Record;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for counters
 */
public class CountersTest {

    private MapDriver<LongWritable, Text, Record, IntWritable> mapDriver;

    /**
     * Initial setup
     */
    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        mapDriver.addCacheFile("config.csv");
        mapDriver.getConfiguration().setStrings("scale", "1s");
    }

    /**
     * Check that MALFORMED counter is incremented if input data does not contain 3 values
     *
     * @throws IOException
     */
    @Test
    public void testMapperCounterMalformedLength() throws IOException {
        String testMalformedLength1 = "2, 1648936362777, ";
        String testMalformedLength2 = "2, 1648936362777";
        String testMalformedLength3 = "2, 1648936362777,";
        mapDriver
                .withInput(new LongWritable(), new Text(testMalformedLength1))
                .withInput(new LongWritable(), new Text(testMalformedLength2))
                .withInput(new LongWritable(), new Text(testMalformedLength3))
                .runTest();

        assertEquals("Malformed counter", 3, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }

    /**
     * Check that MALFORMED counter is incremented if input values cannot be converted to long
     *
     * @throws IOException
     */
    @Test
    public void testMapperCounterMalformedType() throws IOException {
        String testMalformedType1 = "Node1, 1648936362777, 968";
        String testMalformedType2 = "2, 164893636277o, 968";
        String testMalformedType3 = "2, 1648936362777, 9k8";
        mapDriver
                .withInput(new LongWritable(), new Text(testMalformedType1))
                .withInput(new LongWritable(), new Text(testMalformedType2))
                .withInput(new LongWritable(), new Text(testMalformedType3))
                .runTest();

        assertEquals("Malformed counter", 3, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }

    /**
     * Check that MALFORMED counter is incremented if metricId in input data does not exist
     *
     * @throws IOException
     */
    @Test
    public void testMapperCounterMalformedWrongMetric() throws IOException {
        String testMalformedWrongMetric = "5, 1648936362777, 968";
        mapDriver
                .withInput(new LongWritable(), new Text(testMalformedWrongMetric))
                .runTest();

        assertEquals("Malformed counter", 1, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }

    /**
     * Check that MALFORMED counter is not incremented if input data is in correct format
     *
     * @throws IOException
     */
    @Test
    public void testMapperCounterZero() throws IOException {
        String testInput = "2, 1648936362777, 968";
        mapDriver
                .withInput(new LongWritable(), new Text(testInput))
                .withOutput(new Record("Node2", 1648936362000L, "1s"),
                        new IntWritable(968))
                .runTest();

        assertEquals("Malformed counter", 0, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }
}
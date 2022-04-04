import nikolaeva.lab1.HW1Mapper;
import nikolaeva.lab1.HW1Reducer;
import nikolaeva.lab1.Record;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * MapReduce tests
 */
public class MapReduceTest {

    private MapDriver<LongWritable, Text, Record, IntWritable> mapDriver;
    private ReduceDriver<Record, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Record, IntWritable,
            Text, IntWritable> mapReduceDriver;

    private final String testInputMapper = "2, 1648936362777, 968";
    private final Record testInputReducer = new Record("Node2", 1648936362000L, "1s");

    /**
     * Initial setup
     */
    @Before
    public void setUp() {
        final String configName = "config.csv";
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();

        mapDriver = MapDriver.newMapDriver(mapper);
        mapDriver.addCacheFile(configName);

        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        reduceDriver.addCacheFile(configName);

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        mapReduceDriver.addCacheFile(configName);
        mapReduceDriver.getConfiguration().setStrings("scale", "1s");
        mapReduceDriver.getConfiguration().setStrings("function", "avg");
    }

    /**
     * Check timestamp change in map when values are grouped by seconds
     *
     * @throws IOException
     */
    @Test
    public void testMapperScaleSeconds() throws IOException {
        final String scale = "30s";
        mapDriver.getConfiguration().setStrings("scale", scale);
        mapDriver
                .withInput(new LongWritable(), new Text(testInputMapper))
                .withOutput(new Record("Node2", 1648936350000L, scale),
                        new IntWritable(968))
                .runTest();
    }

    /**
     * Check timestamp change in map when values are grouped by minutes
     *
     * @throws IOException
     */
    @Test
    public void testMapperScaleMinutes() throws IOException {
        final String scale = "10m";
        mapDriver.getConfiguration().setStrings("scale", scale);
        mapDriver
                .withInput(new LongWritable(), new Text(testInputMapper))
                .withOutput(new Record("Node2", 1648936200000L, scale),
                        new IntWritable(968))
                .runTest();
    }

    /**
     * Check timestamp change in map when values are grouped by hours
     *
     * @throws IOException
     */
    @Test
    public void testMapperScaleHours() throws IOException {
        final String scale = "5h";
        mapDriver.getConfiguration().setStrings("scale", scale);
        mapDriver
                .withInput(new LongWritable(), new Text(testInputMapper))
                .withOutput(new Record("Node2", 1648926000000L, scale),
                        new IntWritable(968))
                .runTest();
    }

    /**
     * Check timestamp change in map when values are grouped by days
     *
     * @throws IOException
     */
    @Test
    public void testMapperScaleDays() throws IOException {
        final String scale = "1d";
        mapDriver.getConfiguration().setStrings("scale", scale);
        mapDriver
                .withInput(new LongWritable(), new Text(testInputMapper))
                .withOutput(new Record("Node2", 1648857600000L, scale),
                        new IntWritable(968))
                .runTest();
    }

    /**
     * Check if sum is calculated correctly in reducer
     *
     * @throws IOException
     */
    @Test
    public void testReducerSum() throws IOException {
        reduceDriver.getConfiguration().setStrings("function", "sum");
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(5));
        values.add(new IntWritable(12));
        values.add(new IntWritable(4));
        Record res = testInputReducer;
        reduceDriver
                .withInput(res, values)
                .withOutput(new Text(res.toString()), new IntWritable(21))
                .runTest();
    }

    /**
     * Check if average is calculated correctly in reducer
     *
     * @throws IOException
     */
    @Test
    public void testReducerAvg() throws IOException {
        reduceDriver.getConfiguration().setStrings("function", "avg");
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(5));
        values.add(new IntWritable(12));
        values.add(new IntWritable(4));
        Record res = testInputReducer;
        reduceDriver
                .withInput(res, values)
                .withOutput(new Text(res.toString()), new IntWritable(7))
                .runTest();
    }

    /**
     * Check if maximum is calculated correctly in reducer
     *
     * @throws IOException
     */
    @Test
    public void testReducerMax() throws IOException {
        reduceDriver.getConfiguration().setStrings("function", "max");
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(5));
        values.add(new IntWritable(12));
        values.add(new IntWritable(4));
        Record res = testInputReducer;
        reduceDriver
                .withInput(res, values)
                .withOutput(new Text(res.toString()), new IntWritable(12))
                .runTest();
    }

    /**
     * Check if minimum is calculated correctly in reducer
     *
     * @throws IOException
     */
    @Test
    public void testReducerMin() throws IOException {
        reduceDriver.getConfiguration().setStrings("function", "min");
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(5));
        values.add(new IntWritable(12));
        values.add(new IntWritable(4));
        Record res = testInputReducer;
        reduceDriver
                .withInput(res, values)
                .withOutput(new Text(res.toString()), new IntWritable(4))
                .runTest();
    }

    /**
     * Check if map and reduce work correctly (avg value is calculated)
     *
     * @throws IOException
     */
    @Test
    public void testMapReduce() throws IOException {
        Record res = new Record("Node2", 1648936362000L, "1s");
        String testInputMapper1 = "2, 1648936362888, 10";
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testInputMapper))
                .withInput(new LongWritable(), new Text(testInputMapper1))
                .withOutput(new Text(res.toString()), new IntWritable(489))
                .runTest();
    }
}

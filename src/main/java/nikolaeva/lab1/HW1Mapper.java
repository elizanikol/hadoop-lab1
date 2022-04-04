package nikolaeva.lab1;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * Mapper class
 */
@Log4j
public class HW1Mapper extends Mapper<LongWritable, Text, Record, IntWritable> {

    private final static IntWritable metricValue = new IntWritable();
    private final static Record record = new Record();
    private static String scale = "";
    private static final Map<String, String> metricIdsToMetricNamesMap = new HashMap<>();

    /**
     * Mapper initial setup method.
     * Loads value of scale and cache files (config with metric IDs and their corresponding names)
     *
     * @param context - job context
     *
     * @throws IOException
     */
    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        scale = conf.getStrings("scale")[0];
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            readFile(cacheFiles[0].getPath());
        }
    }

    /**
     * Map method:
     * increments MALFORMED counter if data is malformed;
     * else rounds timestamp according to scale and passes transformed record and its value to reducer.
     *
     * @param key - unused
     * @param value - line from generated data input
     * @param context - job context
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws RuntimeException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException, RuntimeException {
        String line = value.toString();
        String[] data = line.split(", ");
        if (isDataMalformed(data)) {
            context.getCounter(CounterType.MALFORMED).increment(1);
        } else if (scaleStrToLong(scale) == 0L) {
            throw new RuntimeException("Invalid scale!");
        } else {
            long scaleValue = scaleStrToLong(scale);
            long timestampScaled = Long.parseLong(data[1]) / scaleValue * scaleValue;
            record.set(metricIdsToMetricNamesMap.get(data[0]), timestampScaled, scale);
            metricValue.set(Integer.parseInt(data[data.length - 1]));
            context.write(record, metricValue);
        }
    }

    /**
     * Checks whether data is malformed:
     * it has to contain 3 numbers; first of which must be a valid metricID.
     *
     * @param data - entry with metricID, timestamp and value.
     * @return true if data is malformed.
     */
    private boolean isDataMalformed(String[] data) {
        if (data.length != 3) {
            return true;
        }
        for (String i : data) {
            try {
                Long.parseLong(i);
            } catch (NumberFormatException e) {
                return true;
            }
        }
        return !metricIdsToMetricNamesMap.containsKey(data[0]);
    }

    /**
     * Method to convert scale from string to long value.
     *
     * @param scaleStr - scale in the following format:
     *                 [number][unit], where unit can be:
     *                 s - for seconds;
     *                 m - for minutes;
     *                 h - for hours;
     *                 d - for days.
     */
    private static long scaleStrToLong(String scaleStr) {
        if (scaleStr == null || scaleStr.length() < 2) {
            return 0L;
        }
        char unit;
        long scaleLong;
        try {
            unit = scaleStr.charAt(scaleStr.length() - 1);
            scaleStr = scaleStr.substring(0, scaleStr.length() - 1);
            scaleLong = Long.parseLong(scaleStr);
        } catch (Exception e) {
            return 0L;
        }
        switch (unit) {
            case 's':
                scaleLong *= 1000;
                break;
            case 'm':
                scaleLong *= 60 * 1000;
                break;
            case 'h':
                scaleLong *= 60 * 60 * 1000;
                break;
            case 'd':
                scaleLong *= 24 * 60 * 60 * 1000;
                break;
            default:
                scaleLong = 0L;
                break;
        }
        return scaleLong;
    }

    /**
     * Method to read cache file (config) into a map with metric IDs and their corresponding names.
     *
     * @param fileName
     */
    private static void readFile(String fileName) {
        try (Scanner scanner = new Scanner(new File(fileName))) {
            while (scanner.hasNextLine()) {
                String[] line = scanner.nextLine().split(";");
                metricIdsToMetricNamesMap.put(line[0], line[1]);
            }
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}

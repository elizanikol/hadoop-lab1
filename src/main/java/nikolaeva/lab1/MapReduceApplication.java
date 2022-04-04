package nikolaeva.lab1;

import lombok.extern.log4j.Log4j;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.util.Arrays;

/**
 * MapReduce Application
 */
@Log4j
public class MapReduceApplication {

    /**
     * main function to run application
     *
     * @param args: input and output folders, metricIDs config, scale and function
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        // Parse arguments
        if (args.length < 5) {
            throw new RuntimeException("You should specify input and output folders, " +
                    "metricIDs config, scale and aggregation function!");
        }
        if (!isScaleValid(args[3])) {
            throw new RuntimeException("Invalid scale!");
        }
        if (!isFunctionValid(args[4])) {
            throw new RuntimeException("Invalid function!");
        }
        // add scale and aggregation function to configuration
        Configuration conf = new Configuration();
        conf.setStrings("scale", args[3]);
        conf.setStrings("function", args[4]);

        // set MapReduce job
        Job job = Job.getInstance(conf, "aggregate values");
        job.setJarByClass(MapReduceApplication.class);
        job.setMapperClass(HW1Mapper.class);
        job.setMapOutputKeyClass(Record.class);
        job.setReducerClass(HW1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.addCacheFile(new Path(args[2]).toUri());

        // add input and output folders
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        log.info("=====================JOB STARTED=====================");
        job.waitForCompletion(true);
        log.info("=====================JOB ENDED=====================");

        // print counter statistics
        Counter counterMalformed = job.getCounters().findCounter(CounterType.MALFORMED);
        log.info("=====================COUNTERS " + counterMalformed.getName() + ": " + counterMalformed.getValue() + "=====================");
    }

    /**
     * Check if provided scale is valid:
     * scale has to be in the following format:
     *      [number][unit], where unit can be:
     *      s - for seconds;
     *      m - for minutes;
     *      h - for hours;
     *      d - for days.
     *
     * @param scale
     * @return true, if scale is valid
     */
    private static boolean isScaleValid(String scale) {
        if (scale.length() < 2) {
            return false;
        }
        char[] units = {'s', 'm', 'h', 'd'};
        char unit;
        try {
            Long.parseLong(scale.substring(0, scale.length() - 1));
            unit = scale.charAt(scale.length() - 1);
        } catch (Exception e) {
            return false;
        }
        return ArrayUtils.contains(units, unit);
    }

    /**
     * Check if provided function is valid:
     *
     * @param function
     * @return true, if function is valid
     */
    private static boolean isFunctionValid(String function) {
        String[] functions = {"sum", "avg", "max", "min"};
        return Arrays.asList(functions).contains(function);
    }
}

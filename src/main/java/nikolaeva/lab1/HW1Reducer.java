package nikolaeva.lab1;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer class
 */
@Log4j
public class HW1Reducer extends Reducer<Record, IntWritable, Text, IntWritable> {

    private static String function = "";
    private final static IntWritable result = new IntWritable();

    /**
     * Reducer initial setup method:
     * loads aggregation function name.
     *
     * @param context - job context
     */
    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        function = conf.getStrings("function")[0];
    }

    /**
     * Reduce method:
     * depending on the function aggregates values received from mapper.
     *
     * @param key - by which values are grouped
     * @param values - values from mapper
     * @param context - job context
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws RuntimeException
     */
    @Override
    protected void reduce(Record key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException, RuntimeException {
        int sum = 0;
        int counter = 0;
        int max = 0;
        int min = Integer.MAX_VALUE;
        int value = 0;

        while (values.iterator().hasNext()) {
            value = values.iterator().next().get();
            switch (function) {
                case "sum":
                    sum += value;
                    break;
                case "avg":
                    sum += value;
                    ++counter;
                    break;
                case ("max"):
                    max = Math.max(max, value);
                    break;
                case ("min"):
                    min = Math.min(min, value);
                    break;
                default:
                    throw new RuntimeException("Invalid function!");
            }
        }

        switch (function) {
            case "sum":
                result.set(sum);
                break;
            case "avg":
                result.set(sum / counter);
                break;
            case ("max"):
                result.set(max);
                break;
            case ("min"):
                result.set(min);
                break;
            default:
                break;
        }

        context.write(new Text(key.toString()), result);
    }
}

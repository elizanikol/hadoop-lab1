package nikolaeva.lab1;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Custom Type - class Record
 */
public class Record implements WritableComparable<Record> {
    private String metricName;
    private long timestamp;
    private String scale;

    /**
     * Default constructor
     */
    public Record() {
        this.metricName = "";
        this.timestamp = 0L;
        this.scale = "";
    }

    /**
     * Constructor
     *
     * @param metricName
     * @param timestamp
     * @param scale
     */
    public Record(String metricName, long timestamp, String scale) {
        set(metricName, timestamp, scale);
    }

    /**
     * Method set, used in map() function of HW1Mapper
     *
     * @param metricName
     * @param timestamp
     * @param scale
     */
    public void set(String metricName, long timestamp, String scale) {
        this.metricName = metricName;
        this.timestamp = timestamp;
        this.scale = scale;
    }

    /**
     * Method to serialize fields of this object to out.
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(metricName);
        out.writeLong(timestamp);
        out.writeUTF(scale);
    }

    /**
     * Method to deserialize fields of this object from in.
     *
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        metricName = in.readUTF();
        timestamp = in.readLong();
        scale = in.readUTF();
    }

    /**
     * Method to compare this object to the one sent as a parameter.
     * (used in mapper and reducer to sort values by keys)
     *
     * @param o (represents an object to which this one is compared)
     * @return -1 if this object is less than o;
     *          0 if this object is equal to o;
     *          1 if this object is greater than o.
     */
    @Override
    public int compareTo(Record o) {
        String thisMetricName = this.metricName;
        String thatMetricName = o.metricName;

        long thisTimestamp = this.timestamp;
        long thatTimestamp = o.timestamp;

        return (thisTimestamp < thatTimestamp ? -1 :
                (thisTimestamp == thatTimestamp ? thisMetricName.compareTo(thatMetricName) : 1));
    }

    /**
     * Method to check equality of 2 objects.
     * (used in assertions in tests)
     *
     * @param o (represents an object to which the equality of this one is checked)
     * @return true if this object is equal to o.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Record)) {
            return false;
        }
        Record tp = (Record) o;
        String thisMetricName = this.metricName;
        String thatMetricName = tp.metricName;

        long thisTimestamp = this.timestamp;
        long thatTimestamp = tp.timestamp;

        String thisScale = this.scale;
        String thatScale = tp.scale;

        return (thisTimestamp == thatTimestamp && thisMetricName.equals(thatMetricName)
                && thisScale.equals(thatScale));
    }

    /**
     * Method to calculate hash code of this object.
     *
     * @return hash code of this object.
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (metricName != null ? metricName.hashCode() : 0);
        result = prime * result + (scale != null ? scale.hashCode() : 0);
        result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    /**
     * Method to convert object to string.
     *
     * @return String, representing this object.
     */
    @Override
    public String toString() {
        return metricName + ", " + timestamp + ", " + scale;
    }
}


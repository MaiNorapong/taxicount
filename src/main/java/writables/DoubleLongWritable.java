package writables;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class DoubleLongWritable implements WritableComparable<DoubleLongWritable> {
    private DoubleWritable first;
    private LongWritable second;

    public DoubleLongWritable() {
        first = new DoubleWritable(0.0d);
        second = new LongWritable(0);
    }

    public DoubleLongWritable(DoubleWritable first, LongWritable second) {
        this.first = first;
        this.second = second;
    }

    public DoubleLongWritable(double first, long second) {
        this.first = new DoubleWritable(first);
        this.second = new LongWritable(second);
    }

    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    public DoubleWritable getFirst() {
        return first;
    }

    public void setFirst(DoubleWritable first) {
        this.first = first;
    }

    public LongWritable getSecond() {
        return second;
    }

    public void setSecond(LongWritable second) {
        this.second = second;
    }

    public int compareTo(DoubleLongWritable o) {
        int dist = Double.compare(this.getFirst().get(), o.getFirst().get());
        return dist == 0 ? Long.compare(this.getSecond().get(), o.getSecond().get()) : dist;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DoubleLongWritable that = (DoubleLongWritable) o;
        return Objects.equals(first, that.first) &&
                Objects.equals(second, that.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }

    @Override
    public String toString() {
        return "(" + first + "," + second + ")";
    }

    static public DoubleLongWritable fromString(String s) {
        String[] arr = s.split("[,)(]");
        return new DoubleLongWritable(Double.parseDouble(arr[1]), Long.parseLong(arr[2]));
    }
}

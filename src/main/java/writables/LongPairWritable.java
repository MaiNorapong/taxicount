package writables;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class LongPairWritable implements WritableComparable<LongPairWritable> {
    private LongWritable first, second;

    public LongPairWritable() {
        first = new LongWritable(0);
        second = new LongWritable(0);
    }

    public LongPairWritable(LongWritable first, LongWritable second) {
        this.first = first;
        this.second = second;
    }

    public LongPairWritable(long first, long second) {
        this.first = new LongWritable(first);
        this.second = new LongWritable(second);
    }

    public LongPairWritable(int first, int second) {
        this.first = new LongWritable(first);
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

    public LongWritable getSecond() {
        return second;
    }

    public void setSecond(LongWritable second) {
        this.second = second;
    }

    public LongWritable getFirst() {
        return first;
    }

    public void setFirst(LongWritable first) {
        this.first = first;
    }

    public int compareTo(LongPairWritable o) {
        int dist = Long.compare(this.getFirst().get(), o.getFirst().get());
        return dist == 0 ? Long.compare(this.getSecond().get(), o.getSecond().get()) : dist;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LongPairWritable that = (LongPairWritable) o;
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

    static public LongPairWritable fromString(String s) {
        String[] arr = s.split("[,)(]");
        return new LongPairWritable(Long.parseLong(arr[1]), Long.parseLong(arr[2]));
    }
}

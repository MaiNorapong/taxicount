package writables;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class DoublePairWritable implements WritableComparable<DoublePairWritable> {
    private DoubleWritable first, second;

    public DoublePairWritable() {
        first = new DoubleWritable(0.0d);
        second = new DoubleWritable(0.0d);
    }

    public DoublePairWritable(DoubleWritable first, DoubleWritable second) {
        this.first = first;
        this.second = second;
    }

    public DoublePairWritable(double first, double second) {
        this.first = new DoubleWritable(first);
        this.second = new DoubleWritable(second);
    }

    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    public DoubleWritable getSecond() {
        return second;
    }

    public void setSecond(DoubleWritable second) {
        this.second = second;
    }

    public DoubleWritable getFirst() {
        return first;
    }

    public void setFirst(DoubleWritable first) {
        this.first = first;
    }

    public int compareTo(DoublePairWritable o) {
        int dist = Double.compare(this.getFirst().get(), o.getFirst().get());
        return dist == 0 ? Double.compare(this.getSecond().get(), o.getSecond().get()) : dist;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DoublePairWritable that = (DoublePairWritable) o;
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

    static public DoublePairWritable fromString(String s) {
        String[] arr = s.split("[,)(]");
        return new DoublePairWritable(Double.parseDouble(arr[1]), Double.parseDouble(arr[2]));
    }
}
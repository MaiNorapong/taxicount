import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairWritable<T extends WritableComparable<T>> implements WritableComparable<PairWritable<T>> {

    private T first;
    private T second;

    @Override
    public int compareTo(PairWritable<T> other) {
        int cmp = first.compareTo(other.first);
        if (cmp != 0) {
            return cmp;
        }
        return second.compareTo(other.second);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }


    public PairWritable() {
        this.first = null;
        this.second = null;
    }

    public PairWritable(T first, T second) {
        this.first = first;
        this.second = second;
    }

    public T getFirst() {
        return first;
    }

    public T getSecond() {
        return second;
    }

    public void set(T first, T second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public String toString() {
        return "(" + first + "," + second + ")";
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PairWritable<?>)) {
            return false;
        } else {
            PairWritable<?> other = (PairWritable<?>) o;
            return first.equals(other.first) && second.equals(other.second);
        }
    }
}

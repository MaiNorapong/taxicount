import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class GeoLocationWritable implements WritableComparable<GeoLocationWritable> {
    private DoubleWritable latitude, longitude;

    //default constructor for (de)serialization
    public GeoLocationWritable() {
        latitude = new DoubleWritable(0.0d);
        longitude = new DoubleWritable(0.0d);
    }

    public GeoLocationWritable(DoubleWritable latitude, DoubleWritable longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public GeoLocationWritable(double latitude, double longitude) {
        this.latitude = new DoubleWritable(latitude);
        this.longitude = new DoubleWritable(longitude);
    }

    public void write(DataOutput dataOutput) throws IOException {
        latitude.write(dataOutput);
        longitude.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        latitude.readFields(dataInput);
        longitude.readFields(dataInput);
    }

    public DoubleWritable getLongitude() {
        return longitude;
    }

    public void setLongitude(DoubleWritable longitude) {
        this.longitude = longitude;
    }

    public DoubleWritable getLatitude() {
        return latitude;
    }

    public void setLatitude(DoubleWritable latitude) {
        this.latitude = latitude;
    }

    public int compareTo(GeoLocationWritable o) {
        int dist = Double.compare(this.getLatitude().get(), o.getLatitude().get());
        return dist == 0 ? Double.compare(this.getLongitude().get(), o.getLongitude().get()) : dist;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GeoLocationWritable that = (GeoLocationWritable) o;
        return Objects.equals(latitude, that.latitude) &&
                Objects.equals(longitude, that.longitude);
    }

    @Override
    public int hashCode() {
        return Objects.hash(latitude, longitude);
    }

    @Override
    public String toString() {
        return "(" + latitude + "," + longitude + ")";
    }

    static public GeoLocationWritable fromString(String s) {
        String[] arr = s.split("[,)(]");
        return new GeoLocationWritable(Double.parseDouble(arr[1]), Double.parseDouble(arr[2]));
    }
}
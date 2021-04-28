import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Objects;

public class GeoLocationWritable implements WritableComparable<GeoLocationWritable> {
    private DoubleWritable longitude, latitude;

    //default constructor for (de)serialization
    public GeoLocationWritable() {
        longitude = new DoubleWritable(0.0d);
        latitude = new DoubleWritable(0.0d);
    }

    public GeoLocationWritable(DoubleWritable longitude, DoubleWritable latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public GeoLocationWritable(double longitude, double latitude) {
        this.longitude = new DoubleWritable(longitude);
        this.latitude = new DoubleWritable(latitude);
    }

    public void write(DataOutput dataOutput) throws IOException {
        longitude.write(dataOutput);
        latitude.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        longitude.readFields(dataInput);
        latitude.readFields(dataInput);
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
        return Objects.equals(longitude, that.longitude) &&
                Objects.equals(latitude, that.latitude);
    }

    @Override
    public int hashCode() {
        return Objects.hash(longitude, latitude);
    }

    @Override
    public String toString() {
        return "(" + longitude + "," + latitude + ")";
    }

    static public GeoLocationWritable fromString(String s) {
        String[] arr = s.split("[,)(]");
        return new GeoLocationWritable(Double.parseDouble(arr[1]), Double.parseDouble(arr[2]));
    }
}
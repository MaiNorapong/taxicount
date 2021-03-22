import java.sql.Date;
import java.text.DateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class Main {
    public static void main(String[] args) {
//        ArrayList<String> list = new ArrayList<>();
//        StringTokenizer itr = new StringTokenizer("2,2016-01-01 00:00:03,2016-01-01 00:06:19,1,.79,-74.006149291992188,40.744918823242188,1,N,-73.993797302246094,40.741439819335938,2,6,0.5,0.5,0,0,0.3,7.3\n".toString(), ",");
//        while (itr.hasMoreTokens()) {
//            list.add(itr.nextToken().trim());
//        }
//        System.out.println(list.get(0));

        LocalDate date = LocalDate.parse("2016-01-01 00:00:00".split(" ")[0]);
        System.out.println(date.getDayOfWeek());



        /*
19
VendorID
tpep_pickup_datetime
tpep_dropoff_datetime
passenger_count
trip_distance
pickup_longitude
pickup_latitude
RatecodeID
store_and_fwd_flag
dropoff_longitude
dropoff_latitude
payment_type
fare_amount
extra
mta_tax
tip_amount
tolls_amount
improvement_surcharge
total_amount

0
 */
    }
}

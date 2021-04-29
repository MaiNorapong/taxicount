import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import writables.DoubleLongWritable;

import java.io.IOException;
import java.util.ArrayList;

public class TaxiAvgDist {
    public static class DistanceMapper
            extends Mapper<Object, Text, IntWritable, DoubleLongWritable> {

        public static int GROUP_SIZE = 1024;

        private static final LongWritable ONE = new LongWritable(1);

        private static int group = 0;
        private static int counter = 0;

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            ArrayList<String> values = Utils.readLineCsv(value.toString());
            if (values.size() != 19) {
                System.out.println("Unexpected number of tokens (" + values.size() + ") at " + key);
            }
            double pickupLongitude = Double.parseDouble(values.get(5));
            double pickupLatitude = Double.parseDouble(values.get(6));
            double dropoffLongitude = Double.parseDouble(values.get(9));
            double dropoffLatitude = Double.parseDouble(values.get(10));
            double dist = distance(pickupLatitude, dropoffLatitude, pickupLongitude, dropoffLongitude, 10.0, 10.0); // New York > Elevation acc. Google
            context.write(new IntWritable(group), new DoubleLongWritable(new DoubleWritable(dist), ONE));
            increment();
        }

        private static void increment() {
            counter = counter + 1;
            if (counter >= GROUP_SIZE) {
                group = group + 1;
                counter = 0;
            }
        }

        /**
         * Calculate distance between two points in latitude and longitude taking
         * into account height difference. If you are not interested in height
         * difference pass 0.0. Uses Haversine method as its base.
         * <p>
         * lat1, lon1 Start point lat2, lon2 End point el1 Start altitude in meters
         * el2 End altitude in meters
         *
         * @returns Distance in Meters
         * @reference https://stackoverflow.com/a/16794680
         */
        public static double distance(double lat1, double lat2, double lon1,
                                      double lon2, double el1, double el2) {

            final int R = 6371; // Radius of the earth

            double latDistance = Math.toRadians(lat2 - lat1);
            double lonDistance = Math.toRadians(lon2 - lon1);
            double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                    + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                    * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
            double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
            double distance = R * c * 1000; // convert to meters

            double height = el1 - el2;

            distance = Math.pow(distance, 2) + Math.pow(height, 2);

            return Math.sqrt(distance);
        }
    }

    public static class CountSumReducer
            extends Reducer<IntWritable, DoubleLongWritable, IntWritable, DoubleLongWritable> {

        public static int GROUP_SIZE = 1024;

        public void reduce(IntWritable key, Iterable<DoubleLongWritable> values, Context context)
                throws IOException, InterruptedException {
            double distSum = 0;
            long countSum = 0;
            for (DoubleLongWritable val : values) {
                distSum = distSum + val.getFirst().get();
                countSum = countSum + val.getSecond().get();
            }

            int group = key.get() / GROUP_SIZE;

            context.write(new IntWritable(group), new DoubleLongWritable(
                    new DoubleWritable(distSum),
                    new LongWritable(countSum)
            ));
        }
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        Utils.addOptions(options, "ior");
        CommandLineParser parser = new GnuParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
            if (cmd.getArgs().length < 1) {
                throw new ParseException("error: specify subJob");
            }
            String subJob = cmd.getArgs()[0];
            if (!subJob.equals("1")) {
                throw new ParseException("error: subJob doesn't exist");
            }
        } catch (ParseException e) {
            formatter.printHelp("TaxiAvgDist [options...] <subJob#>", "", options, "\n", true);
            System.out.println(e.getMessage());
            System.exit(1);
        }

        Path inFile = new Path(cmd.getOptionValue("input"));
        Path outFile = new Path(cmd.getOptionValue("output"));
        Configuration conf = new Configuration();
        if (cmd.getOptionValue("rmdir") != null) {
            FileSystem fs = FileSystem.get(conf);
            fs.delete(outFile, true);
        }

        Job job;
        job = Job.getInstance(conf, "taxicount avg distance: #1 (sum & count distance)");
        job.setJarByClass(TaxiAvgDist.class);

        job.setMapperClass(TaxiAvgDist.DistanceMapper.class);
        job.setCombinerClass(TaxiAvgDist.CountSumReducer.class);
        job.setReducerClass(TaxiAvgDist.CountSumReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleLongWritable.class);

        FileInputFormat.addInputPath(job, inFile);
        FileOutputFormat.setOutputPath(job, outFile);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

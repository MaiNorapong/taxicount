import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import writables.DoublePairWritable;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class TaxiCountLoc {
    public static class PickupLocMapper
            extends Mapper<Object, Text, DoublePairWritable, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);
        private static int precision = 4;

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            ArrayList<String> values = Utils.readLineCsv(value.toString());
            if (values.size() != 19) {
                System.out.println("Unexpected number of tokens (" + values.size() + ") at " + key);
            }
            BigDecimal pickupLongitude = BigDecimal.valueOf(Double.parseDouble(values.get(5)));
            BigDecimal pickupLatitude = BigDecimal.valueOf(Double.parseDouble(values.get(6)));
            DoublePairWritable pair = new DoublePairWritable(
                    pickupLatitude.setScale(PickupLocMapper.precision, BigDecimal.ROUND_HALF_UP).doubleValue(),
                    pickupLongitude.setScale(PickupLocMapper.precision, BigDecimal.ROUND_HALF_UP).doubleValue()
            );
            context.write(pair, ONE);
        }
    }

    public static class IntSumReducer
            extends Reducer<DoublePairWritable, IntWritable, DoublePairWritable, IntWritable> {

        private final IntWritable resultValue = new IntWritable();

        public void reduce(DoublePairWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            resultValue.set(sum);
            context.write(key, resultValue);
        }
    }

    public static class KeyValueSwappingMapper
            extends Mapper<Text, Text, LongWritable, DoublePairWritable> {

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(
                    new LongWritable(Long.parseLong(value.toString())),
                    DoublePairWritable.fromString(key.toString())
            );
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
            if (!subJob.equals("1") && !subJob.equals("2")) {
                throw new ParseException("error: subJob doesn't exist");
            }
        } catch (ParseException e) {
            formatter.printHelp("TaxiCountLoc [options...] <subJob#>", "", options, "\n", true);
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
        if (cmd.getArgs()[0].equals("1")) {
            job = Job.getInstance(conf, "taxicount location: #1 (count geolocations)");
            job.setJarByClass(TaxiCountLoc.class);

            job.setMapperClass(PickupLocMapper.class);
            job.setCombinerClass(TaxiCountLoc.IntSumReducer.class);
            job.setReducerClass(TaxiCountLoc.IntSumReducer.class);

            job.setOutputKeyClass(DoublePairWritable.class);
            job.setOutputValueClass(IntWritable.class);

//            job.setOutputFormatClass(SequenceFileOutputFormat.class);
        } else {
            job = Job.getInstance(conf, "taxicount location: #2 (sort sum:geolocations)");
            job.setJarByClass(TaxiCountLoc.class);

            job.setMapperClass(KeyValueSwappingMapper.class);
            job.setNumReduceTasks(1);
            job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(DoublePairWritable.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(DoublePairWritable.class);

            job.setInputFormatClass(KeyValueTextInputFormat.class);
        }
        FileInputFormat.addInputPath(job, inFile);
        FileOutputFormat.setOutputPath(job, outFile);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

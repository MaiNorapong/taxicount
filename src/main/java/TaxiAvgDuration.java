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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import writables.LongPairWritable;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

public class TaxiAvgDuration {
    public static class DurationMapper
            extends Mapper<Object, Text, IntWritable, LongPairWritable> {

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
            String tpepPickupDatetime = values.get(1);
            String tpepDropoffDatetime = values.get(2);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime pickupDatetime = LocalDateTime.parse(tpepPickupDatetime, formatter);
            LocalDateTime dropoffDatetime = LocalDateTime.parse(tpepDropoffDatetime, formatter);

            Duration duration = Duration.between(pickupDatetime, dropoffDatetime);
            LongWritable durationInSeconds = new LongWritable(duration.getSeconds());
            context.write(new IntWritable(group), new LongPairWritable(durationInSeconds, ONE));
            increment();
        }

        private static void increment() {
            counter = counter + 1;
            if (counter >= GROUP_SIZE) {
                group = group + 1;
                counter = 0;
            }
        }
    }

    public static class CountSumReducer
            extends Reducer<IntWritable, LongPairWritable, IntWritable, LongPairWritable> {

        public static int GROUP_SIZE = 1024;

        public void reduce(IntWritable key, Iterable<LongPairWritable> values, Context context)
                throws IOException, InterruptedException {
            long durationSum = 0;
            long countSum = 0;
            for (LongPairWritable val : values) {
                durationSum = durationSum + val.getFirst().get();
                countSum = countSum + val.getSecond().get();
            }

            int group = key.get() / GROUP_SIZE;

            context.write(new IntWritable(group), new LongPairWritable(
                    new LongWritable(durationSum),
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
            if (!subJob.equals("1") && !subJob.equals("2") ) {
                throw new ParseException("error: subJob doesn't exist");
            }
        } catch (ParseException e) {
            formatter.printHelp("TaxiAvgDuration [options...] <subJob#>", "", options, "\n", true);
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
            job = Job.getInstance(conf, "taxicount avg duration: #1 (sum & count duration)");
            job.setJarByClass(TaxiAvgDuration.class);

            job.setMapperClass(TaxiAvgDuration.DurationMapper.class);
            job.setCombinerClass(CountSumReducer.class);
            job.setReducerClass(CountSumReducer.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(LongPairWritable.class);
        } else {
            job = Job.getInstance(conf, "taxicount avg duration: #2 (sum / count)");
//            job.setJarByClass(TaxiCountLoc.class);
//
//            job.setMapperClass(TaxiCountLoc.KeyValueSwappingMapper.class);
//            job.setNumReduceTasks(1);
//            job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
//
//            job.setOutputKeyClass(LongWritable.class);
//            job.setOutputValueClass(DoublePairWritable.class);
//            job.setMapOutputKeyClass(LongWritable.class);
//            job.setMapOutputValueClass(DoublePairWritable.class);
        }
        FileInputFormat.addInputPath(job, inFile);
        FileOutputFormat.setOutputPath(job, outFile);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

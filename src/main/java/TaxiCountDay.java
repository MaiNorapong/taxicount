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

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class TaxiCountDay {
    public static class PickupDayMapper
            extends Mapper<Object, Text, Text, LongWritable> {

        private final static LongWritable ONE = new LongWritable(1);
        private Text word = new Text();

        public static ArrayList<String> readLineCsv(String line) {
            ArrayList<String> list = new ArrayList<>();
            StringTokenizer itr = new StringTokenizer(line, ",");
            while (itr.hasMoreTokens()) {
                list.add(itr.nextToken().trim());
            }
            return list;
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            ArrayList<String> values = readLineCsv(value.toString());
            if (values.size() != 19) {
                System.out.println("Unexpected number of tokens (" + values.size()  + ") at " + key);
            }
            String pickupDatetime = values.get(1);
            LocalDate pickupDate = LocalDate.parse(pickupDatetime.split(" ")[0]);
            word.set(String.valueOf(pickupDate.getDayOfWeek()));
            context.write(word, ONE);
        }
    }

    public static class LongSumReducer
            extends Reducer<Text,LongWritable,Text,LongWritable> {
        private LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        ArgUtils.addOptions(options, "ior");
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
        job = Job.getInstance(conf, "taxicount day: #1 (count days)");
        job.setJarByClass(TaxiCountDay.class);

        job.setMapperClass(PickupDayMapper.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(LongSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, inFile);
        FileOutputFormat.setOutputPath(job, outFile);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

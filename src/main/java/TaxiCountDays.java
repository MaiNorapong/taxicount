import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducers.LongSumReducer;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;

public class TaxiCountDays {
    public static class PickupDayMapper
            extends Mapper<Object, Text, Text, LongWritable> {

        private final static LongWritable ONE = new LongWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            ArrayList<String> values = Utils.readLineCsv(value.toString());
            if (values.size() != 19) {
                System.out.println("Unexpected number of tokens (" + values.size()  + ") at " + key);
            }
            String pickupDatetime = values.get(1);
            LocalDate pickupDate = LocalDate.parse(pickupDatetime.split(" ")[0]);
            word.set(String.valueOf(pickupDate.getDayOfWeek()));
            context.write(word, ONE);
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
            formatter.printHelp("TaxiCountDays [options...] <subJob#>", "", options, "\n", true);
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
        job = Job.getInstance(conf, "taxicount days: #1 (count days)");
        job.setJarByClass(TaxiCountDays.class);

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

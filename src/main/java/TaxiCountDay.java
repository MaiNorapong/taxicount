import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
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
            context.write(word, one);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "taxicount Day");
        job.setJarByClass(TaxiCountDay.class);

        job.setMapperClass(PickupDayMapper.class);
        job.setCombinerClass(TaxiCountDay.IntSumReducer.class);
        job.setReducerClass(TaxiCountDay.IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

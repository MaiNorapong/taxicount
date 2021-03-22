import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class TaxiCountLoc {
    public static class PickupLocMapper
            extends Mapper<Object, Text, PairWritable<FloatWritable>, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

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
            BigDecimal pickupLongitude = BigDecimal.valueOf(Double.parseDouble(values.get(5)));
            BigDecimal pickupLatitude = BigDecimal.valueOf(Double.parseDouble(values.get(6)));
            PairWritable<FloatWritable> pair = new PairWritable<>(
                    new FloatWritable(pickupLongitude.setScale(4, BigDecimal.ROUND_HALF_UP).floatValue()),
                    new FloatWritable(pickupLatitude.setScale(4, BigDecimal.ROUND_HALF_UP).floatValue())
            );
            context.write(pair, one);
        }
    }

    public static class IntSumReducer
            extends Reducer<PairWritable<FloatWritable>,IntWritable,PairWritable<FloatWritable>,IntWritable> {

//        private final Text resultKey = new Text();
        private final IntWritable resultValue = new IntWritable();

        public void reduce(PairWritable<FloatWritable> key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            resultValue.set(sum);
//            resultKey.set(key.toString());
//            context.write(resultKey, resultValue);
            context.write(key, resultValue);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "taxicount location");
        job.setJarByClass(TaxiCountLoc.class);

        job.setMapperClass(PickupLocMapper.class);
        job.setCombinerClass(TaxiCountLoc.IntSumReducer.class);
        job.setReducerClass(TaxiCountLoc.IntSumReducer.class);

        job.setOutputKeyClass(PairWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

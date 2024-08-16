import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

public class Median {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "median");
        job.setJarByClass(Median.class);
        job.setMapperClass(HDFSMapper.class);
        job.setReducerClass(HDFSReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class HDFSMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private final Text keyText = new Text("median");
        private final DoubleWritable number = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                try {
                    double num = Double.parseDouble(token);
                    number.set(num);
                    context.write(keyText, number);
                } catch (NumberFormatException e) {
                    // Ignore tokens that are not valid numbers
                }
            }
        }
    }

    public static class HDFSReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private final DoubleWritable median = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            List<Double> nums = new ArrayList<>();
            int count = 0;
            for (DoubleWritable val : values) {
                double num = val.get();
                nums.add(num);
                count++;
            }
            Collections.sort(nums);
            double medianValue;
            if (count % 2 == 0) {
                medianValue = (nums.get(count / 2 - 1) + nums.get(count / 2)) / 2.0;
            } else {
                medianValue = nums.get(count / 2);
            }
            median.set(medianValue);
            context.write(key, median);
        }
    }
}


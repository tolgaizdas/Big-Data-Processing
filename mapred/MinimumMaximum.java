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
import java.util.StringTokenizer;

public class MinimumMaximum {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "minimum_maximum");
        job.setJarByClass(MinimumMaximum.class);
        job.setMapperClass(HDFSMapper.class);
        job.setReducerClass(HDFSReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class HDFSMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private final Text minKey = new Text("min");
        private final Text maxKey = new Text("max");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            double firstNumber = Double.parseDouble(itr.nextToken());
            double min = firstNumber;
            double max = firstNumber;
            while (itr.hasMoreTokens()) {
                double number = Double.parseDouble(itr.nextToken());
                if (number < min) {
                    min = number;
                }
                if (number > max) {
                    max = number;
                }
            }
            context.write(minKey, new DoubleWritable(min));
            context.write(maxKey, new DoubleWritable(max));
        }
    }

    public static class HDFSReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private final Text minKey = new Text("min");
        private final Text maxKey = new Text("max");
        private double min = Double.MAX_VALUE;
        private double max = Double.MIN_VALUE;

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) {
            for (DoubleWritable val : values) {
                double value = val.get();
                if (key.toString().equals("min")) {
                    min = Math.min(min, value);
                } else {  // key is "max"
                    max = Math.max(max, value);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(minKey, new DoubleWritable(min));
            context.write(maxKey, new DoubleWritable(max));
        }
    }
}

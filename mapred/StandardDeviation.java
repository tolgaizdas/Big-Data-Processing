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

public class StandardDeviation {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "standard_deviation");
        job.setJarByClass(StandardDeviation.class);
        job.setMapperClass(HDFSMapper.class);
        job.setReducerClass(HDFSReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class HDFSMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private final Text keyText = new Text("standard_deviation");
        private final DoubleWritable number = new DoubleWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
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

    public static class HDFSReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private final DoubleWritable standardDeviation = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            double sumOfSquares = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                double num = val.get();
                sum += num;
                sumOfSquares += num * num;
                count++;
            }
            double mean = sum / count; // Calculate mean
            double variance = (sumOfSquares / count) - (mean * mean); // Calculate variance
            double stdDev = Math.sqrt(variance); // Calculate standard deviation
            standardDeviation.set(stdDev);
            context.write(key, standardDeviation);
        }
    }
}

package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;

public class MapReduceJobRunner {
    private final static String hadoopBin = System.getenv("HADOOP_HOME") + "/bin";

    private final static String inputDir = "/user/hadoop/inputs";
    private final static String outputDir = "/user/hadoop/outputs";

    public static void run(String fileName, String functionName, String inputName) {
        try {
            String inputPath = inputDir + "/" + fileName + "/" + inputName + ".txt";
            String outputPath = outputDir + "/" + fileName + "/" + functionName;

            removeOutputDir(outputPath);
            String jar = hadoopBin + "/" + functionName + ".jar";
            String command = String.format("%s/hadoop jar %s %s %s %s", hadoopBin, jar, functionName, inputPath, outputPath);
            // System.out.println("Running command: " + command);

            Process process = Runtime.getRuntime().exec(command, null, new File(hadoopBin));
            long startTime = System.currentTimeMillis(); // Start measuring time
            process.waitFor();
            long endTime = System.currentTimeMillis(); // Stop measuring time after the process completes
            double durationSeconds = (endTime - startTime) / 1000.0; // Convert to seconds with three decimal points
            DecimalFormat df = new DecimalFormat("#.###");
            System.out.println("Execution duration: " + df.format(durationSeconds) + " seconds");

            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static String getOutput(String fileName, String functionName) {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "your-hdfs-uri");
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(outputDir + "/" + fileName + "/" + functionName + "/part-r-00000");
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
            StringBuilder output = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }
            return output.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static void removeOutputDir(String outputPath) {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "your-hdfs-uri");
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(outputPath);
            fs.delete(path, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

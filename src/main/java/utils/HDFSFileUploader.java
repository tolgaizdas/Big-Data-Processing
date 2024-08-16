package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.file.Paths;

public class HDFSFileUploader {
    private final static String hdfsDirPath = "/user/hadoop/inputs";

    public static void main(String[] args) throws IOException {
    }

    public static void uploadFiles(String localDirPath) {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "your-hdfs-uri"); // Set your HDFS address
            FileSystem fs = FileSystem.get(conf);

            FileSystem hdfs = FileSystem.get(conf);
            String dirName = Paths.get(localDirPath).getFileName().toString();
            if (!hdfs.exists(new Path("/user"))) {
                hdfs.mkdirs(new Path("/user"));
            }
            if (!hdfs.exists(new Path("/user/hadoop"))) {
                hdfs.mkdirs(new Path("/user/hadoop"));
            }

            // Create HDFS directory if it doesn't exist
            if (!hdfs.exists(new Path(hdfsDirPath))) {
                hdfs.mkdirs(new Path(hdfsDirPath));
            }
            if (hdfs.exists(new Path(hdfsDirPath + "/" + dirName))) {
                System.out.println("Data already exists in HDFS. Skipping upload...");
                System.out.println("Done.");
                return;
            }
            hdfs.copyFromLocalFile(false, true, new Path(localDirPath), new Path(hdfsDirPath));
            System.out.println("Data successfully uploaded to HDFS.");

            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

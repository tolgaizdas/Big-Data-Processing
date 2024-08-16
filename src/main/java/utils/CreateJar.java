package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;

public class CreateJar {
    private final static String fileDir = Paths.get("").toAbsolutePath() + "/mapred";
    private final static String hadoopBin = System.getenv("HADOOP_HOME") + "/bin";

    public static void main(String[] args) {
        ArrayList<String> fileNames = new ArrayList<>();
        fileNames.add("MinimumMaximum");
        fileNames.add("Mean");
        fileNames.add("Median");
        fileNames.add("Range");
        fileNames.add("StandardDeviation");
        try {
            for (String fileName : fileNames) {
                copyToHadoop(fileName);
                compile(fileName);
                createJar(fileName);
            }
            copyToSlave("slave1");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void copyToSlave(String slave) throws IOException, InterruptedException {
        File directory = new File(hadoopBin);
        File[] files = directory.listFiles();
        if (files == null) {
            System.out.println("No files found in the functions directory.");
            return;
        }
        for (File file : files) {
            if (file.getName().endsWith(".jar") || file.getName().endsWith(".class") || file.getName().endsWith(".java")) {
                String scp = "scp " + file + " " + slave + ":" + hadoopBin;
                Process process = Runtime.getRuntime().exec(scp);
                process.waitFor();
            }
        }
    }

    private static void copyToHadoop(String fileName) throws IOException {
        String cpy = String.format("cp -f %s.java %s", fileDir + "/" + fileName, hadoopBin);
        Process cpyProcess = Runtime.getRuntime().exec(cpy);
        printOutput(cpyProcess.getInputStream());
        printOutput(cpyProcess.getErrorStream());
    }

    private static void compile(String fileName) throws IOException, InterruptedException {
        String compile = String.format("%s/hadoop com.sun.tools.javac.Main %s.java", hadoopBin, fileName);
        Process compileProcess = Runtime.getRuntime().exec(compile, null, new File(hadoopBin));
        printOutput(compileProcess.getInputStream());
        printOutput(compileProcess.getErrorStream());
        compileProcess.waitFor();
    }

    private static void createJar(String fileName) throws IOException, InterruptedException {
        String createJar = String.format("jar cf %s.jar %s$HDFSMapper.class %s$HDFSReducer.class %s.class", fileName, fileName, fileName, fileName);
        Process createJarProcess = Runtime.getRuntime().exec(createJar, null, new File(hadoopBin));
        printOutput(createJarProcess.getInputStream());
        printOutput(createJarProcess.getErrorStream());
        createJarProcess.waitFor();
    }

    private static void printOutput(java.io.InputStream inputStream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
    }
}

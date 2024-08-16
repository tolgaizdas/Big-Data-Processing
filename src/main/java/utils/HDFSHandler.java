package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

public class HDFSHandler {
    private final static String sbin = System.getenv("HADOOP_HOME") + "/sbin";
    private final static String bin = System.getenv("HADOOP_HOME") + "/bin";

    public static void startDFS() {
        try {
            String startDfs = sbin + "/start-dfs.sh";
            Process process = Runtime.getRuntime().exec(startDfs, null, new File(sbin));
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            process.waitFor();
            String startYarn = sbin + "/start-yarn.sh";
            process = Runtime.getRuntime().exec(startYarn, null, new File(sbin));
            reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            process.waitFor();
            System.out.println("Leaving safe mode...");
            leaveSafeMode();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void stopDFS() {
        try {
            System.out.println("Entering safe mode...");
            enterSafeMode();
            String stopYarn = sbin + "/stop-yarn.sh";
            Process process = Runtime.getRuntime().exec(stopYarn, null, new File(sbin));
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            process.waitFor();
            String stopDfs = sbin + "/stop-dfs.sh";
            process = Runtime.getRuntime().exec(stopDfs, null, new File(sbin));
            reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            process.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static boolean isRunning() {
        try {
            String command = bin + "/hadoop fs -ls /";
            Process process = Runtime.getRuntime().exec(command, null, new File(sbin));
            process.waitFor();
            return process.exitValue() == 0;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private static void enterSafeMode() {
        try {
            String command = bin + "/hadoop dfsadmin -safemode enter";
            Process process = Runtime.getRuntime().exec(command, null, new File(bin));
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            process.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void leaveSafeMode() {
        try {
            String command = bin + "/hadoop dfsadmin -safemode leave";
            Process process = Runtime.getRuntime().exec(command, null, new File(bin));
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            process.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

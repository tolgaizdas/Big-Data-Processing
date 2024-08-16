package utils;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class DataHandler {
    private final static String dataPath = Paths.get("").toAbsolutePath() + "/data";

    public static void main(String[] args) {
        extractData(dataPath + "/your-dataset.csv");
    }

    public static void extractData(String csvFile) {
        try (CSVReader reader = new CSVReader(new FileReader(csvFile))) {
            String fileName = csvFile.substring(csvFile.lastIndexOf("/") + 1, csvFile.lastIndexOf("."));
            String[] headers = reader.readNext();

            Path outputDir = Paths.get(dataPath + "/" + fileName);
            if (!Files.exists(outputDir)) {
                Files.createDirectory(outputDir);
            } else {
                for (int i = 0; i < headers.length; i++) {
                    if (Files.exists(Paths.get(outputDir + "/" + headers[i] + ".txt"))) {
                        System.out.println("Data for '" + headers[i] + "' already extracted. Skipping...");
                        headers[i] = null;
                    }
                }
            }

            for (String header : headers) {
                if (header != null) {
                    System.out.print("Extracting data for '" + header + "'...");
                    writeDataToFile(csvFile, header, outputDir + "/" + header + ".txt");
                    System.out.println(" Done.");
                }
            }
        } catch (IOException | CsvException e) {
            e.printStackTrace();
        }
    }

    private static void writeDataToFile(String filePath, String header, String outputPath) {
        try (CSVReader reader = new CSVReader(new FileReader(filePath)); PrintWriter writer = new PrintWriter(new FileWriter(outputPath))) {
            // Read the header
            String[] headers = reader.readNext();

            // Find the index of the header you want to extract
            int headerIndex = -1;
            for (int i = 0; i < headers.length; i++) {
                if (headers[i].equals(header)) {
                    headerIndex = i;
                    break;
                }
            }

            // If header not found, handle appropriately
            if (headerIndex == -1) {
                System.out.println("Header not found: " + header);
                return;
            }

            // Read and process each line to extract data under the specified header
            List<String> data = new ArrayList<>();
            String[] line;
            while ((line = reader.readNext()) != null) {
                if (line.length > headerIndex) {
                    if (!isNumeric(line[headerIndex])) {
                        System.out.println("Data is not numeric. Skipping...");
                        return;
                    }
                    data.add(line[headerIndex]);
                }
            }

            // Write the data to the output file
            for (String datum : data) {
                writer.println(datum);
            }
        } catch (IOException | CsvValidationException e) {
            e.printStackTrace();
        }
    }

    public static String getDataPath() {
        return dataPath;
    }

    public static boolean isNumeric(String data) {
        try {
            Double.parseDouble(data);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}

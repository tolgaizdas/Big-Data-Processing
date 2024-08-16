package gui;

import utils.DataHandler;
import utils.HDFSFileUploader;
import utils.HDFSHandler;
import utils.MapReduceJobRunner;

import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import java.awt.*;
import java.io.*;
import java.util.ArrayList;

public class GUI extends JFrame {
    private final static JPanel frame = new JPanel();
    private final JButton runButton;
    private final JComboBox<String> headersComboBox;
    private final JComboBox<String> functionComboBox;
    private final JTextArea outputTextArea;
    private final JTextArea logTextArea;
    private final JTextArea dataTextArea;
    private String filePath;
    private boolean isDFSRunning;


    public GUI() {
        setTitle("Hadoop GUI");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(1400, 600);
        setLocationRelativeTo(null);

        frame.setLayout(new BorderLayout());
        JPanel topPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
        isDFSRunning = HDFSHandler.isRunning();

        // Buttons
        JButton startDFSButton = new JButton("Start DFS");
        JButton stopDFSButton = new JButton("Stop DFS");
        runButton = new JButton("Run MapReduce Job");
        if (isDFSRunning) {
            startDFSButton.setEnabled(false);
        } else {
            stopDFSButton.setEnabled(false);
            runButton.setEnabled(false);
        }
        topPanel.add(startDFSButton);
        topPanel.add(stopDFSButton);

        // File selection
        JButton chooseFileButton = new JButton("Choose CSV File");
        chooseFileButton.setEnabled(isDFSRunning);
        topPanel.add(chooseFileButton);
        if (filePath == null) {
            runButton.setEnabled(false);
        }

        // Headers combo box
        headersComboBox = new JComboBox<>();
        headersComboBox.addActionListener(e -> {
            String selectedHeader = (String) headersComboBox.getSelectedItem();
            if (selectedHeader != null && !selectedHeader.equals("Select a header")) {
                updateDataTextArea(selectedHeader);
            }
        });
        headersComboBox.addItem("Select a header");
        headersComboBox.setPreferredSize(new Dimension(300, 30));
        topPanel.add(headersComboBox);

        // Function selection combo box
        functionComboBox = new JComboBox<>();
        functionComboBox.addItem("Mean");
        functionComboBox.addItem("Median");
        functionComboBox.addItem("MinimumMaximum");
        functionComboBox.addItem("Range");
        functionComboBox.addItem("StandardDeviation");
        functionComboBox.setPreferredSize(new Dimension(200, 30));
        topPanel.add(functionComboBox);
        topPanel.add(runButton);

        frame.add(topPanel, BorderLayout.NORTH);

        // Output text area
        outputTextArea = new JTextArea("OUTPUT\n");
        outputTextArea.append("======\n\n");
        outputTextArea.setEditable(false);
        JScrollPane outputScrollPane = new JScrollPane(outputTextArea);
        outputScrollPane.setPreferredSize(new Dimension(575, 600));
        frame.add(outputScrollPane, BorderLayout.EAST);

        // Log text area
        logTextArea = new JTextArea("LOGS\n");
        logTextArea.append("====\n\n");
        logTextArea.setEditable(false);
        JScrollPane logScrollPane = new JScrollPane(logTextArea);
        logScrollPane.setPreferredSize(new Dimension(575, 600));
        frame.add(logScrollPane, BorderLayout.WEST);

        // Data text area
        dataTextArea = new JTextArea("SAMPLE DATA\n");
        dataTextArea.append("==========\n\n");
        dataTextArea.append("Choose a file to see sample data.\n");
        dataTextArea.setEditable(false);
        JScrollPane dataScrollPane = new JScrollPane(dataTextArea);
        dataScrollPane.setPreferredSize(new Dimension(250, 600));
        frame.add(dataScrollPane, BorderLayout.CENTER);

        // Button actions
        chooseFileButton.addActionListener(e -> {
            JFileChooser fileChooser = new JFileChooser(".");
            FileNameExtensionFilter filter = new FileNameExtensionFilter("CSV Files", "csv");
            fileChooser.setFileFilter(filter);
            int returnVal = fileChooser.showOpenDialog(GUI.this);
            if (returnVal == JFileChooser.APPROVE_OPTION) {
                filePath = fileChooser.getSelectedFile().getAbsolutePath();
                if (isDFSRunning) {
                    runButton.setEnabled(true);
                }
                populateHeadersComboBox(readCSVHeaders(filePath));
                logTextArea.append("Extracting data...\n");
                SwingUtilities.invokeLater(() -> {
                    PrintStream originalOut = System.out;
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    PrintStream newOut = new PrintStream(baos);
                    System.setOut(newOut);
                    DataHandler.extractData(filePath);
                    System.out.flush();
                    System.setOut(originalOut);
                    String output = baos.toString();
                    logTextArea.append(output);
                    logTextArea.append("Data extracted.\n");
                });

                SwingUtilities.invokeLater(() -> {
                    PrintStream originalOut = System.out;
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    PrintStream newOut = new PrintStream(baos);
                    System.setOut(newOut);
                    String fileName = filePath.substring(filePath.lastIndexOf("/") + 1, filePath.lastIndexOf("."));
                    HDFSFileUploader.uploadFiles(DataHandler.getDataPath() + "/" + fileName);
                    System.out.flush();
                    System.setOut(originalOut);
                    String output = baos.toString();
                    logTextArea.append(output);
                });
                logTextArea.getCaret().setDot(Integer.MAX_VALUE);
            }
        });

        startDFSButton.addActionListener(e -> {
            if (isDFSRunning) {
                logTextArea.append("DFS is already running.\n");
                return;
            }
            logTextArea.append("Starting DFS...\n");
            startDFSButton.setEnabled(false);
            SwingUtilities.invokeLater(() -> {
                PrintStream originalOut = System.out;
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                PrintStream newOut = new PrintStream(baos);
                System.setOut(newOut);
                HDFSHandler.startDFS();
                System.out.flush();
                System.setOut(originalOut);
                String output = baos.toString();
                logTextArea.append(output);
                logTextArea.append("DFS started.\n");
                isDFSRunning = true;
                stopDFSButton.setEnabled(true);
                chooseFileButton.setEnabled(true);
                if (filePath != null) {
                    runButton.setEnabled(true);
                }
            });
            logTextArea.getCaret().setDot(Integer.MAX_VALUE);
        });

        stopDFSButton.addActionListener(e -> {
            if (!isDFSRunning) {
                logTextArea.append("DFS is not running.\n");
                return;
            }
            logTextArea.append("Stopping DFS...\n");
            stopDFSButton.setEnabled(false);
            runButton.setEnabled(false);
            chooseFileButton.setEnabled(false);
            SwingUtilities.invokeLater(() -> {
                PrintStream originalOut = System.out;
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                PrintStream newOut = new PrintStream(baos);
                System.setOut(newOut);
                HDFSHandler.stopDFS();
                System.out.flush();
                System.setOut(originalOut);
                String output = baos.toString();
                logTextArea.append(output);
                logTextArea.append("DFS stopped.\n");
                isDFSRunning = false;
                startDFSButton.setEnabled(true);
            });
            logTextArea.getCaret().setDot(Integer.MAX_VALUE);
        });

        runButton.addActionListener(e -> {
            if (!isDFSRunning) {
                logTextArea.append("Start DFS to run a MapReduce job.\n");
                return;
            }
            if (filePath == null) {
                logTextArea.append("Choose a file to run a MapReduce job.\n");
                return;
            }
            stopDFSButton.setEnabled(false);
            runButton.setEnabled(false);
            String fileName = filePath.substring(filePath.lastIndexOf("/") + 1, filePath.lastIndexOf("."));
            String function = (String) functionComboBox.getSelectedItem();
            String header = (String) headersComboBox.getSelectedItem();
            logTextArea.append("Running '" + function + "' on '" + fileName + "." + header + "'...\n");
            SwingUtilities.invokeLater(() -> {
                PrintStream originalOut = System.out;
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                PrintStream newOut = new PrintStream(baos);
                System.setOut(newOut);
                MapReduceJobRunner.run(fileName, function, header);
                System.out.flush();
                System.setOut(originalOut);
                String output = baos.toString();
                logTextArea.append(output);
                logTextArea.append("Job completed.\n");
                outputTextArea.append(fileName + "." + header + ":\n" + MapReduceJobRunner.getOutput(fileName, function) + "\n");
                stopDFSButton.setEnabled(true);
                runButton.setEnabled(true);
            });
            logTextArea.getCaret().setDot(Integer.MAX_VALUE);
            outputTextArea.getCaret().setDot(Integer.MAX_VALUE);
        });

        add(frame);
        setVisible(true);
    }

    private ArrayList<String> readCSVHeaders(String filePath) {
        ArrayList<String> headers = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            String headerLine = reader.readLine(); // Read the header line
            if (headerLine != null) {
                String[] headersArray = headerLine.split(",");
                String firstDataLine = reader.readLine(); // Read the first line of data
                if (firstDataLine != null) {
                    String[] firstData = firstDataLine.split(",");
                    if (firstData.length == headersArray.length) {
                        for (int i = 0; i < firstData.length; i++) {
                            if (DataHandler.isNumeric(firstData[i].trim())) {
                                headers.add(headersArray[i].trim());
                            }
                        }
                    }
                }
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return headers;
    }

    private void populateHeadersComboBox(ArrayList<String> headers) {
        headersComboBox.removeAllItems();
        for (String header : headers) {
            if (header.toLowerCase().contains("id")) {
                continue;
            }
            headersComboBox.addItem(header);
        }
        headersComboBox.setMaximumRowCount(headersComboBox.getItemCount());
    }

    // Method to update the data text area with the first 10 data points for the selected header
    private void updateDataTextArea(String selectedHeader) {
        dataTextArea.setText("SAMPLE DATA\n");
        dataTextArea.append("==========\n\n");
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            int headerIndex = -1;
            int count = 0;
            while (count < 10 && (line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (headerIndex == -1) {
                    // Find the index of the selected header
                    for (int i = 0; i < parts.length; i++) {
                        if (parts[i].trim().equals(selectedHeader)) {
                            headerIndex = i;
                            break;
                        }
                    }
                    if (headerIndex == -1) {
                        // Header not found in the file
                        dataTextArea.setText("Header not found in the file.");
                        return;
                    } else {
                        dataTextArea.append(selectedHeader + "\n");
                        for (int i = 0; i < selectedHeader.length(); i++) {
                            dataTextArea.append("-");
                        }
                        dataTextArea.append("\n");
                    }
                } else {
                    String data = parts[headerIndex].trim();
                    try {
                        Double.parseDouble(data);
                    } catch (NumberFormatException e) {
                        dataTextArea.append("Data type is not numeric.\n");
                        runButton.setEnabled(false);
                        return;
                    }
                    if (isDFSRunning) {
                        runButton.setEnabled(true);
                    }
                    dataTextArea.append(parts[headerIndex].trim() + "\n");
                    count++;
                }
            }
            dataTextArea.append(".....\n....\n...\n..\n.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


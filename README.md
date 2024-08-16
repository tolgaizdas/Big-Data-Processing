# Big Data Performance Evaluation with Apache Hadoop

## Overview

This project evaluates the performance of Apache Hadoop in processing large datasets by applying various statistical functions. It compares the efficiency of single-node versus multi-node setups, with the multi-node configuration consisting of two virtual machines on the same device.

## Functions Used

- Mean
- Median
- Minimum-Maximum
- Range
- Standard Deviation

## Implementation

- **Java Classes:** Separate classes for each statistical function with Mapper and Reducer methods.
- **Helper Classes:** 
  - Compiles and deploys Java classes to Hadoop.
  - Splits dataset into files and transfers to Hadoop DFS.
  - Executes Map-Reduce jobs and returns results.
- **GUI:** Provides an interface for service management and function execution.

## Dataset

The dataset used for performance evaluation is the [Brewery Operations and Market Analysis Dataset](https://www.kaggle.com/datasets/ankurnapa/brewery-operations-and-market-analysis-dataset) from Kaggle.

## Performance Results

### pH_Level

| Function            | Single Node (Seconds) | 2 Nodes (Seconds) |
|---------------------|------------------------|--------------------|
| Mean                | 18.506                 | 15.661             |
| Median              | 22.488                 | 20.730             |
| Minimum-Maximum     | 22.507                 | 19.173             |
| Range               | 21.930                 | 19.720             |
| Standard Deviation  | 17.463                 | 16.213             |
| **Total Time**      | **102.894**            | **91.497**         |

### Fermentation_Time

| Function            | Single Node (Seconds) | 2 Nodes (Seconds) |
|---------------------|------------------------|--------------------|
| Mean                | 20.496                 | 18.414             |
| Median              | 22.521                 | 23.209             |
| Minimum-Maximum     | 24.609                 | 22.763             |
| Range               | 24.582                 | 21.728             |
| Standard Deviation  | 19.902                 | 18.660             |
| **Total Time**      | **112.110**            | **104.774**        |

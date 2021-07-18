package com.apache.spark.stuff.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvReaderFactory {

  public Dataset<Row> apply(SparkSession sparkSession, String sourceFileNameAndPath) {
    return sparkSession.read().option("header", true).csv(sourceFileNameAndPath);
  }
}

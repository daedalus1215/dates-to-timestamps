package com.apache.spark.stuff.functions.readers;

import java.util.function.BiFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GetDatasetFromCsv implements BiFunction<SparkSession, String, Dataset<Row>> {

  @Override
  public Dataset<Row> apply(SparkSession sparkSession, String sourceFileNameAndPath) {
    System.out.println("Source file: " + sourceFileNameAndPath);
    return sparkSession.read()
        .option("header", true)
        .csv(sourceFileNameAndPath);
  }
}

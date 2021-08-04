package com.apache.spark.stuff.functions.readers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GetDatasetFromCsv extends AbstractReader {

  public GetDatasetFromCsv(String sourceFileNameAndPath) {
    super(sourceFileNameAndPath);
  }

  @Override
  public Dataset<Row> apply(SparkSession sparkSession) {
    System.out.println("Source file: " + this.sourceFileNameAndPath);
    return sparkSession.read()
        .option("header", true)
        .csv(this.sourceFileNameAndPath);
  }
}

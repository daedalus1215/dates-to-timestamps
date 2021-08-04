package com.apache.spark.stuff.functions.readers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GetDatasetFromJsonMultilineRecursive extends AbstractReader {

  public GetDatasetFromJsonMultilineRecursive(String sourceFileNameAndPath) {
    super(sourceFileNameAndPath);
  }

  @Override
  public Dataset<Row> apply(SparkSession sparkSession) {
    System.out.println("Source file: " + sourceFileNameAndPath);
    return sparkSession.read()
        .option("multiline", true)
        .option("recursiveFileLookup", "true")
        .json(sourceFileNameAndPath);
  }
}

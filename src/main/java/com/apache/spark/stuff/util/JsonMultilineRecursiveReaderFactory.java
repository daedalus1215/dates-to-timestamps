package com.apache.spark.stuff.util;

import java.util.function.BiFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JsonMultilineRecursiveReaderFactory implements
    BiFunction<SparkSession, String, Dataset<Row>> {

  @Override
  public Dataset<Row> apply(SparkSession sparkSession, String sourceFileNameAndPath) {
    return sparkSession.read()
        .option("recursiveFileLookup", "true")
        .json(sourceFileNameAndPath);
  }
}

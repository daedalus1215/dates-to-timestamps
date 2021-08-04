package com.apache.spark.stuff.functions.writers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CsvWriter implements WriterInterface {

  private final String RESOURCES_DIRECTORY = "src/main/resources/";

  @Override
  public void accept(Dataset<Row> rowDataset, String subPath) {
    rowDataset.write()
        .option("header", "true")
        .mode("overwrite")
        .csv(RESOURCES_DIRECTORY
            .concat(subPath)
            .concat("/"));
  }
}

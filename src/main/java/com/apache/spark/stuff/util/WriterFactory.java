package com.apache.spark.stuff.util;

import java.util.function.BiConsumer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class WriterFactory implements BiConsumer<Dataset<Row>, String> {

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

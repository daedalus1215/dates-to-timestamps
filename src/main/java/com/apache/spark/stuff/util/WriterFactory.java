package com.apache.spark.stuff.util;

import java.util.function.Consumer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class WriterFactory implements Consumer<Dataset<Row>> {

  @Override
  public void accept(Dataset<Row> rowDataset) {
    rowDataset.write()
        .option("header", "true")
        .mode("overwrite")
        .csv("src/main/resources/target.csv");
  }
}

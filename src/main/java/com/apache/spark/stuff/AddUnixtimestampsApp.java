package com.apache.spark.stuff;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.unix_timestamp;

import com.apache.spark.stuff.functions.GetSparkSession;
import com.apache.spark.stuff.functions.readers.GetDatasetFromCsv;
import com.apache.spark.stuff.functions.writers.CsvWriter;
import com.apache.spark.stuff.functions.writers.WriterInterface;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AddUnixtimestampsApp {

  @SuppressWarnings("resource")
  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "d:/hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    // Declare
    final GetSparkSession getSparkSession = new GetSparkSession();
    final GetDatasetFromCsv getDatasetFromCsv = new GetDatasetFromCsv();
    final WriterInterface writer = new CsvWriter();

    // Setup
    final SparkSession sparkSession = getSparkSession.get();
    Dataset<Row> historicalPrices = getDatasetFromCsv.apply(sparkSession, args[0]);

    final Dataset<Row> rowDataset = historicalPrices
        .withColumn("Unix", unix_timestamp(col("Date")));

    writer.accept(rowDataset, "AddUnixtimestampsApp");

    sparkSession.close();
  }
}

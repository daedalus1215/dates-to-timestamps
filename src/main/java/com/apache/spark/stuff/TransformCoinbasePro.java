package com.apache.spark.stuff;

import static org.apache.spark.sql.functions.abs;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.unix_timestamp;

import com.apache.spark.stuff.functions.writers.CsvWriter;
import com.apache.spark.stuff.functions.readers.GetDatasetFromCsv;
import com.apache.spark.stuff.functions.GetSparkSession;
import com.apache.spark.stuff.functions.writers.WriterInterface;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Transform Coinbase Pro spreadsheet into columns I can use for the other app
 */
public class TransformCoinbasePro {

  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "d:/hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    // Declare everything
    final GetSparkSession getSparkSession = new GetSparkSession();
    final GetDatasetFromCsv getDatasetFromCsv = new GetDatasetFromCsv();
    final WriterInterface writer = new CsvWriter();

    // Setup
    final SparkSession sparkSession = getSparkSession.get();
    final Dataset<Row> originalDataset = getDatasetFromCsv.apply(sparkSession, args[0]);

    writer.accept(originalDataset
            .withColumn("Prep Columns ->", lit("----->"))
            .withColumn("created_date", to_date(col("created at")))
            .withColumn("Starting here ->", lit("--------------->"))
            .withColumn("Activity", col("side"))
            .withColumn("PricePerCoin", col("price"))
            .withColumn("Time", col("created_date"))
            .withColumn("Date", regexp_replace(to_timestamp(col("created_date")), "T", " "))
            .withColumn("Unix", unix_timestamp(col("created_date")))
            .withColumn("Order", abs(col("total")))
            .withColumn("Amount", col("size"))
        .withColumn("Coin", col("size unit"))
        .withColumn("Source", lit("Coinbase Pro"))
        .withColumn("FormatName", lit("Coinbase Pro"))
        ,"TransformCoinbasePro"
//        .show();
  );
  }
}

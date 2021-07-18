package com.apache.spark.stuff;

import com.apache.spark.stuff.util.JsonMultilineReaderFactory;
import com.apache.spark.stuff.util.SparkSessionFactory;
import com.apache.spark.stuff.util.WriterFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TransformCoinbasePro {

  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "d:/hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    // Declare everything
    final SparkSessionFactory sparkSessionFactory = new SparkSessionFactory();
    final JsonMultilineReaderFactory jsonMultilineReaderFactory = new JsonMultilineReaderFactory();
    final WriterFactory writerFactory = new WriterFactory();

    // Setup
    final SparkSession sparkSession = sparkSessionFactory.get();
    final Dataset<Row> originalDataset = jsonMultilineReaderFactory.apply(sparkSession, args[0]);

    // product - Coin
    // side - Activity
    // created at - Time
    // size - Amount
    // price - PricePerCoin
    // abs(total) - Amount
    // unix_timestamp(created at) - Unix
    // date_format(create at) - Date
  }
}

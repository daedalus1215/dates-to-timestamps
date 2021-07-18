package com.apache.spark.stuff;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.unix_timestamp;

import com.apache.spark.stuff.util.CsvReaderFactory;
import com.apache.spark.stuff.util.SparkSessionFactory;
import com.apache.spark.stuff.util.WriterFactory;
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
    final SparkSessionFactory sparkSessionFactory = new SparkSessionFactory();
    final CsvReaderFactory csvReaderFactory = new CsvReaderFactory();
    final WriterFactory writerFactory = new WriterFactory();

    // Setup
    final SparkSession sparkSession = sparkSessionFactory.get();
    Dataset<Row> historicalPrices = csvReaderFactory.apply(sparkSession, args[0]);

    final Dataset<Row> rowDataset = historicalPrices
        .withColumn("Unix", unix_timestamp(col("Date")));

    writerFactory.accept(rowDataset);

    sparkSession.close();
  }
}

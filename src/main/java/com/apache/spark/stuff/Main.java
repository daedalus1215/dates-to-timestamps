package com.apache.spark.stuff;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.unix_timestamp;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

  @SuppressWarnings("resource")
  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "d:/hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    SparkConf conf = new SparkConf()
        .setAppName("startingSpark")
        .setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    final SparkSession sparkSession =
        SparkSession.builder()
            .appName("testingSql") // name we see this in a profiler
            .master("local[*]") // how many cores
            .config("spark.sql.warehouse.dir", "file:///d:/tmp/") // required
            .getOrCreate();

    Dataset<Row> historicalPrices =
        sparkSession.read().option("header", true).csv("src/main/resources/source.csv");

    final Dataset<Row> rowDataset = historicalPrices
        .withColumn("Unix", unix_timestamp(col("Date")));

    rowDataset.write()
        .option("header", "true")
        .mode ("overwrite")
        .csv("src/main/resources/target.csv");

    sparkSession.close();
  }
}

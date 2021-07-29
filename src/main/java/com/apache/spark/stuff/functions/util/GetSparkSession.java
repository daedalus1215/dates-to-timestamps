package com.apache.spark.stuff.functions.util;

import java.util.function.Supplier;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class GetSparkSession implements Supplier<SparkSession> {

  @Override
  public SparkSession get() {
    SparkConf conf = new SparkConf()
        .setAppName("startingSpark")
        .setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);
    return SparkSession.builder()
        .appName("testingSql") // name we see this in a profiler
        .master("local[*]") // how many cores
        .config("spark.sql.warehouse.dir", "file:///d:/tmp/") // required
        .getOrCreate();
  }
}

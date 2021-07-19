package com.apache.spark.stuff;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_date;

import com.apache.spark.stuff.util.JsonMultilineReaderFactory;
import com.apache.spark.stuff.util.SparkSessionFactory;
import com.apache.spark.stuff.util.WriterFactory;
import com.sun.prism.PixelFormat.DataType;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Test {

  @SuppressWarnings("resource")
  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "d:/hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    // Declare everything
    final SparkSessionFactory sparkSessionFactory = new SparkSessionFactory();
    final JsonMultilineReaderFactory jsonMultilineReaderFactory = new JsonMultilineReaderFactory();
    final WriterFactory writerFactory = new WriterFactory();

    final SparkSession sparkSession = sparkSessionFactory.get();

    // Setup
    final Dataset<Row> ravencoinRig = jsonMultilineReaderFactory.apply(sparkSession, args[0])
        .withColumn("Day", to_date(col("date")));

    final Dataset<Row> bagginsRig = jsonMultilineReaderFactory.apply(sparkSession, args[1])
        .withColumn("Day", to_date(col("date")));

    final Dataset<Row> combined = ravencoinRig.union(bagginsRig);

    final Dataset<Row> agg = combined
        .withColumnRenamed("name", "Name")
        .withColumn("Wattz", col("watts").cast(String.valueOf(DataType.INT)))
        .groupBy(col("Day"), col("name"))
        .agg(count(lit(1)).alias("Number of Log Entries"),
            avg("Wattz").alias("Average Watts an Hour"));

    final Dataset<Row> rowDataset = agg
        .withColumn("Energy Consumption for Day", col("Average Watts an Hour").divide(1000))
        .withColumn("Energy Cost for Day", col("Energy Consumption for Day").multiply(0.07));

//    writerFactory.accept(rowDataset);
    rowDataset.show();

    sparkSession.close();
  }
}

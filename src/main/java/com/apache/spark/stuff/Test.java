package com.apache.spark.stuff;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.to_date;

import com.apache.spark.stuff.util.SparkSessionFactory;
import com.apache.spark.stuff.util.JsonMultilineReaderFactory;
import com.apache.spark.stuff.util.WriterFactory;
import com.sun.prism.PixelFormat.DataType;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
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

    // Setup
    final SparkSession sparkSession = sparkSessionFactory.get();
    final Dataset<Row> originalDataset = jsonMultilineReaderFactory.apply(sparkSession, args[0]);

    final Dataset<Row> agg = originalDataset
        .withColumn("wattz", col("watts").cast(String.valueOf(DataType.INT)))
        .withColumn("date_no_time", to_date(col("date")))
        .groupBy(col("DeviceHost"), col("date_no_time"))
        .agg(
            count(lit(1)).alias("Num_Of_Records"),
            sum("wattz").alias("summed_watts"));

    final Dataset<Row> rowDataset = agg
        .withColumn("energy_for_day",
            col("summed_watts").divide(col("Num_Of_Records")).divide(1000))
        .withColumn("cost_for_day", col("energy_for_day").multiply(0.07));

    writerFactory.accept(rowDataset);

    sparkSession.close();
  }

  private static Dataset<Row> getAverage(Dataset<Row> ds) {
    final Column dateFormat = date_format((col("date")), "yyyy-MM-dd HH:mm:ss");
    return ds
        .withColumn("check", dateFormat)
        .withColumn("wattz", col("watts").cast(String.valueOf(DataType.INT)))
        .groupBy(col("DeviceHost")).avg("wattz");
  }
}

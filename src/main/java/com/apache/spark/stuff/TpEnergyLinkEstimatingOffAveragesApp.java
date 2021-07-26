package com.apache.spark.stuff;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.year;

import com.apache.spark.stuff.functions.util.GetDatasetFromJsonMultilineRecursive;
import com.apache.spark.stuff.functions.util.GetSparkSession;
import com.apache.spark.stuff.functions.util.WriterFactory;
import com.sun.prism.PixelFormat.DataType;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class TpEnergyLinkEstimatingOffAveragesApp {

  @SuppressWarnings("resource")
  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "d:/hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    final String pathToFiles = args[0];
    final String removeableSubString = args[1];

    // Declare everything
    final GetSparkSession getSparkSession = new GetSparkSession();
    final GetDatasetFromJsonMultilineRecursive jsonMultilineReaderFactory = new GetDatasetFromJsonMultilineRecursive();
    final WriterFactory writerFactory = new WriterFactory();

    final SparkSession sparkSession = getSparkSession.get();

    // Setup
    final Dataset<Row> testing = jsonMultilineReaderFactory.apply(sparkSession, pathToFiles)
        .withColumn("Name", input_file_name())
        .withColumn("FormatName", regexp_replace(col("Name"), removeableSubString, ""))
        .withColumn("Date", date_format(col("ts").divide(1000).cast(DataTypes.TimestampType), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("Day", to_date(col("Date")))
        .withColumn("Month", month(col("Date")))
        .withColumn("Year", year(col("Date")));

//    final Dataset<Row> testing2 = jsonMultilineReaderFactory.apply(sparkSession, args[1])
//        .withColumn("Name", lit(args[1]))
//        .withColumn("Date", date_format(col("ts").divide(1000).cast(DataTypes.TimestampType),
//            "yyyy-MM-dd HH:mm:ss"))
//        .withColumn("Day", to_date(col("Date")))
//        .withColumn("Month", month(col("Date")))
//        .withColumn("Year", year(col("Date")));

    final Dataset<Row> combined = testing;
//        .union(testing2);
//        .union(fiveNintyAndFiftySevenHundred);

    final Dataset<Row> agg = combined
        .withColumn("Wattz", col("pw").cast(String.valueOf(DataType.INT)))
        .groupBy(col("Month"), col("Year"))
        .agg(count(lit(1)).alias("Number of Log Entries"),
            avg("Wattz").alias("Average Watts an Hour"));

    //@TODO: Doublecheck these numbers
    final Dataset<Row> rowDataset = agg.withColumn("Energy Consumption for Day",
            col("Average Watts an Hour").divide(1000))
        .withColumn("Energy Cost for Day",
            col("Energy Consumption for Day").multiply(0.07).multiply(24))
        .withColumn("Expected Monthly Cost",
            col("Energy Cost for Day").multiply(30))
        .withColumn("Watt Hours a day",
            col("Energy Consumption for Day").multiply(24))
        .withColumn("Watt Hours a Month",
            col("Energy Consumption for Day").multiply(24).multiply(30).multiply(1000));

    writerFactory.accept(rowDataset.repartition(1), "tpEnergyLinkEstimatingOffAveragesApp");
//    rowDataset.show();

    sparkSession.close();
  }
}

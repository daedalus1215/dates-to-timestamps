package com.apache.spark.stuff.tpEnergyLink;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.year;

import com.apache.spark.stuff.functions.GetPlugIdAndNameDataset;
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


public class TpEnergyLinkApp {

  @SuppressWarnings("resource")
  /**
   * The arguments are still being flushed out, the args[1] will probably stay, args[0] is more of a spike for the moment.
   * args[0] = "/Users/{userName}/{pathToProject}/src/main/resources/tplink/{idOfAPlugin}"
   * args[1] = "file:///Users/{userName}/{pathToProject}/src/main/resources/tplink/"
   */
  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "d:/hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    final String pathToFiles = args[0];
    final String pathToFiles2 = args[1];
    final String pathToFiles3 = args[2];
    final String absolute_path_to_source_directory = args[3];

    // Declare everything
    final GetIdField getIdField = new GetIdField();
    final JoinWithPluginIdAndName joinWithPluginIdAndName = new JoinWithPluginIdAndName();

    final GetSparkSession getSparkSession = new GetSparkSession();
    final GetDatasetFromJsonMultilineRecursive getDatasetFromJsonMultilineRecursive = new GetDatasetFromJsonMultilineRecursive();
    final WriterFactory writerFactory = new WriterFactory();

    // Setup
    final SparkSession sparkSession = getSparkSession.get();
    final GetPlugIdAndNameDataset getPlugIdAndNameDataset = new GetPlugIdAndNameDataset(sparkSession);
    final Dataset<Row> pluginIdAndNameDS = getPlugIdAndNameDataset.get();

    final Dataset<Row> plugin1 = getDatasetFromJsonMultilineRecursive.apply(sparkSession, pathToFiles);
    final Dataset<Row> plugin2 = getDatasetFromJsonMultilineRecursive.apply(sparkSession, pathToFiles2);
    final Dataset<Row> plugin3 = getDatasetFromJsonMultilineRecursive.apply(sparkSession, pathToFiles3);

    final Dataset<Row> plug1 = getIdField.apply(plugin1, pathToFiles);
    final Dataset<Row> plug2 = getIdField.apply(plugin2, pathToFiles2);
    final Dataset<Row> plug3 = getIdField.apply(plugin3, pathToFiles3);

    final Dataset<Row> joinedWithName = joinWithPluginIdAndName
        .apply(plug1, pluginIdAndNameDS)
        .union(joinWithPluginIdAndName.apply(plug2, pluginIdAndNameDS))
        .union(joinWithPluginIdAndName.apply(plug3, pluginIdAndNameDS));

    final Dataset<Row> withDates = joinedWithName
        .withColumn("Date", date_format(col("ts").divide(1000).cast(DataTypes.TimestampType), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("Day", to_date(col("Date")))
        .withColumn("Month", month(col("Date")))
        .withColumn("Year", year(col("Date")));

//    final Dataset<Row> testing2 = getDatasetFromJsonMultilineRecursive.apply(sparkSession, args[1])
//        .withColumn("Name", lit(args[1]))
//        .withColumn("Date", date_format(col("ts").divide(1000).cast(DataTypes.TimestampType),
//            "yyyy-MM-dd HH:mm:ss"))
//        .withColumn("Day", to_date(col("Date")))
//        .withColumn("Month", month(col("Date")))
//        .withColumn("Year", year(col("Date")));

    final Dataset<Row> combined = withDates;
//        .union(testing2);
//        .union(fiveNintyAndFiftySevenHundred);

    final Dataset<Row> d = combined
        .withColumn("Wattz", col("pw").cast(String.valueOf(DataType.INT)))
        .groupBy(col("Day"), col("id"), col("name"))
        .agg(
            count(lit(1)).alias("Number of Log Entries"),
            sum("Wattz").alias("Sum_watts_for_day_and_plug_TEMP"),
            max("Wattz").alias("Max watts for day and plug"),
            min("Wattz").alias("Min watts for day and plug"),
            avg("Wattz").alias("Avg watts for day and plug"))
        .sort(col("Day"));

    final Dataset<Row> agg = d
        .withColumn("Real Summed Watts for Day and plug",
            col("Sum_watts_for_day_and_plug_TEMP").divide(col("Number of Log Entries")))
        .drop("Sum_watts_for_day_and_plug_TEMP", "file_name_and_path_TEMP", "file_name_and_parent_dir_TEMP", "id_TEMP");

//    final Dataset<Row> e = combined
//        .withColumn("Wattz", col("pw").cast(String.valueOf(DataType.INT)))
//        .groupBy(col("Month"), col("Year"), col("FormatName"))
//        .agg(
//            count(lit(1)).alias("Number of Log Entries"),
//            sum("Wattz").alias("Summed Watts for Month and plug")
//        ).sort(col("Year"), col("Month"));
//
//    final Dataset<Row> sort = e.withColumn("Real Summed Watts for month and plug",
//        col("Summed Watts for Month and plug").divide(col("Number of Log Entries")));

    //@TODO: Doublecheck these numbers
//    final Dataset<Row> rowDataset = agg.withColumn("Energy Consumption for Day",
//            col("Average Watts an Hour").divide(1000))
//        .withColumn("Energy Cost for Day",
//            col("Energy Consumption for Day").multiply(0.07).multiply(24))
//        .withColumn("Expected Monthly Cost",
//            col("Energy Cost for Day").multiply(30))
//        .withColumn("Watt Hours a day",
//            col("Energy Consumption for Day").multiply(24))
//        .withColumn("Watt Hours a Month",
//            col("Energy Consumption for Day").multiply(24).multiply(30).multiply(1000));

    writerFactory.accept(agg.repartition(1), "daily");
//    writerFactory.accept(sort.repartition(1), "monthly");
//    rowDataset.show();

    sparkSession.close();
  }
}

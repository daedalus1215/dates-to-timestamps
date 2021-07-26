package com.apache.spark.stuff;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.regexp_replace;
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

  /**
   * We have established a pattern, for the source files, to be something like {idOfPlugin}/{idOfPlugin}-log-{#}.json
   * format. This regular expression will help us just keep the {id}.
   * Eg of a file: file:///Users/{userName}/{pathToProject}/src/main/resources/tplink/{idOfPlugin}/{idOfPlugin}-log-{#}.json
   */
  private final static String STRIP_FILE_PATH_FROM_PLUG_ID = "/[A-Z0-9]+-log-([0-9]|([0-9]+[0-9])).json";

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
    final String absolute_path_to_source_directory = args[1];

    // Declare everything
    final GetSparkSession getSparkSession = new GetSparkSession();
    final GetDatasetFromJsonMultilineRecursive getDatasetFromJsonMultilineRecursive = new GetDatasetFromJsonMultilineRecursive();
    final WriterFactory writerFactory = new WriterFactory();

    // Setup
    final SparkSession sparkSession = getSparkSession.get();
    final GetPlugIdAndNameDataset getPlugIdAndNameDataset = new GetPlugIdAndNameDataset(sparkSession);

    final Dataset<Row> pluginIdAndNameDS = getPlugIdAndNameDataset.get();

    final Dataset<Row> rowDataset = getDatasetFromJsonMultilineRecursive.apply(sparkSession, pathToFiles)
        .withColumn("file_name_and_path_TEMP", input_file_name())
        .withColumn("file_name_and_parent_dir_TEMP", regexp_replace(col("file_name_and_path_TEMP"), absolute_path_to_source_directory, ""))
        .withColumn("row_id", regexp_replace(col("file_name_and_parent_dir_TEMP"), STRIP_FILE_PATH_FROM_PLUG_ID, ""));

    writerFactory.accept(rowDataset, "temp");

    final Dataset<Row> joinedWithName = rowDataset.join(pluginIdAndNameDS,
        rowDataset.col("row_id").equalTo(pluginIdAndNameDS.col("id")), "left"
    );

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
        .drop("Sum_watts_for_day_and_plug_TEMP", "file_name_and_path_TEMP", "file_name_and_parent_dir_TEMP");

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

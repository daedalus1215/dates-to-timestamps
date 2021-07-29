package com.apache.spark.stuff.tpEnergyLink;

import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_FILE_NAME_AND_PARENT_DIR_TEMP;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_FILE_NAME_AND_PREFIX_TEMP;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_ID;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_REMOVED_PATH_TO_FILE_TEMP;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_SUM_WATTS_FOR_DAY_AND_PLUG_TEMP;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_SOURCE_ID;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_CREATED_COUNT_OF_RECORDS_A_DAY;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_CREATED_DATE;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_CREATED_SUM_OF_A_DAY;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_SOURCE_AMOUNT;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_SOURCE_DATE;
import static com.apache.spark.stuff.tpEnergyLink.Constants.SOURCE_POWER;
import static com.apache.spark.stuff.tpEnergyLink.Constants.SOURCE_UNIX_TIMESTAMP;
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
import com.apache.spark.stuff.functions.util.GetDatasetFromCsv;
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
   * args[0] = "/Users/{userName}/{pathToProject}/src/main/resources/tplink/{idOfAPlugin}"
   * args[1] = "/Users/{userName}/{pathToProject}/src/main/resources/tplink/{idOfAPlugin}"
   */
  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "d:/hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    final String pathToFiles2 = args[0];
    final String pathToFiles3 = args[1];
    final String pathToRvnLog = args[2];

    // Declare everything
    final GetIdField getIdField = new GetIdField();
    final JoinWithPluginIdAndName joinWithPluginIdAndName = new JoinWithPluginIdAndName();
    final GetDatasetFromCsv getDatasetFromCsv = new GetDatasetFromCsv();

    final GetSparkSession getSparkSession = new GetSparkSession();
    final GetDatasetFromJsonMultilineRecursive getDatasetFromJsonMultilineRecursive = new GetDatasetFromJsonMultilineRecursive();
    final WriterFactory writerFactory = new WriterFactory();
    final JoinWithMinedAsset joinWithMinedAsset = new JoinWithMinedAsset();

    // Setup
    final SparkSession sparkSession = getSparkSession.get();
    final GetPlugIdAndNameDataset getPlugIdAndNameDataset = new GetPlugIdAndNameDataset(sparkSession);

    final Dataset<Row> pluginIdAndNameDS = getPlugIdAndNameDataset.get();

    final Dataset<Row> ravenDS = getDatasetFromCsv.apply(sparkSession, pathToRvnLog)
        .withColumn(RVN_CREATED_DATE, to_date(col(RVN_SOURCE_DATE)))
        .groupBy(col(RVN_CREATED_DATE), col(RVN_SOURCE_ID))
        .agg(
            count(lit(1)).alias(RVN_CREATED_COUNT_OF_RECORDS_A_DAY),
            sum(col(RVN_SOURCE_AMOUNT)).alias(RVN_CREATED_SUM_OF_A_DAY)
        );

//    ravenDS.show();

    final Dataset<Row> plugin2 = getDatasetFromJsonMultilineRecursive.apply(sparkSession, pathToFiles2);
    final Dataset<Row> plugin3 = getDatasetFromJsonMultilineRecursive.apply(sparkSession, pathToFiles3);

    final Dataset<Row> plug2 = getIdField.apply(plugin2, pathToFiles2);
    final Dataset<Row> plug3 = getIdField.apply(plugin3, pathToFiles3);

    final Dataset<Row> joinedWithName = joinWithPluginIdAndName.apply(plug2, pluginIdAndNameDS)
        .union(joinWithPluginIdAndName.apply(plug3, pluginIdAndNameDS));

    final Dataset<Row> withDatesAndAggregations = joinedWithName
        .withColumn("Date", date_format(col(SOURCE_UNIX_TIMESTAMP).divide(1000).cast(DataTypes.TimestampType), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("Day", to_date(col("Date")))
        .withColumn("Month", month(col("Date")))
        .withColumn("Year", year(col("Date")));

    final Dataset<Row> withWatts = withDatesAndAggregations.withColumn("Wattz", col(SOURCE_POWER).cast(String.valueOf(DataType.INT)))
        .groupBy(col("Day"), col("id"), col("name"))
        .agg(
            count(lit(1)).alias("Number of Log Entries"),
            sum("Wattz").alias(CREATED_SUM_WATTS_FOR_DAY_AND_PLUG_TEMP),
            max("Wattz").alias("Max watts for day and plug"),
            min("Wattz").alias("Min watts for day and plug"),
            avg("Wattz").alias("Avg watts for day and plug"))
        .sort(col("Day")
        );

    withWatts.show();
    final Dataset<Row> withAsset = joinWithMinedAsset.apply(withWatts, ravenDS);

    final Dataset<Row> withMathDone = withAsset
        .withColumn("Real Summed Watts for Day and plug", col(CREATED_SUM_WATTS_FOR_DAY_AND_PLUG_TEMP).divide(col("Number of Log Entries")))
        .withColumn("Energy Cost for Day", col("Real Summed Watts for Day and plug").divide(1000).multiply(0.07))
        .withColumn("Watt Hours a day", col("Real Summed Watts for Day and plug").multiply(24));


    final Dataset<Row> cleanedUp = withMathDone.drop(CREATED_SUM_WATTS_FOR_DAY_AND_PLUG_TEMP,
        CREATED_FILE_NAME_AND_PREFIX_TEMP,
        CREATED_FILE_NAME_AND_PARENT_DIR_TEMP,
        CREATED_ID,
        CREATED_REMOVED_PATH_TO_FILE_TEMP);

    writerFactory.accept(cleanedUp.repartition(1), "daily");

    sparkSession.close();
  }
}

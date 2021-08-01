package com.apache.spark.stuff.tpEnergyLink;

import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_DATE;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_DAY;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_FILE_NAME_AND_PARENT_DIR_TEMP;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_FILE_NAME_AND_PREFIX_TEMP;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_ID;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_MONTH;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_REMOVED_PATH_TO_FILE_TEMP;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_SUM_WATTS_FOR_DAY_AND_PLUG_TEMP;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_UNIQUE_ID;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_WATTS;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_YEAR;
import static com.apache.spark.stuff.tpEnergyLink.Constants.PRICE_CREATED_DATE;
import static com.apache.spark.stuff.tpEnergyLink.Constants.PRICE_SOURCE_DATE;
import static com.apache.spark.stuff.tpEnergyLink.Constants.PRICE_SOURCE_PRICE;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_CREATED_COUNT_OF_RECORDS_A_DAY;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_CREATED_DATE;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_CREATED_SUM_OF_A_DAY;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_CREATED_UNIQUE_ID;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_SOURCE_AMOUNT;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_SOURCE_DATE;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_SOURCE_ID;
import static com.apache.spark.stuff.tpEnergyLink.Constants.SOURCE_POWER;
import static com.apache.spark.stuff.tpEnergyLink.Constants.SOURCE_UNIX_TIMESTAMP;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
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

public class TpEnergyLinkDayToDayApp {

  @SuppressWarnings("resource")
  /*
   * args[0] = "/Users/{userName}/{pathToProject}/src/main/resources/tplink/{idOfAPlugin}".
   * args[1] = "/Users/{userName}/{pathToProject}/src/main/resources/tplink/{idOfAPlugin}".
   */
  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "d:/hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    final String pathToFiles2 = args[0];
    final String pathToFiles3 = args[1];
    final String pathToRvnLog = args[2];
    final String pathToFiles4 = args[3];

    // Declare everything
    final GetIdField getIdField = new GetIdField();
    final JoinWithPluginIdAndName joinWithPluginIdAndName = new JoinWithPluginIdAndName();
    final GetDatasetFromCsv getDatasetFromCsv = new GetDatasetFromCsv();
    final JoinAssetWithPrice joinAssetWithPrice = new JoinAssetWithPrice();

    final GetSparkSession getSparkSession = new GetSparkSession();
    final GetDatasetFromJsonMultilineRecursive getDatasetFromJsonMultilineRecursive = new GetDatasetFromJsonMultilineRecursive();
    final WriterFactory writerFactory = new WriterFactory();
    final JoinWithMinedAsset joinWithMinedAsset = new JoinWithMinedAsset();

    // Setup
    final SparkSession sparkSession = getSparkSession.get();
    final GetPlugIdAndNameDataset getPlugIdAndNameDataset = new GetPlugIdAndNameDataset(sparkSession);

    // Load the files
    final Dataset<Row> pluginIdAndNameDS = getPlugIdAndNameDataset.get();
    final Dataset<Row> ravenDS = getDatasetFromCsv.apply(sparkSession, pathToRvnLog)
        .withColumn(RVN_CREATED_DATE, to_date(col(RVN_SOURCE_DATE)))
        .withColumn(RVN_CREATED_UNIQUE_ID, concat(col(RVN_CREATED_DATE), lit('_'), col(RVN_SOURCE_ID)))
        .groupBy(col(RVN_CREATED_DATE), col(RVN_SOURCE_ID), col(RVN_CREATED_UNIQUE_ID))
        .agg(
            count(lit(1)).alias(RVN_CREATED_COUNT_OF_RECORDS_A_DAY),
            sum(col(RVN_SOURCE_AMOUNT)).alias(RVN_CREATED_SUM_OF_A_DAY))
        .sort(col(RVN_CREATED_DATE));
    final Dataset<Row> plugin2 = getDatasetFromJsonMultilineRecursive.apply(sparkSession, pathToFiles2);
    final Dataset<Row> plugin3 = getDatasetFromJsonMultilineRecursive.apply(sparkSession, pathToFiles3);
    final Dataset<Row> priceOfRvn = getDatasetFromCsv.apply(sparkSession, pathToFiles4);

    // Process files
    final Dataset<Row> plug2 = getIdField.apply(plugin2, pathToFiles2);
    final Dataset<Row> plug3 = getIdField.apply(plugin3, pathToFiles3);

    final Dataset<Row> union = plug2.union(plug3);

    final Dataset<Row> withDatesAndAggregations = union
        .withColumn(CREATED_DATE, date_format(col(SOURCE_UNIX_TIMESTAMP)
            .divide(1000).cast(DataTypes.TimestampType), "yyyy-MM-dd HH:mm:ss"))
        .withColumn(CREATED_DAY, to_date(col(CREATED_DATE)))
        .withColumn(CREATED_MONTH, month(col(CREATED_DATE)))
        .withColumn(CREATED_YEAR, year(col(CREATED_DATE)))
        .withColumn(CREATED_UNIQUE_ID, concat(col(CREATED_DAY), lit('_'), col(CREATED_ID)));

    final Dataset<Row> withWatts = withDatesAndAggregations
        .withColumn(CREATED_WATTS, col(SOURCE_POWER).cast(String.valueOf(DataType.INT)))
        .groupBy(col(CREATED_DAY), col(CREATED_ID), col(CREATED_UNIQUE_ID))
        .agg(
            count(lit(1)).alias("Number of Log Entries"),
            sum(CREATED_WATTS).alias(CREATED_SUM_WATTS_FOR_DAY_AND_PLUG_TEMP),
            max(CREATED_WATTS).alias("Max watts for day and plug"),
            min(CREATED_WATTS).alias("Min watts for day and plug"),
            avg(CREATED_WATTS).alias("Avg watts for day and plug"))
        .sort(col(CREATED_DAY)
        );

    final Dataset<Row> assetWithDate = priceOfRvn.withColumn(PRICE_CREATED_DATE, to_date(col(PRICE_SOURCE_DATE)));
    final Dataset<Row> withAsset = joinWithMinedAsset.apply(withWatts, joinAssetWithPrice.apply(ravenDS, assetWithDate));
    final Dataset<Row> d = withAsset.withColumn("PRICE_ASSET_TOTAL_VALUE", col(PRICE_SOURCE_PRICE).multiply(col(RVN_CREATED_SUM_OF_A_DAY)));

    final Dataset<Row> withMathDone = d
        .withColumn("Real Summed Watts for Day and plug", col(CREATED_SUM_WATTS_FOR_DAY_AND_PLUG_TEMP).divide(col("Number of Log Entries")))
        .withColumn("Energy Cost for Day", col("Real Summed Watts for Day and plug").divide(1000).multiply(0.07))
        .withColumn("Watt Hours a day", col("Real Summed Watts for Day and plug").multiply(24));

    final Dataset<Row> joinedWithName = joinWithPluginIdAndName.apply(withMathDone, pluginIdAndNameDS);

    final Dataset<Row> cleanedUp = joinedWithName.drop(CREATED_SUM_WATTS_FOR_DAY_AND_PLUG_TEMP,
        CREATED_FILE_NAME_AND_PREFIX_TEMP,
        CREATED_FILE_NAME_AND_PARENT_DIR_TEMP,
        CREATED_ID,
        CREATED_REMOVED_PATH_TO_FILE_TEMP,
        PRICE_SOURCE_DATE,
        "market_value",
        "total_volume",
        "market_cap",
        RVN_CREATED_UNIQUE_ID);

    writerFactory.accept(cleanedUp.repartition(1), "daily");

    sparkSession.close();
  }
}

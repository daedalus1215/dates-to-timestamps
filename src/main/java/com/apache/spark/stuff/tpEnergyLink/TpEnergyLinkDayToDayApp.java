package com.apache.spark.stuff.tpEnergyLink;

import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_DATE;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_DAY;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_FILE_NAME_AND_PREFIX_TEMP;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_ID;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_MONTH;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_REMOVED_PATH_TO_FILE_TEMP;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_SUM_WATTS_FOR_DAY_AND_PLUG_TEMP;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_UNIQUE_ID;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_WATTS;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_YEAR;
import static com.apache.spark.stuff.tpEnergyLink.Constants.PRICE_SOURCE_PRICE;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_CREATED_SUM_OF_A_DAY;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_SOURCE_ADDRESS;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_SOURCE_AMOUNT_ASSET;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_SOURCE_AMOUNT_RIG_ID;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_SOURCE_AMOUNT_RVN;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_SOURCE_AMOUNT_TRANSACTION_ID;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_SOURCE_CONFIRMED;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_SOURCE_DATE;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_SOURCE_LABEL;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_SOURCE_TYPE;
import static com.apache.spark.stuff.tpEnergyLink.Constants.SOURCE_POWER;
import static com.apache.spark.stuff.tpEnergyLink.Constants.SOURCE_UNIX_TIMESTAMP;
import static java.util.Arrays.asList;
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

import com.apache.spark.stuff.SparkPipeline;
import com.apache.spark.stuff.functions.GetSparkSession;
import com.apache.spark.stuff.functions.readers.GetDatasetFromCsv;
import com.apache.spark.stuff.functions.readers.GetDatasetFromJsonMultilineRecursive;
import com.apache.spark.stuff.functions.writers.CsvWriter;
import com.apache.spark.stuff.functions.writers.WriterInterface;
import com.apache.spark.stuff.tpEnergyLink.rvn.mining.transformers.CreatedSumOfDayAggregation;
import com.apache.spark.stuff.tpEnergyLink.rvn.mining.transformers.CreatedUniqueId;
import com.apache.spark.stuff.tpEnergyLink.rvn.mining.transformers.MiningCreatedDate;
import com.apache.spark.stuff.tpEnergyLink.rvn.prices.transformers.PricesCreatedDate;
import com.apache.spark.stuff.tpEnergyLink.transformers.PluginIdFromSourceFile;
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
//    final PluginIdFromSourceFile pluginIdFromSourceFile = new PluginIdFromSourceFile(absolutePathToSourceDirectory);
    final JoinWithPluginIdAndName joinWithPluginIdAndName = new JoinWithPluginIdAndName();
//    final GetDatasetFromCsv getDatasetFromCsv = new GetDatasetFromCsv();
    final JoinAssetWithPrice joinAssetWithPrice = new JoinAssetWithPrice();

    final GetSparkSession getSparkSession = new GetSparkSession();
//    final GetDatasetFromJsonMultilineRecursive getDatasetFromJsonMultilineRecursive = new GetDatasetFromJsonMultilineRecursive();
    final WriterInterface writer = new CsvWriter();
    final JoinWithMinedAsset joinWithMinedAsset = new JoinWithMinedAsset();

    // Setup
    final SparkSession sparkSession = getSparkSession.get();
    final PlugIdAndNameReader pluginIdAndNameDS = new PlugIdAndNameReader(sparkSession);

    // Energy
    final Dataset<Row> withWatts = (new SparkPipeline(sparkSession)
        .withReader(new GetDatasetFromJsonMultilineRecursive(pathToFiles2))
        .withTransformers(asList(new PluginIdFromSourceFile(pathToFiles2)))
        .withRemovableColumns(CREATED_FILE_NAME_AND_PREFIX_TEMP, CREATED_REMOVED_PATH_TO_FILE_TEMP)
        .build()
    ).union(new SparkPipeline(sparkSession)
        .withReader(new GetDatasetFromJsonMultilineRecursive(pathToFiles3))
        .withTransformers(asList(new PluginIdFromSourceFile(pathToFiles3)))
        .withRemovableColumns(CREATED_FILE_NAME_AND_PREFIX_TEMP, CREATED_REMOVED_PATH_TO_FILE_TEMP)
        .build()).withColumn(CREATED_DATE, date_format(col(SOURCE_UNIX_TIMESTAMP)
        .divide(1000).cast(DataTypes.TimestampType), "yyyy-MM-dd HH:mm:ss"))
        .withColumn(CREATED_DAY, to_date(col(CREATED_DATE)))
        .withColumn(CREATED_MONTH, month(col(CREATED_DATE)))
        .withColumn(CREATED_YEAR, year(col(CREATED_DATE)))
        .withColumn(CREATED_UNIQUE_ID, concat(col(CREATED_DAY), lit('_'), col(CREATED_ID)))
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

    // Raven
    final Dataset<Row> assetWithDate = new SparkPipeline(sparkSession)
        .withReader(new GetDatasetFromCsv(pathToFiles4))
        .withTransformers(asList(new PricesCreatedDate()))
        .build();

    final Dataset<Row> ravenMiningDS = new SparkPipeline(sparkSession)
        .withReader(new GetDatasetFromCsv(pathToRvnLog))
        .withRemovableColumns(RVN_SOURCE_CONFIRMED, RVN_SOURCE_DATE, RVN_SOURCE_TYPE, RVN_SOURCE_LABEL, RVN_SOURCE_ADDRESS,
            RVN_SOURCE_AMOUNT_RVN, RVN_SOURCE_AMOUNT_ASSET, RVN_SOURCE_AMOUNT_TRANSACTION_ID, RVN_SOURCE_AMOUNT_RIG_ID)
        .withTransformers(asList(new MiningCreatedDate(), new CreatedUniqueId(), new CreatedSumOfDayAggregation()))
        .build();

    //@TODO: DONE ABOVE
    final Dataset<Row> withAsset = joinWithMinedAsset.apply(withWatts, joinAssetWithPrice.apply(ravenMiningDS, assetWithDate));
    final Dataset<Row> withMathDone = withAsset.withColumn("PRICE_ASSET_TOTAL_VALUE", col(PRICE_SOURCE_PRICE).multiply(col(RVN_CREATED_SUM_OF_A_DAY)))
        .withColumn("Real Summed Watts for Day and plug", col(CREATED_SUM_WATTS_FOR_DAY_AND_PLUG_TEMP).divide(col("Number of Log Entries")))
        .withColumn("Energy Cost for Day", col("Real Summed Watts for Day and plug").divide(1000).multiply(0.07).multiply(24))
        .withColumn("Watt Hours a day", col("Real Summed Watts for Day and plug").multiply(24));

    final Dataset<Row> joinedWithName = joinWithPluginIdAndName.apply(withMathDone, pluginIdAndNameDS.get());

    writer.accept(joinedWithName.repartition(1), "daily");

    sparkSession.close();
  }
}

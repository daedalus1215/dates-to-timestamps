package com.apache.spark.stuff.tpEnergyLink.rvn.mining.transformers;

import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_CREATED_AMOUNT;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_CREATED_AMOUNT_ASSET;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_CREATED_COUNT_OF_RECORDS_A_DAY;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_CREATED_DATE;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_CREATED_SUM_OF_A_DAY;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_CREATED_UNIQUE_ID;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_SOURCE_AMOUNT_ASSET;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_SOURCE_AMOUNT_RIG_ID;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_SOURCE_AMOUNT_RVN;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;

import com.apache.spark.stuff.tpEnergyLink.transformers.TransformerInterface;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CreatedSumOfDayAggregation implements TransformerInterface {

  @Override
  public Dataset<Row> apply(Dataset<Row> ds) {
    return ds
        .withColumn(RVN_CREATED_AMOUNT, col(RVN_SOURCE_AMOUNT_RVN))
        .withColumn(RVN_CREATED_AMOUNT_ASSET, col(RVN_SOURCE_AMOUNT_ASSET))
        .groupBy(col(RVN_CREATED_DATE), col(RVN_SOURCE_AMOUNT_RIG_ID), col(RVN_CREATED_UNIQUE_ID))
        .agg(
            count(lit(1)).alias(RVN_CREATED_COUNT_OF_RECORDS_A_DAY),
            sum(col(RVN_SOURCE_AMOUNT_RVN)).alias(RVN_CREATED_SUM_OF_A_DAY)
        ).sort(col(RVN_CREATED_DATE));
  }
}

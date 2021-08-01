package com.apache.spark.stuff.tpEnergyLink;

import static com.apache.spark.stuff.tpEnergyLink.Constants.LEFT_JOIN;
import static com.apache.spark.stuff.tpEnergyLink.Constants.PRICE_CREATED_DATE;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_CREATED_DATE;

import java.util.function.BiFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JoinAssetWithPrice implements BiFunction<Dataset<Row>, Dataset<Row>, Dataset<Row>> {

  @Override
  public Dataset<Row> apply(Dataset<Row> targetDS, Dataset<Row> assetDS) {
    return targetDS.join(assetDS,
        targetDS.col(RVN_CREATED_DATE).equalTo(assetDS.col(PRICE_CREATED_DATE)),
        LEFT_JOIN);
  }
}

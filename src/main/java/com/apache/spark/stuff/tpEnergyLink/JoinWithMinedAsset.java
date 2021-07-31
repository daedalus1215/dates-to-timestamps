package com.apache.spark.stuff.tpEnergyLink;

import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_UNIQUE_ID;
import static com.apache.spark.stuff.tpEnergyLink.Constants.OUTER_JOIN;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_CREATED_UNIQUE_ID;

import java.util.function.BiFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JoinWithMinedAsset implements BiFunction<Dataset<Row>, Dataset<Row>, Dataset<Row>> {

  @Override
  public Dataset<Row> apply(Dataset<Row> targetDS, Dataset<Row> assetDS) {
    return targetDS.join(assetDS,
        targetDS.col(CREATED_UNIQUE_ID).equalTo(assetDS.col(RVN_CREATED_UNIQUE_ID)),
        OUTER_JOIN);
  }
}

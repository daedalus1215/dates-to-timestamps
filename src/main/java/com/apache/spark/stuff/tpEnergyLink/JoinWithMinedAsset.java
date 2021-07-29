package com.apache.spark.stuff.tpEnergyLink;

import static com.apache.spark.stuff.tpEnergyLink.Constants.LEFT_JOIN;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_CREATED_DATE;

import java.util.function.BiFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JoinWithMinedAsset implements BiFunction<Dataset<Row>, Dataset<Row>, Dataset<Row>> {

  @Override
  public Dataset<Row> apply(Dataset<Row> targetDS, Dataset<Row> assetDS) {
    targetDS.show();

    assetDS.show();
    //@TODO: Left off here, need to clean this up
    final Dataset<Row> day = targetDS.join(assetDS, targetDS.col("Day").equalTo(assetDS.col(RVN_CREATED_DATE)),
        LEFT_JOIN);
    day.show();
    return day;
  }
}

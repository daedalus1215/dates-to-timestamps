package com.apache.spark.stuff.tpEnergyLink;

import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_ID;

import java.util.function.BiFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JoinWithPluginIdAndName implements BiFunction<Dataset<Row>, Dataset<Row>, Dataset<Row>> {

  @Override
  public Dataset<Row> apply(Dataset<Row> targetDS, Dataset<Row> pluginIdAndNameDS) {
    return targetDS.join(pluginIdAndNameDS, targetDS.col(CREATED_ID)
            .equalTo(pluginIdAndNameDS.col("id")),
        "left");
  }
}

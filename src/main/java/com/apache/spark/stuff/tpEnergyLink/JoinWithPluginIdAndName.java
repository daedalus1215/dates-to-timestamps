package com.apache.spark.stuff.tpEnergyLink;

import java.util.function.BiFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JoinWithPluginIdAndName implements BiFunction<Dataset<Row>, Dataset<Row>, Dataset<Row>> {

  @Override
  public Dataset<Row> apply(Dataset<Row> targetDS, Dataset<Row> pluginIdAndNameDS) {
    return targetDS.join(pluginIdAndNameDS, targetDS.col("id_TEMP")
            .equalTo(pluginIdAndNameDS.col("id")),
        "left");
  }
}

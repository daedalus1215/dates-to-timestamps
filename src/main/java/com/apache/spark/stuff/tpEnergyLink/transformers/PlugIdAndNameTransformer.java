package com.apache.spark.stuff.tpEnergyLink.transformers;

import java.util.List;
import java.util.function.Supplier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;

public class PlugIdAndNameTransformer implements Supplier<Dataset<Row>> {

  private final SparkSession sparkSession;
  private final List<Row> rows = new GetPlugIdAndNameRows().get();

  /**
   * Can make this far more dynamic, but no need for the moment.
   * @param sparkSession
   */
  public PlugIdAndNameTransformer(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  @Override
  public Dataset<Row> get() {
    return sparkSession.createDataFrame(rows, new StructType()
        .add("id", DataTypes.StringType, true, Metadata.empty())
        .add("name", DataTypes.StringType, true, Metadata.empty()));
  }
}

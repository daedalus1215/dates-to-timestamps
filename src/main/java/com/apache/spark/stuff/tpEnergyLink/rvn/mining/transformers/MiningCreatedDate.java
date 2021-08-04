package com.apache.spark.stuff.tpEnergyLink.rvn.mining.transformers;

import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_CREATED_DATE;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_SOURCE_DATE;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_date;

import com.apache.spark.stuff.tpEnergyLink.transformers.TransformerInterface;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class MiningCreatedDate implements TransformerInterface {

  @Override
  public Dataset<Row> apply(Dataset<Row> ds) {
    return ds.withColumn(RVN_CREATED_DATE, to_date(col(RVN_SOURCE_DATE)));
  }
}

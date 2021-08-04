package com.apache.spark.stuff.tpEnergyLink.rvn.mining.transformers;

import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_CREATED_DATE;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_CREATED_UNIQUE_ID;
import static com.apache.spark.stuff.tpEnergyLink.Constants.RVN_SOURCE_AMOUNT_RIG_ID;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import com.apache.spark.stuff.tpEnergyLink.transformers.TransformerInterface;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CreatedUniqueId implements TransformerInterface {

  @Override
  public Dataset<Row> apply(Dataset<Row> ds) {
    return ds.withColumn(RVN_CREATED_UNIQUE_ID,
        concat(col(RVN_CREATED_DATE), lit('_'), col(RVN_SOURCE_AMOUNT_RIG_ID)));
  }
}

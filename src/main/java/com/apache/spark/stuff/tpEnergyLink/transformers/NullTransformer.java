package com.apache.spark.stuff.tpEnergyLink.transformers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class NullTransformer implements TransformerInterface{

  @Override
  public Dataset<Row> apply(Dataset<Row> ds) {
    return ds;
  }
}

package com.apache.spark.stuff.tpEnergyLink.transformers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface TransformerInterface {
  Dataset<Row> apply(Dataset<Row> ds);
}

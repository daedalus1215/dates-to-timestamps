package com.apache.spark.stuff.functions.readers;

import java.util.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface ReaderInterface extends Function<SparkSession, Dataset<Row>> {

  Dataset<Row> apply(SparkSession sparkSession);
}

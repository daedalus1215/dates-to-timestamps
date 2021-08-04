package com.apache.spark.stuff.functions.writers;

import java.util.function.BiConsumer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface WriterInterface extends BiConsumer<Dataset<Row>, String> {
}

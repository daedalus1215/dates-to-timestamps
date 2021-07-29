package com.apache.spark.stuff.tpEnergyLink;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.regexp_replace;

import java.util.function.BiFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class GetIdField implements BiFunction<Dataset<Row>, String, Dataset<Row>> {

  /**
   * We have established a pattern, for the source files, to be something like {idOfPlugin}/{idOfPlugin}-log-{#}.json
   * format. This regular expression will help us just keep the {id}.
   * Eg of a file: file:///Users/{userName}/{pathToProject}/src/main/resources/tplink/{idOfPlugin}/{idOfPlugin}-log-{#}.json
   */
  private final static String STRIP_FILE_PATH_FROM_PLUG_ID = "/[A-Z0-9]+-log-([0-9]|([0-9]+[0-9])).json";

  public Dataset<Row> apply(Dataset<Row> ds, String absolutePathToSourceDirectory) {
    return ds.withColumn("file_name_and_path_TEMP", input_file_name())
        .withColumn("file_name_and_parent_dir_TEMP", regexp_replace(col("file_name_and_path_TEMP"), absolutePathToSourceDirectory, ""))
        .withColumn("id_TEMP", regexp_replace(col("file_name_and_parent_dir_TEMP"), STRIP_FILE_PATH_FROM_PLUG_ID, ""));
  }
}

package com.apache.spark.stuff.tpEnergyLink;

import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_FILE_NAME_AND_PARENT_DIR_TEMP;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_FILE_NAME_AND_PREFIX_TEMP;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_REMOVED_PATH_TO_FILE_TEMP;
import static com.apache.spark.stuff.tpEnergyLink.Constants.CREATED_ID;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.regexp_replace;

import java.util.function.BiFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class GetIdField implements BiFunction<Dataset<Row>, String, Dataset<Row>> {

  /**
   * We have established a pattern, for the source files, to be something like {idOfPlugin}/{idOfPlugin}-log-{#}.json
   * format. This regular expression will help us just keep the {idOfPlugin}.
   * Eg of a file: file:///Users/{userName}/{pathToProject}/src/main/resources/tplink/{idOfPlugin}/{idOfPlugin}-log-{#}.json
   */
  private final static String STRIP_FILE_PATH_FROM_PLUG_ID = "-log-([0-9]|([0-9]+[0-9])).json";
  private final static String EMPTY_STRING = "";
  private final static String MAC_ROOT_FILE_PREFIX = "file:///";

  /**
   *
   * @param ds the dataset we want to work off of
   * @param absolutePathToSourceDirectory: /Users/{username}/{pathToProject}/dates-to-timestamps/src/main/resources/tplink/{folderWhereAllTheSourceFilesLive}
   * @return
   */
  public Dataset<Row> apply(Dataset<Row> ds, String absolutePathToSourceDirectory) {
    return ds.withColumn(CREATED_FILE_NAME_AND_PREFIX_TEMP, input_file_name())
        .withColumn(CREATED_REMOVED_PATH_TO_FILE_TEMP, regexp_replace(col(CREATED_FILE_NAME_AND_PREFIX_TEMP), absolutePathToSourceDirectory, EMPTY_STRING))
        .withColumn(CREATED_FILE_NAME_AND_PARENT_DIR_TEMP, regexp_replace(col(CREATED_REMOVED_PATH_TO_FILE_TEMP), MAC_ROOT_FILE_PREFIX, EMPTY_STRING))
        .withColumn(CREATED_ID, regexp_replace(col(CREATED_FILE_NAME_AND_PARENT_DIR_TEMP), STRIP_FILE_PATH_FROM_PLUG_ID, EMPTY_STRING));
  }
}

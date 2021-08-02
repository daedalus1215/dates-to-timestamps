package com.apache.spark.stuff;

import com.apache.spark.stuff.functions.util.GetDatasetFromCsv;
import com.apache.spark.stuff.functions.util.GetSparkSession;
import com.apache.spark.stuff.functions.util.WriterFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class UnionizeFilesApp {

  @SuppressWarnings("resource")
  /*
   * args[0] = "/Users/{userName}/{pathToProject}/src/main/resources/tplink/{idOfAPlugin}".
   * args[1] = "/Users/{userName}/{pathToProject}/src/main/resources/tplink/{idOfAPlugin}".
   */
  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "d:/hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    final String pathToFiles = args[0];
    final String pathToFiles2 = args[1];

    // Declare everything
    final GetDatasetFromCsv getDatasetFromCsv = new GetDatasetFromCsv();

    final GetSparkSession getSparkSession = new GetSparkSession();
    final WriterFactory writerFactory = new WriterFactory();

    // Setup
    final SparkSession sparkSession = getSparkSession.get();
    // Load the files
    // Process files
    final Dataset<Row> unionized = getDatasetFromCsv.apply(sparkSession, pathToFiles)
        .union(getDatasetFromCsv.apply(sparkSession, pathToFiles2));

    writerFactory.accept(unionized.repartition(1), "temp");

    sparkSession.close();
  }
}

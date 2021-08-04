package com.apache.spark.stuff;

import com.apache.spark.stuff.functions.readers.ReaderInterface;
import com.apache.spark.stuff.tpEnergyLink.transformers.TransformerInterface;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkPipeline {

  private final SparkSession sparkSession;
  private String[] removableColumns;
  private TransformerInterface[] transformers;
  private ReaderInterface reader;

  public SparkPipeline(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  public SparkPipeline withRemovableColumns(String... sourceColumns) {
    this.removableColumns = sourceColumns;
    return this;
  }

  public SparkPipeline withTransformers(List<TransformerInterface> transformers) {
    this.transformers = transformers.toArray(new TransformerInterface[transformers.size()]);
    return this;
  }

  public SparkPipeline withReader(ReaderInterface reader) {
    this.reader = reader;
    return this;
  }

  public Dataset<Row> build() {
    final Dataset<Row> rowDataset = buildDatasetWithTransformers(reader.apply(sparkSession));
    if (removableColumns != null && removableColumns.length > 0) {
      return rowDataset.drop(removableColumns);
    }
    return rowDataset;
  }

  private Dataset<Row> buildDatasetWithTransformers(Dataset<Row> dataset) {
    for (TransformerInterface transformer : transformers) {
      dataset = transformer.apply(dataset);
    }
    return dataset;
  }

}

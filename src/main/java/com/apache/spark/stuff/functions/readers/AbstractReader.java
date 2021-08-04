package com.apache.spark.stuff.functions.readers;

public abstract class AbstractReader implements ReaderInterface {

  protected final String sourceFileNameAndPath;

  public AbstractReader(String sourceFileNameAndPath) {
    this.sourceFileNameAndPath = sourceFileNameAndPath;
  }
}

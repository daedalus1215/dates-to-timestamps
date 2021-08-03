package com.apache.spark.stuff.tpEnergyLink;

public class Constants {
  // Join
  public static String LEFT_JOIN = "left";
  public static String OUTER_JOIN = "outer";

  // Energy Source Files
  public static String SOURCE_POWER = "pw";
  public static String SOURCE_UNIX_TIMESTAMP = "ts";

  public static String CREATED_ID = "id_TEMP";
  public static String CREATED_REMOVED_PATH_TO_FILE_TEMP = "REMOVED_PATH_TO_FILE_TEMP";
  public static String CREATED_FILE_NAME_AND_PREFIX_TEMP = "FILE_NAME_AND_PREFIX_TEMP";
  public static String CREATED_FILE_NAME_AND_PARENT_DIR_TEMP = "FILE_NAME_AND_PARENT_DIR_TEMP";
  public static String CREATED_SUM_WATTS_FOR_DAY_AND_PLUG_TEMP = "CREATED_SUM_WATTS_FOR_DAY_AND_PLUG_TEMP";
  public static String CREATED_UNIQUE_ID = "CREATED_UNIQUE_ID";
  public static String CREATED_DAY = "CREATED_DAY";
  public static String CREATED_DATE = "CREATED_DATE";
  public static String CREATED_MONTH = "CREATED_MONTH";
  public static String CREATED_YEAR = "CREATED_YEAR";
  public static String CREATED_WATTS = "CREATED_WATTS";

  // Plugin File

  // Assets
  public static String RVN_SOURCE_ID = "RIG_ID";
  public static String RVN_SOURCE_DATE = "Date";
  public static String RVN_SOURCE_AMOUNT = "Amount (RVN)";
  public static String RVN_CREATED_DATE = "RVN_CREATED_DATE";
  public static String RVN_CREATED_COUNT_OF_RECORDS_A_DAY = "RVN_CREATED_COUNT_OF_RECORDS_A_DAY_FOR_RVN";
  public static String RVN_CREATED_SUM_OF_A_DAY = "RVN_CREATED_SUM_OF_RVN_A_DAY";
  public static String RVN_CREATED_UNIQUE_ID = "RVN_CREATED_UNIQUE_ID";

  public static String A2_SOURCE_DATE = "Date time";
  public static String A2_SOURCE_LOCAL_DATE_TIME = "Local date time";
  public static String A2_SOURCE_PURPOSE = "Purpose";
  public static String A2_SOURCE_AMOUNT_BTC = "Amount (BTC)";
  public static String A2_SOURCE_EXCHANGE_RATE = "Exchange rate";
  public static String A2_SOURCE_AMOUNT_USD = "Amount (USD)";


  // Price
  public static String PRICE_SOURCE_DATE = "snapped_at";
  public static String PRICE_SOURCE_PRICE = "price";
  public static String PRICE_CREATED_DATE = "PRICE_CREATED_DATE";
}

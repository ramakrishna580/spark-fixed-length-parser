package com.connected

import com.connected.parser.SingleFileParser
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SampleParserMain {
  def main(args: Array[String]): Unit = {

    val hdfsFilePath = args(0)
    val appConfig: Config = ConfigFactory.load

    val spark = SparkSession.builder
      .appName("Parser").master("local[2]")
      .getOrCreate()

    val dataframe = SingleFileParser.convertFixedFileToDf(spark, appConfig, hdfsFilePath)
    dataframe.show(false)

    dataframe.select("Account_No", "Credit_amount", "Debit_amount", "Post_Date")
      .where(
        to_date(unix_timestamp(col("Post_Date"), "MM/dd/yyyy")
          .cast("TIMESTAMP")
        )
          .leq("2012-11-30")
      ).show(false)
  }
}

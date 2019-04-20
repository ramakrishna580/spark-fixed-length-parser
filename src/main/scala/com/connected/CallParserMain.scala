package com.connected

import com.connected.parser.SingleFileParser
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CallParserMain {
  def main(args: Array[String]): Unit = {

    val hdfsFilePath = args(0)
    val appConfig: Config = ConfigFactory.load

    val spark = SparkSession.builder
      .appName("Parser").master("local[2]")
      .getOrCreate()

    val dataframe = SingleFileParser.convertFixedFileToDf(spark, appConfig, hdfsFilePath)

  }
}

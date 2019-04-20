package com.connected.parser

import com.connected.configparser.{ConfigObject, ConfigParser}
import com.typesafe.config.Config
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

object SingleFileParser {

  def convertFixedFileToDf(spark: SparkSession, appConfig: Config, hdfsFilePath: String): DataFrame = {

    val confDataArr = ConfigParser.parseConfig(appConfig)
    val confObj = ConfigObject(confDataArr)
    val selectSql = ConfigObject.convertConfigToSql(confObj)

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val rawFixedFileDf = spark.read.text(hdfsFilePath)

    getDataFrame(spark, rawFixedFileDf, selectSql)
  }

  def getDataFrame(spark: SparkSession, rawDataframe: DataFrame, selectSql: String): DataFrame = {

    rawDataframe.select(col("value").as("SingleFileParserValue"))
      .createOrReplaceTempView("single_file_parser_table_temp")
    spark.sql(s"SELECT $selectSql FROM single_file_parser_table_temp")
  }

}

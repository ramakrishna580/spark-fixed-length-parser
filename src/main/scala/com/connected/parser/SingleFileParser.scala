package com.connected.parser

import java.util

import com.connected.common.ApplicationVariables
import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

object SingleFileParser {

  def main(args: Array[String]): Unit = {

    val appConfig: Config = ConfigFactory.load
    val filePath = args(0)

    val noOfColumns = appConfig.getConfig("single-file-config").entrySet().size

    val confDataIterator1 = appConfig
      .getConfig("single-file-config")
      .entrySet()
      .toArray[util.Map.Entry[String, ConfigValue]](new Array[util.Map.Entry[String, ConfigValue]](noOfColumns))

    val d: Array[Array[String]] = confDataIterator1.map(
      data =>
        Array(
          data.getKey,
          data.getValue.unwrapped().asInstanceOf[util.ArrayList[Int]].get(0).toString,
          data.getValue.unwrapped().asInstanceOf[util.ArrayList[Int]].get(1).toString)
    )

    val confData = ParserConfig(d)
    val selectSql = ParserConfig.convertConfigToSql(confData)

    println(selectSql)

    convertFixedFileToDf(ApplicationVariables.spark, filePath, selectSql).show(false)
  }

  def convertFixedFileToDf(spark: SparkSession, hdfsFilePath: String, selectSql: String): DataFrame = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    spark.read.text(hdfsFilePath)
      .select(col("value").as("SingleFileParserValue"))
      .createOrReplaceTempView("single_file_parser_table_temp")
    spark.sql(s"SELECT $selectSql FROM single_file_parser_table_temp")
  }
}

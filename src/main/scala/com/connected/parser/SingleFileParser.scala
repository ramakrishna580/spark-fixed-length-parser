package com.connected.parser

import java.util
import java.util.Set

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}

object SingleFileParser {

  def main(args: Array[String]): Unit = {

    val appConfig: Config = ConfigFactory.load
    val filePath = args(0)

    val noOfColumns = appConfig.getConfig("single-file-config").entrySet().size
    val confDataIterator1 = appConfig.getConfig("single-file-config").entrySet().toArray[util.Map.Entry[String, ConfigValue]](new Array[util.Map.Entry[String, ConfigValue]](noOfColumns))
    val d: Array[Array[String]] = confDataIterator1.map( data => Array(data.getKey,data.getValue.unwrapped().asInstanceOf[util.ArrayList[Int]].get(0).toString,data.getValue.unwrapped().asInstanceOf[util.ArrayList[Int]].get(1).toString))

    val confData = ParserConfig(d)
    val selectSql = ParserConfig.convertConfigToSql(confData)

    println(selectSql)
  }

}

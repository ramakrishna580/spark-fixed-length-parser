package com.connected.configparser

import java.util
import com.typesafe.config.{Config, ConfigValue}

object ConfigParser {

  def parseConfig(parserConfig: Config): Array[Array[String]] = {

    val noOfColumns = parserConfig.getConfig("single-file-config").entrySet().size

    val confDataIterator = parserConfig
      .getConfig("single-file-config")
      .entrySet()
      .toArray[util.Map.Entry[String, ConfigValue]](new Array[util.Map.Entry[String, ConfigValue]](noOfColumns))

    confDataIterator.map(
      data =>
        Array(
          data.getKey,
          data.getValue.unwrapped().asInstanceOf[util.ArrayList[Int]].get(0).toString,
          data.getValue.unwrapped().asInstanceOf[util.ArrayList[Int]].get(1).toString)
    )
  }
}

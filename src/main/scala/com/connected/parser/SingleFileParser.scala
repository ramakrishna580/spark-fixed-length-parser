package com.connected.parser

import java.util

import com.typesafe.config.{Config, ConfigFactory}

object SingleFileParser {

  def main(args: Array[String]): Unit = {

    val appConfig: Config = ConfigFactory.load
    val filePath = args(0)


    val confDataIterator = appConfig.getConfig("single-file-config").entrySet().iterator()

    while(confDataIterator.hasNext) {

          val confMapEntry = confDataIterator.next()
          val columnName = confMapEntry.getKey.trim
          val columnConf = appConfig.getAnyRef("single-file-config." + columnName).asInstanceOf[util.ArrayList[Int]]
          val startIndex = columnConf.get(0)
          val length = columnConf.get(1)


      println(columnName)
      println(startIndex)

      println(length)
    }


  }

}

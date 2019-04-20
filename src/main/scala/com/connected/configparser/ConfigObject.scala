package com.connected.configparser

case class ConfigObject(columnName: String, startIndex: Int, length: Int)

object ConfigObject {

  def apply(parserConfArr: Array[Array[String]]): Array[ConfigObject] = {

    val finalFieldsConfig = new Array[ConfigObject](parserConfArr.length)
    for (i <- parserConfArr.indices) {
        val configRow = parserConfArr(i)
      finalFieldsConfig(i) = ConfigObject(configRow(0).trim, configRow(1).trim.toInt, configRow(2).trim.toInt)
    }
    finalFieldsConfig
  }

  def convertConfigToSql(columnConfigData: Array[ConfigObject]):String = {

    val columnsSqlArr = new Array[String](columnConfigData.length)

    for(i<-columnConfigData.indices){
      columnsSqlArr(i) = s"TRIM(SUBSTR(SingleFileParserValue,${columnConfigData(i).startIndex},${columnConfigData(i).length})) AS ${columnConfigData(i).columnName}"
    }
    columnsSqlArr.mkString(",\n")
  }

}

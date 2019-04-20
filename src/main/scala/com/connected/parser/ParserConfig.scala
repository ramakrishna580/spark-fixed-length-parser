package com.connected.parser

case class ParserConfig(columnName: String, startIndex: Int, length: Int)

object ParserConfig {

  def apply(parserConfArr: Array[Array[String]]): Array[ParserConfig] = {

    val finalFieldsConfig = new Array[ParserConfig](parserConfArr.length)
    for (i <- parserConfArr.indices) {
        val configRow = parserConfArr(i)
      finalFieldsConfig(i) = ParserConfig(configRow(0).trim, configRow(1).trim.toInt, configRow(2).trim.toInt)
    }
    finalFieldsConfig
  }

  def convertConfigToSql(columnConfigData: Array[ParserConfig]):String = {

    val columnsSqlArr = new Array[String](columnConfigData.length)

    for(i<-columnConfigData.indices){
      columnsSqlArr(i) = s"TRIM(SUBSTR(SingleFileParserValue,${columnConfigData(i).startIndex},${columnConfigData(i).length})) AS ${columnConfigData(i).columnName}"
    }
    columnsSqlArr.mkString(",\n")
  }

}

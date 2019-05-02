package com.connected.configparser

/**
  * This class is representation of config used to parser the data.
  * @param columnName Name of the column to be returned.
  * @param startIndex Starting index of the column data for the column.
  * @param length Length of the data to be extracted.
  */
case class ConfigObject(columnName: String, startIndex: Int, length: Int)

/**
  * This object contains config parsing methods
  */
object ConfigObject {

  /**
    * This method is used to convert the config to ConfigObject.
    * @param parserConfArr Conf data extracted from config file.
    * @return [['Array[ConfigObject]']] containing all columns data.
    */
  def apply(parserConfArr: Array[Array[String]]): Array[ConfigObject] = {

    val finalFieldsConfig = new Array[ConfigObject](parserConfArr.length)
    for (i <- parserConfArr.indices) {
        val configRow = parserConfArr(i)
      finalFieldsConfig(i) = ConfigObject(configRow(0).trim, configRow(1).trim.toInt, configRow(2).trim.toInt)
    }
    finalFieldsConfig
  }

  /**
    * This method will be converting the config object to sql string.
    * @param columnConfigData [['Array[ConfigObject]']] containing all columns data.
    * @return String as select column sql
    */
  def convertConfigToSql(columnConfigData: Array[ConfigObject]):String = {

    val columnsSqlArr = new Array[String](columnConfigData.length)

    for(i<-columnConfigData.indices){
      columnsSqlArr(i) = s"TRIM(SUBSTR(SingleFileParserValue,${columnConfigData(i).startIndex},${columnConfigData(i).length})) AS ${columnConfigData(i).columnName}"
    }
    columnsSqlArr.mkString(",\n")
  }

}

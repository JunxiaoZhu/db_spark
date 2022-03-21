package com.imooc.tiantian

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

/**
 * Spark实现多路输出
 */
class MyMultipleOut extends MultipleTextOutputFormat[String, Integer] {

  override def generateFileNameForKeyValue(key: String, value: Integer, name: String): String = {
    //直接按照key分文件
    //key.asInstanceOf[String]
    //hello分一个，其他分一个
    key match {
      case "hello" => "type1"
      case _ => "type2"
    }
  }

}
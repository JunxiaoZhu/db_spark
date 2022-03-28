package com.imooc.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 需求：使用json文件创建DataFrame
 * Created by xuwei
 */
object SqlDemoScala {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local")

    //创建SparkSession对象，里面包含SparkContext和SqlContext
    val sparkSession = SparkSession.builder()
      .appName("SqlDemoScala")
      .config(conf)
      .getOrCreate()

    //读取json文件，获取DataFrame
    val stuDf = sparkSession.read.json("/Users/zhujunxiao/Downloads/学习资料/bigdata_course_materials/spark/下/student.json").as("aa")

    //查看DataFrame中的数据
    stuDf.show()

    sparkSession.stop()
  }

}

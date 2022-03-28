package com.imooc.scala

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求：checkpoint的使用
 * Created by xuwei
 */
object CheckpointOpScala {

  def main(args: Array[String]): Unit = {
    val ip = "172.16.7.220"

    val conf = new SparkConf()
    conf.setAppName("CheckpointOpScala")
      .setMaster("local")
    val sc = new SparkContext(conf)

    //1：设置checkpoint目录
    sc.setCheckpointDir("hdfs://"+ip+":9000/checkpoint")

    val dataRDD = sc.textFile("hdfs://"+ip+":9000/hello_10000000.dat")
        .persist(StorageLevel.MEMORY_ONLY)//执行持久化

    //2：对rdd执行checkpoint操作
    dataRDD.checkpoint()

    val ty = dataRDD.flatMap(_.split(" "))


    ty.map((_,1))
      .reduceByKey(_ + _).foreach(println(_))

    sc.stop()

  }

}

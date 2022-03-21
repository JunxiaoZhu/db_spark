package com.imooc.tiantian

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark实现多路输出
 */
class RDDMultipleTextOutputFormat {

  def main(args: Array[String]) {
    //第一步：创建SparkContext
    val conf = new SparkConf().setAppName("RDDMultipleTextOutputFormat").setMaster("local")
    val sc = new SparkContext(conf)
    //第二步：加载数据
    var path = "D:\\hello.txt"
    val linesRDD = sc.textFile(path)
    //第三步：对数据进行切割，把一行数据切分成一个个的单词
    val wordsRDD = linesRDD.flatMap(_.split(","))
    //第四步：迭代words，将每个word转化为(word,1)这种形式
    val pairRDD = wordsRDD.map((_,1))
    //第五步：根据key（其实就是word）进行分组聚合操作
    val wordCountRDD = pairRDD.reduceByKey(_ + _)

    //第六步：将结果输出到hdfs文件中
    wordCountRDD.saveAsHadoopFile("hdfs://bigdata01:9000/test/MyMultipleOut",
      classOf[String],classOf[Integer],classOf[MyMultipleOut])

    sc.stop()
  }

}

package com.imooc.tiantian

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 排序
 */
object SortText {

  def main(args: Array[String]): Unit = {
    //第一步：创建SparkContext
    val conf = new SparkConf().setAppName("RDDMultipleTextOutputFormat").setMaster("local")
    val sc = new SparkContext(conf)
    //第二步：加载数据
    val linesRDD = sc.textFile("/Users/zhujunxiao/Downloads/logs/hello.txt").sortBy(e => Nums(e)).foreach(println(_))
    sc.stop()
  }


  case class Nums(nums: String) extends Ordered[Nums] with Serializable {
    def getNums() : String = {
      nums
    }
    override def compare(that: Nums): Int = {
      val numArr1 = nums.split(" ")
      val numA1 = Integer.parseInt(numArr1(0))
      val numA2 = Integer.parseInt(numArr1(1))

      val numArr2 = that.getNums().split(" ")
      val numB1 = Integer.parseInt(numArr2(0))
      val numB2 = Integer.parseInt(numArr2(1))
      var result : Int = 0
      if(numA1 > numB1){
        1
      }
      else if(numA1 < numB1){
        -1
      }else{
        if(numA2 > numB2){
          -1
        }else{
          1
        }
      }
    }
  }


}

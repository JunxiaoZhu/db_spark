package com.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;

/**
 *         使用 java函数式编程实现
 *         统计单词数
 * @author JunxiaoZhu
 */
public class WordCount {

    public static void main(String[] args) {
       /* //第一步：创建JavaSparkContext
        var conf = new SparkConf();
        conf.setAppName("WordCountJava");     //设置任务名称
               // .setMaster("local[2]");      //local表示在本地执行（提交服务器时，会被注释）
        var sc = new JavaSparkContext(conf);

        //第二步：加载数据
        String path = "/Users/zhujunxiao/Downloads/logs/hello.txt";
        if(args.length==1){
            path = args[0];      //方便提交执行
        }
        var linesRDD = sc.textFile(path);

        //第三步：对数据进行切割，把一行数据切分成一个一个的单词
        JavaRDD<String> wordsRDD = linesRDD.flatMap(a1 -> { return  Arrays.asList(a1.split(" ")).iterator(); });

        //第四步：迭代words,将每个word转化为(word,1)这种形式
        var pairRDD = wordsRDD.mapToPair(a1 ->  new Tuple2<String, Integer>(a1, 1));

        //第五步：根据key(其实就是word)进行分组聚合统计
        var wordCountRDD = pairRDD.reduceByKey((a1,a2) ->  a1 + a2);

        //第六步：将结果打印到控制台
        wordCountRDD.foreach(wordCount->System.out.println(wordCount._1+"--"+wordCount._2));

        //第七步：停止SparkContext
        sc.stop();*/
    }

}

package com.imooc.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * 需求：load和save的使用
 * Created by xuwei
 */
public class LoadAndSaveOpJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");

        //创建SparkSession对象，里面包含SparkContext和SqlContext
        SparkSession sparkSession = SparkSession.builder()
                .appName("LoadAndSaveOpJava")
                .config(conf)
                .getOrCreate();

        //读取数据
        Dataset<Row> stuDf = sparkSession.read().format("json").load("/Users/zhujunxiao/Downloads/学习资料/bigdata_course_materials/spark/下/student.json");

        //保存数据
        stuDf.select("name","age")
                .write()
                .format("csv")
                //.mode(SaveMode.Ignore)
                .save("/Users/zhujunxiao/Downloads/logs/test");

        sparkSession.stop();
    }

}

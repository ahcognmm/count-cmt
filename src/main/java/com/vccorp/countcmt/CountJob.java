package com.vccorp.countcmt;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import org.apache.spark.sql.SparkSession;
import scala.Tuple1;
import scala.Tuple2;
import scala.collection.JavaConverters;

import javax.xml.bind.SchemaOutputResolver;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CountJob {

    //    static SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Count Fb comments").set("spark.executor.memory", "40g");
//    static SparkContext sc = new SparkContext(conf);
//    static SparkSession spark = new SparkSession(sc);
    //static SQLContext sqlContext = new SQLContext(sc);


    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Count fb comment")
                .getOrCreate();

        Dataset<Row> comment;
        JavaRDD<Row> cmt, cmtOfPost;
        JavaRDD<String> input;
        JavaPairRDD<String, Integer> input1 = null;
        JavaPairRDD<String, Integer> finalOut;

//        comment.select("createTime").show();

        ArrayList<String> fileNames = readFileName("/adscloud2/fb-logging/page-comments/2017-11-30/");
        comment = spark.read().json("file:///adscloud2/fb-logging/page-comments/2017-11-30/" + fileNames.get(0));
        fileNames.remove(0);
        cmt = comment.select("createTime").javaRDD();
        cmtOfPost = comment.select("postId").javaRDD();

        System.out.println("Hien lan 1 ==========" + cmt.count());
        System.out.println("Hien lan 1 ==========" + cmtOfPost.count());

        cmt = cmt.union(spark.read().json("file:///adscloud2/fb-logging/page-comments/2017-11-30/" + fileNames.get(0)).select("createTime").javaRDD());
        cmtOfPost = cmtOfPost.union(spark.read().json("file:///adscloud2/fb-logging/page-comments/2017-11-30/" + fileNames.get(0)).select("postId").javaRDD());

        System.out.println("Hien lan 2 ==========" + cmt.count());
        System.out.println("Hien lan 2 ==========" + cmtOfPost.count());
        int size = fileNames.size() - 1;

        //delete some elements of array
//        for (int i = 0; i < size; i++) {
//            if (i > 50) {
//                fileNames.remove(50);
//            }
//        }
        //cant use forEach cause cmt is not final
        for (String s : fileNames) {
            Dataset<Row> cmtt = spark.read().json("file:///adscloud2/fb-logging/page-comments/2017-11-30/" + s);
            cmt = cmt.union(cmtt.select("createTime").javaRDD());
            cmtOfPost = cmtOfPost.union(cmtt.select("postId").javaRDD());
        }


        //count cmt per day
        input = cmt.map(o -> {
            String[] arr = formatString(String.valueOf(o)).split(" ");
            if (arr.length > 2) return arr[0] + " " + arr[1] + " " + arr[2];
            return "syntax error";
        });
        JavaPairRDD<String, Integer> result = input.mapToPair(s -> new Tuple2(s, 1));
        JavaPairRDD<String, Integer> finalRs = result.reduceByKey((i1, i2) -> i1 + i2);
        finalRs.cache();
        System.out.println("total syntax error is: "+ finalRs.lookup("syntax error"));
        System.out.println("total cmt of 2017-11-30:"+finalRs.lookup("30 Nov 2017"));
        System.out.println("hello=====================================" + finalRs.count());

        //count cmt per a post
        input1 = cmtOfPost.map(s -> String.valueOf(s)).mapToPair(s -> new Tuple2(s, 1));
        finalOut = input1.reduceByKey((i1, i2) -> i1 + i2);
//        finalOut.saveAsTextFile("file:///data/hapn/output2");
        System.out.println("finalOut.count() ===== " + finalOut.count());
    }


    private static ArrayList<String> readFileName(String path) {
        ArrayList<String> fileNames = new ArrayList<>();
        File folder = new File(path);
        File[] listFiles = folder.listFiles();
        for (File f : listFiles) {
            fileNames.add(f.getName());
        }
        return fileNames;
    }

    private static String formatString(String s) {
        return s.substring(1);
    }

}

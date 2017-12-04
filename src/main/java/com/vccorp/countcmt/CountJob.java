package com.vccorp.countcmt;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CountJob {

    static SparkConf conf = new SparkConf().setMaster("local").setAppName("Count Fb comments");
    static SparkContext sc = new SparkContext(conf);

    static SQLContext sqlContext = new SQLContext(sc);

    static Dataset<Row> comment;
    static JavaRDD<Row> cmt;

    public static void main(String[] args) {
//        comment.select("createTime").show();

        ArrayList<String> fileNames = readFileName("/adscloud2/fb-logging/page-comments/2017-11-30/");
        comment = sqlContext.read().json("/adscloud2/fb-logging/page-comments/2017-11-30/" + fileNames.get(0));
        fileNames.remove(0);

        cmt = comment.select("createTime").toJavaRDD();
        fileNames.forEach(s -> {
            Dataset<Row> cmtT = sqlContext.read().json("/adscloud2/fb-logging/page-comments/2017-11-30/" + s);
            cmt = cmt.union(cmtT.select("createTime").toJavaRDD());
        });

        System.out.println("hello from the other side" + comment.count());

        JavaRDD<String> input = cmt.flatMap(o -> {
            String s = String.valueOf(o);
            String[] arr = s.split(" ");
            List<String> arrs = new ArrayList<>();
            arrs.add(arr[0] + " " + arr[1] + " " + arr[2]);
            return arrs.iterator();
        });

        JavaPairRDD<String, Integer> result = input.mapToPair(s -> new Tuple2(s, 1));
        JavaPairRDD<String, Integer> finalRs = result.reduceByKey((i1, i2) -> i1 + i2);
        System.out.println(input.count());
        finalRs.collectAsMap().entrySet().forEach(e -> System.out.println(e.getKey() + ":::" + e.getValue().toString() + "]"));
        input.collect().forEach(s -> System.out.println(s));
        cmt.collect().forEach(s -> System.out.println(s));
        System.out.println(cmt.count());
        System.out.println(finalRs.lookup("[15 Sep 2017"));

        JavaRDD<Row> cmtOfPost = comment.select("postId").toJavaRDD();
        JavaPairRDD<String, Integer> intput1 = cmtOfPost.flatMap(s -> Arrays.asList(s.toString()).iterator()).mapToPair(s -> new Tuple2(s, 1));
        JavaPairRDD<String, Integer> finalOut = intput1.reduceByKey((i1, i2) -> i1 + i2);
        finalOut.collectAsMap().entrySet().forEach(e -> System.out.println(e.getKey() + ":::" + e.getValue().toString()));
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

}

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object CountJob {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder().appName("Count cmt by scala").getOrCreate
        import spark.implicits._
        var fileNames = getFileNames("/adscloud2/fb-logging/page-comments/2017-11-30/")
        var raw = spark.read.json("file:///adscloud2/fb-logging/page-comments/2017-11-30/" + fileNames(0)).select("createTime")
        var rawCmt = spark.read.json("file:///adscloud2/fb-logging/page-comments/2017-11-30/" + fileNames(0)).select("commentFbId")

        val fileSize = Integer.parseInt(args(0))
        if (fileSize != null) fileNames = fileNames.take(fileSize)

        fileNames.foreach(name => {
            if (name != fileNames(0)) {
                raw = raw.union(spark.read.json("file:///adscloud2/fb-logging/page-comments/2017-11-30/" + name).select("createTime"))
                rawCmt = raw.union(spark.read.json("file:///adscloud2/fb-logging/page-comments/2017-11-30/" + name).select("commentFbId"))
            }
        })

        var input = raw.map(o => {
            var arr = String.valueOf(o).split(" ")
            if (arr.length > 2) arr(0) + " " + arr(1) + " " + arr(2)
            else "syntax error"
        }).rdd
        var rs = input.map((_, 1)).reduceByKey(_ + _)
        rs.cache


        var inputCmt = rawCmt.map(String.valueOf(_)).rdd
        var rsCmt = inputCmt.map((_, 1)).reduceByKey(_ + _)
        rsCmt.cache

        val totalSynErr = rs.lookup("syntax error")
        val cmtNow = rs.lookup("[30 Nov 2017")
        val countrs = rs.count()
        val totalPost = rsCmt.count()

        println("total posts : " + totalPost)
        println("total syntax errors : " + totalSynErr)
        println("cmt on 30 Nov 2017 is: " + cmtNow)
        //total days pulled
        println("total days pulled: " + countrs)
        spark.close()

    }

    def getFileNames(path: String): List[String] = {
        var folder = new File(path)
        var listFile = folder.listFiles
        var rs = listFile.map(f => f.getName).toList
        return rs
    }
}
//total posts : 19077935
//total syntax errors : ArrayBuffer(690)
//cmt on 30 Nov 2017 is: ArrayBuffer(142117)
//hello from the other side===========1797

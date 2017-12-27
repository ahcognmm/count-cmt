import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CountPost {

    def main(args: Array[String]): Unit = {
        //        val conf = new SparkConf().setMaster("local[*]").setAppName("Simple App")
        //        val spark = SparkSession.builder().config(conf).getOrCreate()
        val spark = SparkSession.builder().appName("Get post commented per user").getOrCreate()
        import spark.implicits._

        var fileNames = getFileNames("/adscloud2/fb-logging/page-comments/2017-11-30//")
        var input = spark.read.json(s"file:///adscloud2/fb-logging/page-comments/2017-11-30//${fileNames(0)}").select("userId", "postID")

        val fileSize = Integer.parseInt(args(0))
        fileNames = fileNames.take(fileSize)

        fileNames.foreach(file => {
            if (file != fileNames(0)) {
                input = input.union(spark.read.json(s"file:///adscloud2/fb-logging/page-comments/2017-11-30//$file").select("userId", "postId"))
            }
        })

        val pair = input.map(o => {
            val string = String.valueOf(o).split(",")
            (string(0), string(1))
        }).rdd
        val rs = pair.groupByKey
        rs.cache()

        //result test
        val pairCount = pair.count
        val rsCount = rs.count
        val idTest = rs.lookup("122649745154715")

        println(s"=====================$pairCount==============")
        println(s"=====================$rsCount++++++++++++++++")
        println(s"id: 122649745154715::::::$idTest")
        //shutsrs.foreach(println(_))
        spark.close()

    }

    def getFileNames(path: String): List[String] = {
        var folder = new File(path)
        var listFile = folder.listFiles
        var rs = listFile.map(f => f.getName).toList
        return rs
    }
}

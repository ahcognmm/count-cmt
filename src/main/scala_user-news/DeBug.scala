//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//
//object DeBug {
//    def main(args: Array[String]): Unit = {
//
//        var listFileName = new GetListFile("/home/ahcogn/Documents/user_data/2018-01-16").getList
//        val conf = new SparkConf().setAppName("UserNewsJob").setMaster("local[*]").set("spark.executor.memory", "4g").set("spark.excutor.cores", "3")
//        val sc = new SparkContext(conf)
//
//        var textFile: RDD[String] = sc.textFile(s"/home/ahcogn/Documents/user_data/2018-01-16/${listFileName(0)}")
//                .flatMap(line => line.split("\n"))
//
//        listFileName.take(100).foreach(file => {
//            if (file != listFileName(0)) {
//                val stickFile: RDD[String] = sc.textFile(s"/home/ahcogn/Documents/user_data/2018-01-16/$file").flatMap(line => line.split("\n"))
//                textFile = textFile.union(stickFile)
//            }
//        })
//
//        //        val user: RDD[(String, Int)] = textFile
//        //                .map(line => {
//        //                    val words = line.split("\t")
//        //                    try {
//        //                        (words(13), 1)
//        //                    } catch {
//        //                        case e: Exception => ("error", -1)
//        //                    }
//        //                }).reduceByKey(_ + _)
//        //        val max = user.filter(use => use._1 == "6322334301952877222")
//        //        println("===============================")
//        //        max.foreach(println(_))
//        //        println(s"ket qua ${textFile.count()}")
//
//        val relationship: RDD[(String, Long)] = textFile.map(line => { //start
//            val words = line.split("\t")
//            try {
//                (words(8) + words(11), words(13).toLong)
//            } catch {
//                case e: Exception => (words(8) + words(11), -1L)
//            }
//        }).distinct()
//
//        print(s"ket qua ${relationship.filter(relation => relation._1 == "dantri.com.vn/su-kien/chuyen-tinh-vo-kem-chong-52-tuoi-day-song-du-luan-20180115142656851.htm").count()}")
//
//        val news: RDD[(String, Int)] = textFile.map(line => {
//            val words = line.split("\t")
//            (words(8) + words(11), 1)
//        }).reduceByKey(_ + _)
//
//        val newsRs = news.filter(newss => newss._2 > 200)
//        newsRs.foreach(println(_))
//
//    }
//
//}

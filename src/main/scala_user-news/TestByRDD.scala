import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class TestByRDD(val quanOfFile: Int, val sc: SparkContext) {

    protected def getRelationshipRDD: RDD[(String, Long)] = {

        var listFileName = new GetListFile("/home/ahcogn/Documents/user_data/2018-01-16").getList

        var listRawRDD: Seq[RDD[String]] = listFileName.take(quanOfFile).map(file => {
            sc.textFile(s"/home/ahcogn/Documents/user_data/2018-01-16/$file")
        })
        val rawtextFile = sc.union(listRawRDD)

        val textFile = rawtextFile.flatMap(line => line.split("\n"))

        val relationship: RDD[(String, Long)] = textFile.map(line => {
            val words = line.split("\t")
            try {
                (words(8) + words(11), words(13).toLong)
            } catch {
                case e: Exception => (words(8) + words(11), -1L)
            }
        }).distinct()
        relationship
    }

    def getNews(idUser: Long): Array[String] = {
        getRelationshipRDD.filter(relation => relation._2 == idUser).map(relation => relation._1).collect()
    }

}

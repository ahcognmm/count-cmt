import java.io.ByteArrayOutputStream

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel

class DeBug(val quanOfFile: Int, val sc: SparkContext) {

    def getGraph(newsLink: String) = {

        val si = SparkEnv.get.closureSerializer.newInstance()

        var listFileName = new GetListFile("/home/ahcogn/Documents/user_data/2018-01-16").getList

        //        var rawtextFile = sc.textFile(s"/home/ahcogn/Documents/user_data/2018-01-16/${listFileName(0)}")
//                listFileName.take(quanOfFile).foreach(file => {
//                    if (file != listFileName(0)) {
//                        val stickFile: RDD[String] = sc.textFile(s"/home/ahcogn/Documents/user_data/2018-01-16/$file")
//                        rawtextFile = sc.union(Seq(rawtextFile,stickFile))
//                    }
//                })


        //        val textFile = rawtextFile.flatMap(line => line.split("\n"))
        //        textFile.persist(StorageLevel.MEMORY_ONLY_SER_2)
        //
        //        val countTextFile = textFile.count();

        // create vertex : user_news
        // user_news contain: user & news
        // malformed .flatMap(line => line.split("\n")user return id = -1
        //        val user: RDD[(VertexId, String)] = textFile.map(line => {
        //            val words = line.split("\t")
        //            try {
        //                (words(13).toLong, words(13))
        //            } catch {
        //                case e: Exception => (-1L, "error user")
        //            }
        //        }).distinct
        //
        //        val rawNews = textFile.map(line => {
        //            val words = line.split("\t")
        //            words(8) + words(11)
        //        }).distinct()
        //
        //        val news: RDD[(String, Long)] = rawNews.zipWithUniqueId()
        //
        //        val user_news: RDD[(VertexId, String)] = user.union(news.map(news => (news._2, news._1)))
        //
        //        val user_newsCount = user_news.count();


        //create edge: edge relationship
        // srcId : user Id
        //dstID : news ID
        // properity : news's link
        //        val relationship: RDD[(String, Long)] = textFile
        //                .map(line => {
        //                    val words = line.split("\t")
        //                    try {
        //                        (words(8) + words(11), words(13).toLong)
        //                    } catch {
        //                        case e: Exception => (words(8) + words(11), -1L)
        //                    }
        //                })
        //
        //        val edge_relationship: RDD[Edge[String]] = relationship.leftOuterJoin(news).map(relation => {
        //            Edge(relation._2._1, relation._2._2.get, relation._1)
        //        }).distinct()
        //
        //        val relationshipCount = relationship.count()

        // create Graph
        //        val graph = Graph(user_news, edge_relationship)
        //        println(s"++++++++++++ da den day ${countTextFile}")
        //        println(graph.vertices.count())


        //        val newsId = graph.vertices.filter(news => news._2 == newsLink).map(news => news._1).collect()(0)
        //        val edgeRDDs = graph.edges.filter(edgeRDD => edgeRDD.dstId == newsId)
        //        edgeRDDs.map(edgeRDDs => edgeRDDs.srcId.toString).collect.foreach(println(_))


    }


    //    def getNews(userId: Long): Array[String] = {
    //
    //        val edgeRDDs = graphX.edges.filter(edgeRDD => edgeRDD.srcId == userId)
    //        edgeRDDs.map(edgeRDDs => edgeRDDs.attr).collect
    //    }
    //
    //    def getTotalsEdges: Long = {
    //        val total = graphX.edges.count()
    //        total
    //    }
    //
    //    def getUsers(newsLink: String): Array[String] = {
    //        val graphX = getGraph
    //        val newsId = graphX.vertices.filter(news => news._2 == newsLink).map(news => news._1).collect()(0)
    //        val edgeRDDs = graphX.edges.filter(edgeRDD => edgeRDD.dstId == newsId)
    //        edgeRDDs.map(edgeRDDs => edgeRDDs.srcId.toString).collect
    //    }

}
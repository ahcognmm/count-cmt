import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

class UserNewsJob(val quanOfFile: Int, val sc: SparkContext) extends Serializable {

    protected def getGraph: Graph[Seq[String], String] = {


        var listFileName = new GetListFile("/home/ahcogn/Documents/user_data/2018-01-16").getList

        var listRawRDD: Seq[RDD[String]] = listFileName.take(quanOfFile).map(file => {
            sc.textFile(s"/home/ahcogn/Documents/user_data/2018-01-16/$file")
        })
        val rawtextFile = sc.union(listRawRDD)

        val textFile = rawtextFile.flatMap(line => line.split("\n"))


        // create vertex : user_news
        // user_news contain: user & news
        // malformed .flatMap(line => line.split("\n")user return id = -1
        val user: RDD[(VertexId, Seq[String])] = textFile.map(line => {
            val words = line.split("\t")

            try {
                (words(13).toLong, Seq(words(13)))
            } catch {
                case e: Exception => (-1L, Seq("error user"))
            }
        }).distinct

        val rawNews = textFile.map(line => {
            val words = line.split("\t")
            words(8) + words(11)
        }).distinct()

        val news: RDD[(String, Long)] = rawNews.zipWithUniqueId()

        val user_news: RDD[(VertexId, Seq[String])] = user.union(news.map(news => (news._2, Seq(news._1))))

        //create edge: edge relationship
        // srcId : user Id
        //dstID : news ID
        // properity : news's link
        val relationship: RDD[(String, Long)] = textFile.map(line => {
            val words = line.split("\t")
            try {
                (words(8) + words(11), words(13).toLong)
            } catch {
                case e: Exception => (words(8) + words(11), -1L)
            }
        })

        val edge_relationship: RDD[Edge[String]] = relationship.leftOuterJoin(news).map(relation => {
            Edge(relation._2._1, relation._2._2.get, relation._1)
        }).distinct()

        // create Graph
        val graph = Graph(user_news, edge_relationship)
        graph.cache()
        return graph
    }

    val graphX = getGraph

    def getNews(userId: Long): Array[String] = {
        graphX.edges.filter(edgeRDD => edgeRDD.srcId == userId).map(edgeRDDs => edgeRDDs.attr).collect
    }

    def getTotalsEdges: Long = {
        val total = graphX.edges.count()
        total
    }

    def getUsers(newsLink: String): Seq[Seq[String]] = {
        val newsId = graphX.vertices.filter(news => news._2(0) == newsLink).map(news => news._1).collect()(0)
        //        graphX.edges.filter(edgeRDD => edgeRDD.dstId == newsId).map(edgeRDDs => edgeRDDs.srcId.toString).collect
        pregelGraph().vertices.lookup(newsId)
    }


    // using prehel Graph
    val initialMsg = Seq("Start_here")

    def pregelGraph(): Graph[Seq[String], String] = {
        val pregelGraph = graphX.pregel(initialMsg, Int.MaxValue, EdgeDirection.Out)(vprog, sendMsg, mergeMsg)
        pregelGraph.cache()
        pregelGraph
    }

    def vprog(vertexId: VertexId, value: Seq[String], message: Seq[String]): Seq[String] = {
        if (message == initialMsg)
            value
        else
            value.union(message)
    }

    def sendMsg(triplet: EdgeTriplet[Seq[String], String]): Iterator[(VertexId, Seq[String])] = {
        val sourceVertex = triplet.srcAttr
        val destVertex = triplet.dstAttr

        if (destVertex.last == sourceVertex.head) {

            Iterator.empty
        }
        else
            Iterator((triplet.dstId, sourceVertex))
    }

    def mergeMsg(msg1: Seq[String], msg2: Seq[String]): Seq[String] = msg1.union(msg2)

    // using aggrerate neighbor
    def getUserByAggrerate(newsLink : String)={
        val newsId : VertexId = graphX.vertices.filter(news => news._2(0) == newsLink).map(news => news._1).collect()(0)
        graphX.aggregateMessages[Seq[Long]](tripletFields =>{
            tripletFields.sendToDst(Seq(tripletFields.srcId))},
            (msg1, msg2) =>{ msg1.union(msg2)}
        ).lookup(newsId)
    }

    // using collectNeighbor
    def testGetUser (newsLink :String): Array[Long] ={
        val newsId : VertexId = graphX.vertices.filter(news => news._2(0) == newsLink).map(news => news._1).collect()(0)
        graphX.collectNeighborIds(EdgeDirection.In).lookup(newsId)(0)
    }


}

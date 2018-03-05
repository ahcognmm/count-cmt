import java.io.ByteArrayOutputStream

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel

object DeBug {
    val initialMsg = Seq(9999)

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local").setAppName("hello from the other side")
        val sc: SparkContext = new SparkContext(conf)


        // Create an RDD for the vertices
        val vertices: RDD[(VertexId, Seq[Int])] =
            sc.parallelize(Array((1L, Seq(1)), (2L, Seq(2)),
                (3L, Seq(3)), (4L, Seq(4)), (5L, Seq(5)), (6L, Seq(6))))

        // Create an RDD for edges
        val relationships: RDD[Edge[Boolean]] =
            sc.parallelize(Array(
                Edge(6L, 1L, true),
                Edge(6L, 3L, true),
                Edge(5L, 1L, true),
                Edge(5L, 4L, true),
                Edge(2L, 4L, true),
                Edge(2L, 1L, true),
                Edge(2L, 3L, true)
            ))

        // Create the graph
        val graph = Graph(vertices, relationships)

        val x: VertexId = 3L

//        val hello = graph.collectNeighborIds(EdgeDirection.Out).lookup(x)
        val hell = graph.aggregateMessages[Seq[Long]](tripletFields => {
            tripletFields.sendToDst(Seq(tripletFields.srcId))
        }, _.union(_)
        ).lookup(3)

        println(hell)
        //        hello.foreach(a => {
        //            print(a._1 + " ")
        //            a._2.foreach(print(_))
        //            println()
        //        })


        //        val minGraph = graph.pregel(initialMsg,
        //            Int.MaxValue,
        //            EdgeDirection.Out)(
        //            vprog,
        //            sendMsg,
        //            mergeMsg)
        //        minGraph.vertices.collect.foreach {
        //            case (seq) => println(seq)
        //        }

        //                println(test)

    }

    def vprog(vertexId: VertexId, value: Seq[Int], message: Seq[Int]): Seq[Int] = {
        if (message == initialMsg)
            value
        else
            value.union(message)
    }

    def sendMsg(triplet: EdgeTriplet[Seq[Int], Boolean]): Iterator[(VertexId, Seq[Int])] = {
        val sourceVertex = triplet.srcAttr
        val destVertex = triplet.dstAttr

        if (destVertex.last == sourceVertex.head) {

            Iterator.empty
        }
        else
            Iterator((triplet.dstId, sourceVertex))
    }

    def mergeMsg(msg1: Seq[Int], msg2: Seq[Int]): Seq[Int] = msg1.union(msg2)

    def test: Int = {
        val hehe = Seq(1, 2, 3, 4, 5)
        val haha = Seq(6, 7, 8, 9, 10)
        println(hehe.union(haha))
        return 1
    }
}
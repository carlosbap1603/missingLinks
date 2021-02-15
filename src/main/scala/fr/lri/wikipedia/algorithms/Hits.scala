package fr.lri.wikipedia.algorithms

import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, Pregel, VertexId, VertexRDD}
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

object Hits {

  case class VertexAttr(srcId: Long, authScore: Double, hubScore:Double)

  case class HitsMsg(authScore:Double, hubScore:Double)


  def initialize[VD:ClassTag, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int) ( implicit spark: SparkSession): Graph[VertexAttr, ED] = {
    val hitsGraph = graph.mapVertices { case (vid, _) =>
      new VertexAttr( srcId = vid, authScore = 1.0, hubScore = 1.0 )
    }

    def sendMessage(e: EdgeTriplet[VertexAttr, ED]): Iterator[(VertexId, HitsMsg)] = {
      Iterator( (e.dstId, HitsMsg(0.0, e.srcAttr.hubScore)), (e.srcId, HitsMsg(0.0, e.dstAttr.hubScore)))
    }

    def mergeMessage(msg1: HitsMsg, msg2: HitsMsg): HitsMsg = {
      HitsMsg(msg1.authScore + msg2.authScore, msg1.hubScore + msg2.hubScore)
    }

    def vertexProgram(vId: VertexId, vInfo: VertexAttr, message: HitsMsg)( implicit spark: SparkSession): VertexAttr = {
      VertexAttr(vInfo.srcId, if (message.authScore == 0.0) 1.0 else message.authScore , if (message.hubScore == 0.0) 1.0 else message.hubScore)
    }

    val initialMessage: HitsMsg = null

    Pregel(hitsGraph, initialMessage, maxIterations = maxSteps, EdgeDirection.Both)(
    vprog = vertexProgram,
    sendMsg = sendMessage,
    mergeMsg = mergeMessage)

  }
}
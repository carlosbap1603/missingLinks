package fr.lri.wikipedia.centrality

import fr.lri.wikipedia.{Link, WikiPage}
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

private class ReciprocalPageRank extends CentralityMeasure {
  def getMapVector( originalGraph:Graph[WikiPage,Link], neighborhood: Set[Long] ):Map[String,Double]={

    val map = if( !neighborhood.isEmpty ) {

      val neighborhoodGraph = originalGraph.subgraph(vpred = (id,_) => neighborhood.contains(id))
      val pRankGraph = neighborhoodGraph.pageRank(0.01)

      pRankGraph.vertices.map { case (id, rank) =>
        Map(id.toString -> (1/rank) )
      }.reduce((a, b) => a ++ b)
    }else{ Map[String,Double]() }

    map
  }
}
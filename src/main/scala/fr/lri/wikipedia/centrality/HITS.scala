package fr.lri.wikipedia.centrality

import fr.lri.wikipedia.algorithms.Hits
import fr.lri.wikipedia.{Link, WikiPage}
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

private class HITS extends CentralityMeasure {

  def getMapVector(originalGraph: Graph[WikiPage, Link], neighborhood: Set[Long])( implicit spark: SparkSession): Map[String, Double] = {

    val map = if (!neighborhood.isEmpty) {

      val neighborhoodGraph = originalGraph.subgraph(vpred = (id, _) => neighborhood.contains(id))

      val hitsGraph = Hits.initialize(neighborhoodGraph,10)

      hitsGraph.vertices.map { case (id, vertexAttr) =>
        Map(id.toString -> vertexAttr.authScore )
      }.reduce((a, b) => a ++ b)

    } else {
      Map[String, Double]()
    }

    map
  }
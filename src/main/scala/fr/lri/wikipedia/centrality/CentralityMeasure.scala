package fr.lri.wikipedia.centrality

import fr.lri.wikipedia.{Link, WikiPage}
import org.apache.spark.graphx.Graph

trait CentralityMeasure extends Serializable {
  def getMapVector( originalGraph:Graph[WikiPage,Link], neighborhood: Set[Long] ):Map[String,Double]
}

object CentralityType extends Enumeration {
  val RECIPROCAL_PAGE_RANK = Value("RPR")
  val HITS = Value("HITS")
}

object CentralityMeasure{
  def apply(centralityType:CentralityType.Value):CentralityMeasure = {
    centralityType match {
      case CentralityType.RECIPROCAL_PAGE_RANK => new ReciprocalPageRank()
      case CentralityType.HITS => new HITS()
      case _ => new ReciprocalPageRank()
    }
  }
}
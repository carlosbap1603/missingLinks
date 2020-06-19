package fr.lri.wikipedia.graph

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{ScallopConf, Serialization}

class GraphRankNetConf(args:Seq[String]) extends ScallopConf(args) with Serialization {
  val dumpPath = opt[String](required = true, name="dumpPath")
  val titleSearch = opt[String](required = true, name="titleSearch")
  val step = opt[String](required = true, name="step")
  val languages = opt[List[String]](name="languages", default=Some(List()))
  verify()
}

object GraphRankNet {

  val sconf = new SparkConf().setAppName("Wikipedia: candidate analysis")
                              .setMaster("local[*]")
  val session = SparkSession.builder.config(sconf).getOrCreate()
  val ga = new GraphAnalyser(session)

  def main(args:Array[String]): Unit = {

    val conf = new GraphRankNetConf(args)
    val dumpDir = conf.dumpPath()
    val titleSearch =  conf.titleSearch()
    val step = conf.step().toInt
    val lang = conf.languages()

    ga.executeRankAnalysis(dumpDir, titleSearch, step, lang: _*)

  }
}

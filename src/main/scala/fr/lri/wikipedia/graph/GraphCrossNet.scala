package fr.lri.wikipedia.graph

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{ScallopConf, Serialization}

class GraphCrossNetConf(args:Seq[String]) extends ScallopConf(args) with Serialization {
  val dumpPath = opt[String](required = true, name="dumpPath")
  val languages = opt[List[String]](name="languages", default=Some(List()))
  verify()
}

object GraphCrossNet {

  val sconf = new SparkConf().setAppName("Wikipedia: crosslink analysis")
//                              .setMaster("local[*]")
  val session = SparkSession.builder.config(sconf).getOrCreate()
  val ga = new GraphAnalyser(session)

  def main(args:Array[String]): Unit = {

    val conf = new GraphCrossNetConf(args)
    val dumpDir = conf.dumpPath()
    val lang = conf.languages()

    ga.printAnomalies(dumpDir, lang: _*)

  }
}

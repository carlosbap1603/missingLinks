package fr.lri.wikipedia.graph

import fr.lri.wikipedia.AvroWriter
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{ScallopConf, Serialization}

class GraphInternalNetConf(args:Seq[String]) extends ScallopConf(args) with Serialization {
  val dumpPath = opt[String](required = true, name="dumpPath")
  val titleSearch = opt[String](required = true, name="titleSearch")
  val step = opt[String](required = true, name="step")
  val languages = opt[List[String]](name="languages", default=Some(List()))
  verify()
}

object GraphInternalNet extends AvroWriter{
  val sconf = new SparkConf().setAppName("Wikipedia graph candidates")
//                              .setMaster("local[*]")
  val session = SparkSession.builder.config(sconf).getOrCreate()
  val ga = new GraphAnalyser(session)

  def main(args:Array[String]): Unit = {

    val conf = new GraphInternalNetConf(args)
    val dumpDir = conf.dumpPath()
    val titleSearch =  conf.titleSearch()
    val step = conf.step().toInt
    val lang = conf.languages()

    ga.executeInternalLinkAnalysis(dumpDir, titleSearch, step, lang: _*)

  }
}

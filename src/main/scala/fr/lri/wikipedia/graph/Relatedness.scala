package fr.lri.wikipedia.graph

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{ScallopConf, Serialization}

class RelatednessConf(args:Seq[String]) extends ScallopConf(args) with Serialization {
  val dumpPath = opt[String](required = true, name="dumpPath")
  val titleA = opt[String](required = true, name="titleA")
  val titleB = opt[String](required = true, name="titleB")
  val step = opt[String](required = true, name="step")
  val languages = opt[List[String]](name="languages", default=Some(List()))
  verify()
}

object Relatedness {

  val sconf = new SparkConf().setAppName("Wikipedia: similarity analysis")
                                .setMaster("local[*]")
  val session = SparkSession.builder.config(sconf).getOrCreate()
  val ga = new GraphAnalyser(session)

  def main(args:Array[String]): Unit = {

    val conf = new RelatednessConf(args)
    val dumpDir = conf.dumpPath()
    val titleA =  conf.titleA()
    val titleB =  conf.titleB()
    val step = conf.step().toInt
    val lang = conf.languages()

    ga.executeSimilarityAnalysis(dumpDir, titleA, titleB, step, lang: _*)

  }

}

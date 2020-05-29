package fr.lri.wikipedia.parser

import java.nio.file.Paths

import fr.lri.wikipedia.graph.{GraphAnalyser, GraphBuilder}
import fr.lri.wikipedia.{AvroWriter, DumpLangLink, WikiLink, WikiPage}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.rogach.scallop.{ScallopConf, Serialization}

class LangLinkParserConf(args:Seq[String]) extends ScallopConf(args) with Serialization {
  val dumpPath = opt[String](required = true, name="dumpPath")
  val languages = opt[List[String]](name="languages", default=Some(List()))
  verify()
}

object LangLinkParser extends AvroWriter {

  val sconf =  new SparkConf().setAppName("Wikipedia language link parser")
//                              .setMaster("local[*]")
  val session = SparkSession.builder.config(sconf).getOrCreate()
  val gb = new GraphBuilder(session)

  def main(args:Array[String]) = {

    val conf = new LangLinkParserConf(args)
    val languages = conf.languages()
    val dumpDir = conf.dumpPath()

    getLangLinksId(dumpDir, languages: _*)

  }

  def getLangLinksId( dumpDir: String, languages: String* ) ={

    import session.implicits._

    val pages = gb.getPages(dumpDir,false, languages: _* )

    languages.foreach { src =>

      val outPath = Paths.get(s"${dumpDir}/${src}/langlinks", s"links").toString
      val langPath = Paths.get(s"${dumpDir}/${src}/langlinks", s"dumps").toString
      val langLinksDf = session.read.format("avro").load(langPath).as[DumpLangLink]
      val langlinks_id = mergeLangLink(langLinksDf, pages).as[WikiLink]

      writeAvro(langlinks_id.toDF(), outPath)

    }
  }

  def mergeLangLink(langLinksDf:Dataset[DumpLangLink], pagesDf:Dataset[WikiPage] ):DataFrame = {

    val links = langLinksDf.withColumnRenamed("sid", "from")
                            .withColumnRenamed("lang","fromLang")

    val pages = pagesDf.withColumnRenamed("sid","to")
                        .withColumnRenamed("lang","toLang")

    val result = links.join( pages, Seq("title","toLang") )

    result.select("from", "to", "fromLang", "toLang" )
  }

}

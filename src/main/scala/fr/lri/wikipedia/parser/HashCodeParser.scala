package fr.lri.wikipedia.parser

import java.nio.file.Paths

import fr.lri.wikipedia._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop.{ScallopConf, Serialization}

class HashCodeConf(args:Seq[String]) extends ScallopConf(args) with Serialization {
  val dumpPath = opt[String](required = true, name="dumpPath")
  val outputPath = opt[String](required = true, name="outputPath")
  val languages = opt[List[String]](name="languages", default=Some(List()))
  val fileType = opt[List[String]](name="fileType", default=Some(List()))

  verify()
}

object HashCodeParser extends AvroWriter {

  val sconf =  new SparkConf().setAppName("Wikipedia hash code parser")
//                              .setMaster("local[*]")
  val session = SparkSession.builder.config(sconf).getOrCreate()

  def processToDf[T <: WikiElement with Product]( lang:String,
                                                  input:RDD[String],
                                                  fileType:ElementType.Value,
                                                  filter: ElementFilter[T] = new DefaultElementFilter[T]):DataFrame = {

    val parser = fileType match {
      case ElementType.Page => new HashPageParser(lang, filter.asInstanceOf[ElementFilter[WikiPage]])
      case ElementType.Category => new HashPageParser(lang, filter.asInstanceOf[ElementFilter[WikiPage]])
      case ElementType.CategoryLink => new HashCategoryLinkParser(lang, filter.asInstanceOf[ElementFilter[WikiLink]])
      case ElementType.PageLink => new HashPageLinkParser(lang, filter.asInstanceOf[ElementFilter[WikiLink]])
      case ElementType.LangLink => new HashLangLinkParser(filter.asInstanceOf[ElementFilter[DumpLangLink]] )
      case ElementType.RawLangLink => new WikiDumpLangLinkParser(filter.asInstanceOf[ElementFilter[DumpLangLink]] )
    }

    parser.getDataFrame(session, input)
  }

  def processFileToDf[T <: WikiElement with Product](lang:String, inputFilename:String, wikiType:ElementType.Value,
                                                     filter: ElementFilter[T] = new DefaultElementFilter[T]):DataFrame = {
    val lines = session.sparkContext.textFile(inputFilename)
    processToDf(lang, lines, wikiType, filter)
  }

  def main(args:Array[String]) = {

    val conf = new HashCodeConf(args)

    val dumpDir = conf.dumpPath()
    val outDir = conf.outputPath()
    val languages = conf.languages()
    var fileType = conf.fileType()

    if( fileType.isEmpty )
      fileType =  List( ElementType.Page.toString,
                        ElementType.Category.toString,
                        ElementType.CategoryLink.toString,
                        ElementType.PageLink.toString,
                        ElementType.LangLink.toString )

    languages.foreach { src =>

      if( fileType.contains( ElementType.Page.toString ) ) {
        val inNormalPage = Paths.get(s"${dumpDir}/${src}/page/normal_pages").toString
        val outNormalPage = Paths.get(s"${outDir}/${src}/page/normal_pages").toString
        val normalPagesDF = processFileToDf(src, inNormalPage, ElementType.Page)

        writeAvro(normalPagesDF, outNormalPage)
      }

      if( fileType.contains( ElementType.Category.toString ) ) {
        val inCategoryPage = Paths.get(s"${dumpDir}/${src}/page/category_pages").toString
        val outCategoryPage = Paths.get(s"${outDir}/${src}/page/category_pages").toString
        val categoryPagesDF = processFileToDf(src, inCategoryPage, ElementType.Category)

        writeAvro(categoryPagesDF, outCategoryPage)
      }

      if( fileType.contains( ElementType.CategoryLink.toString ) ) {
        val inCategoryLinks = Paths.get(s"${dumpDir}/${src}/categorylinks").toString
        val outCategoryLinks = Paths.get(s"${outDir}/${src}/categorylinks").toString
        val categoryLinksDF = processFileToDf(src, inCategoryLinks, ElementType.CategoryLink)

        writeAvro(categoryLinksDF, outCategoryLinks)
      }

      if( fileType.contains( ElementType.PageLink.toString ) ) {
        val inPageLinks = Paths.get(s"${dumpDir}/${src}/pagelinks").toString
        val outPageLinks = Paths.get(s"${outDir}/${src}/pagelinks").toString
        val pageLinksDF = processFileToDf(src, inPageLinks, ElementType.PageLink)

        writeAvro(pageLinksDF, outPageLinks)
      }

      if( fileType.contains( ElementType.LangLink.toString ) ) {
        val inLangLinks = Paths.get(s"${dumpDir}/${src}/langlinks/dumps").toString
        val outLangLinks = Paths.get(s"${outDir}/${src}/langlinks/dumps").toString
        val langLinksDF = processFileToDf(src, inLangLinks, ElementType.LangLink, new ElementFilter[DumpLangLink with Product] {
          override def filterElt(t: DumpLangLink with Product): Boolean = languages.contains(t.lang) && !t.title.contains(":")
        })

        writeAvro(langLinksDF, outLangLinks)
      }
    }

    LangLinkParser.getLangLinksId(outDir, languages: _*)

  }
}

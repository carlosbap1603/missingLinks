package fr.lri.wikipedia.parser

import fr.lri.wikipedia._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


trait ElementFilter[T <: WikiElement with Product] extends Serializable {
  def filterElt(t: T): Boolean
}

sealed class DefaultElementFilter[T <: WikiElement with Product] extends ElementFilter[T] {
  def filterElt(t:T):Boolean = true
}

trait WikiElementParserBase[T <: WikiElement with Product] {
  def parseLine(lineInput:String):List[T]
  def getRDD(lines: RDD[String]): RDD[T]
  def getDataFrame(session:SparkSession, data: RDD[String]):DataFrame
  def defaultFilterElt(t: T): Boolean
  def filterElt(t: T): Boolean
}

abstract class WikiElementParser[T <: WikiElement with Product]( elementFilter: ElementFilter[T] ) extends WikiElementParserBase[T] with Serializable {
  override def filterElt(t:T): Boolean = elementFilter.filterElt(t) && defaultFilterElt(t)
}

class HashPageParser( lang: String, elementFilter: ElementFilter[WikiPage] = new DefaultElementFilter[WikiPage] ) extends WikiElementParser[WikiPage](elementFilter)  {

  val pageRegex = """(\d+)\s+(.*?)\s+(.*?)\s+(.*?)\s?$""".r

  def parseLine(lineInput:String):List[WikiPage] = {
    val r = pageRegex.findAllIn(lineInput).matchData.toList
    r.map( m => WikiPage( s"${m.group(1)}-${lang}".hashCode,m.group(1).toLong, m.group(2), lang ) )
  }

  def getRDD(lines: RDD[String]): RDD[WikiPage] = {
    lines.flatMap(l => parseLine(l)).filter(filterElt)
  }

  def getDataFrame(session:SparkSession, data:RDD[String]):DataFrame = session.createDataFrame(getRDD(data))

  def defaultFilterElt(t: WikiPage): Boolean = true
}

class HashCategoryLinkParser(lang:String, elementFilter: ElementFilter[WikiLink] = new DefaultElementFilter[WikiLink]) extends WikiElementParser[WikiLink](elementFilter)  {

  val linkRegex = """(\d+)\s+(.*?)\s+(\d+)\s+(.*?)\s?$""".r

  def parseLine(lineInput:String):List[WikiLink] = {
    val r = linkRegex.findAllIn(lineInput).matchData.toList
    r.map( m => WikiLink( s"${m.group(1)}-${lang}".hashCode  , s"${m.group(3)}-${lang}".hashCode , lang, lang ) )
  }

  def getRDD(lines: RDD[String]): RDD[WikiLink] = {
    lines.flatMap(l => parseLine(l)).filter(filterElt)
  }

  def getDataFrame(session:SparkSession, data:RDD[String]):DataFrame = session.createDataFrame(getRDD(data))

  def defaultFilterElt(t: WikiLink): Boolean = true
}

class HashPageLinkParser(lang:String, elementFilter: ElementFilter[WikiLink] = new DefaultElementFilter[WikiLink]) extends WikiElementParser[WikiLink](elementFilter)  {

  val linkRegex = """(\d+)\s+(\d+)\s?$""".r

  def parseLine(lineInput:String):List[WikiLink] = {
    val r = linkRegex.findAllIn(lineInput).matchData.toList
    r.map( m => WikiLink( s"${m.group(1)}-${lang}".hashCode, s"${m.group(2)}-${lang}".hashCode  , lang, lang ) )
  }

  def getRDD(lines: RDD[String]): RDD[WikiLink] = {
    lines.flatMap(l => parseLine(l)).filter(filterElt)
  }

  def getDataFrame(session:SparkSession, data:RDD[String]):DataFrame = session.createDataFrame(getRDD(data))

  def defaultFilterElt(t: WikiLink): Boolean = true
}

class HashLangLinkParser(elementFilter: ElementFilter[DumpLangLink] = new DefaultElementFilter[DumpLangLink]) extends WikiElementParser[DumpLangLink](elementFilter)  {

  val linkRegex = """(\d+)\s+(.*?)\s+(.*?)\s+(.*?)\s?$""".r

  def parseLine(lineInput:String):List[DumpLangLink] = {
    val r = linkRegex.findAllIn(lineInput).matchData.toList
    r.map( m => DumpLangLink( s"${m.group(1)}-${m.group(4)}".hashCode , m.group(2), m.group(3), m.group(4) ) )
  }

  def getRDD(lines: RDD[String]): RDD[DumpLangLink] = {
    lines.flatMap(l => parseLine(l)).filter(filterElt)
  }

  def getDataFrame(session:SparkSession, data:RDD[String]):DataFrame = session.createDataFrame(getRDD(data))

  def defaultFilterElt(t: DumpLangLink): Boolean = true
}

class WikiDumpLangLinkParser(elementFilter: ElementFilter[DumpLangLink] = new DefaultElementFilter[DumpLangLink]) extends WikiElementParser[DumpLangLink](elementFilter)  {

  val linkRegex = """(\d+)\s+(.*?)\s+(.*?)\s+(.*?)\s?$""".r

  def parseLine(lineInput:String):List[DumpLangLink] = {
    val r = linkRegex.findAllIn(lineInput).matchData.toList
    r.map( m => DumpLangLink( m.group(1).toLong, m.group(2), m.group(3), m.group(4) ) )
  }

  def getRDD(lines: RDD[String]): RDD[DumpLangLink] = {
    lines.flatMap(l => parseLine(l)).filter(filterElt)
  }

  def getDataFrame(session:SparkSession, data:RDD[String]):DataFrame = session.createDataFrame(getRDD(data))

  def defaultFilterElt(t: DumpLangLink): Boolean = true
}

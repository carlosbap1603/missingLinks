package fr.lri.wikipedia.graph

import java.nio.file.Paths

import fr.lri.wikipedia.{Link, WikiLink, WikiPage}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

class GraphBuilder(val session: SparkSession) extends Serializable  {

  import session.implicits._

  private def getPages(dumpDir: String, category: Boolean = false, lang: String): Dataset[WikiPage] = {

    val pagePath = Paths.get(s"${dumpDir}/${lang}/page", "normal_pages").toString
    val page = session.read.format("avro").load(pagePath).as[WikiPage]

    if (category) {
      val categoryPath = Paths.get(s"${dumpDir}/${lang}/page", "category_pages").toString
      val category = session.read.format("avro").load(categoryPath).as[WikiPage]
      return page.union(category)
    }

    page
  }

  def getPages(dumpDir: String, category: Boolean, lang: String*): Dataset[WikiPage] = {

    var allPages = getPages(dumpDir, category, lang(0))

    for (l <- lang.drop(1)) {
      val pages = getPages(dumpDir, category, l)
      allPages = allPages.union(pages)
    }

    allPages
  }

  private def getPageLinks(dumpDir: String, category: Boolean = false, lang: String): Dataset[WikiLink] = {

    val pageLinkPath = Paths.get(s"${dumpDir}/${lang}", "pagelinks").toString
    val pagelink = session.read.format("avro").load(pageLinkPath).as[WikiLink]

    if (category) {
      val categoryLinkPath = Paths.get(s"${dumpDir}/${lang}", "categorylinks").toString
      val categorylink = session.read.format("avro").load(categoryLinkPath).as[WikiLink]

      return pagelink.union(categorylink)
    }

    pagelink
  }

  def getPageLinks(dumpDir: String, category: Boolean, lang: String*): Dataset[WikiLink] = {
    var allLinks = getPageLinks(dumpDir, category, lang(0))

    for (l <- lang.drop(1)) {
      val links = getPageLinks(dumpDir, category, l)
      allLinks = allLinks.union(links)
    }

    allLinks
  }

  private def getLangLinks(dumpDir: String, lang: String, languages: String*): Dataset[WikiLink] = {

    val langLinksPath = Paths.get(s"${dumpDir}/${lang}/langlinks", "links").toString

    val langLinks = session.read.format("avro").load(langLinksPath).as[WikiLink]
    langLinks.filter(l => languages.contains(l.toLang))
  }

  def getLangLinks(dumpDir: String, lang: String*): Dataset[WikiLink] = {

    val languages = lang
    var allLinks = getLangLinks(dumpDir, lang(0), languages: _*)

    for (l <- lang.drop(1)) {
      val links = getLangLinks(dumpDir, l, languages: _*)
      allLinks = allLinks.union(links)
    }

    allLinks
  }

  private def toVertexRDD(pageDf: Dataset[WikiPage]): RDD[(Long,WikiPage)] = {
    pageDf.rdd.map(p => (p.sid, p))
  }

  private def toEdgeRDD(linkDf: Dataset[WikiLink], wType: String): RDD[Edge[Link]] = {
    linkDf.rdd.map(l => Edge(l.from, l.to, Link(wType, l.fromLang, l.toLang ) ))
  }

  def getValidGraph(pages: Dataset[WikiPage], links: Dataset[WikiLink], wType: String): Graph[WikiPage, Link] = {
    val defaultArticle = WikiPage()
    val graph = Graph(toVertexRDD(pages), toEdgeRDD(links,wType), defaultArticle)

    val validGraph = graph.subgraph(vpred = (id, attr) => attr.title != "DEFAULT")
    validGraph
  }

}

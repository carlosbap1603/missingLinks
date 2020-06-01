package fr.lri.wikipedia.graph

import java.nio.file.Paths

import breeze.linalg.{DenseVector, norm}
import fr.lri.wikipedia.{AvroWriter, ElementType, JaccardVector, Link, Neighborhood, WikiLink, WikiPage}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, Dataset, SparkSession}

class GraphAnalyser(val session: SparkSession) extends Serializable with AvroWriter {

  import session.implicits._

  private val gb = new GraphBuilder(session)

  def mergeMaps(m1: Map[String, Set[Long]], m2: Map[String, Set[Long]]): Map[String, Set[Long]] = {
    var vMap: Map[String, Set[Long]] = Map()

    (m1.keys ++ m2.keys).foreach { l =>
      val ids1 = m1.getOrElse(l, Set())
      val ids2 = m2.getOrElse(l, Set())

      vMap += (l -> (ids1 ++ ids2))
    }

    vMap
  }

  def mapContainsEgo(m1: Map[String, Set[Long]], m2: Set[Long]): Boolean = {

    if (!m1.isEmpty) {
      val ids = m1.values.reduce((a, b) => a ++ b)
      if (ids.size > ids.diff(m2).size) {
        return true
      }
    }

    false
  }

  def getCrossPages(dumpDir: String, lang: String* ) = {
    val crossNetPath = Paths.get(s"${dumpDir}/analysis/crosslinks_${lang.mkString("_")}").toString
    session.read.format("avro").load(crossNetPath).as[WikiPage]
  }

  def executeCrossLinkAnalysis(dumpDir:String, lang: String*)={
    val graph = getCrossNet( dumpDir, lang: _*).persist()

    val pages = removeAnomalies( graph.vertices ).toDF().as[WikiPage]
    writeAvro( pages.toDF(), s"${dumpDir}/analysis/crosslinks_${lang.mkString("_")}" )

    val anomalies = graph.vertices.filter{ case (vid, p) => !p.crossNet.isEmpty && p.crossNet.values.filter( s => s.size > 1 ).size > 0 }.map{ case (vid, p) => p }

    writeAvro( anomalies.toDF(), s"${dumpDir}/analysis/anomalies_${lang.mkString("_")}")
  }

  def removeAnomalies( pages: VertexRDD[WikiPage] ):RDD[WikiPage] ={
    pages.map{ case (vid, p) =>

      if( !p.crossNet.isEmpty && p.crossNet.values.filter( s => s.size > 1 ).size > 0 ){

        val crossNet = p.crossNet.filter{ case (k,v) => v.size == 1 }

        WikiPage( p.sid, p.id, p.title, p.lang, crossNet )
      } else {
        p
      }
    }
  }

  def printAnomalies(dumpDir:String, lang: String*)={

    val anomalyPath = Paths.get(s"${dumpDir}/analysis/anomalies_${lang.mkString("_")}").toString

    val pages:Dataset[WikiPage] = gb.getPages(dumpDir, false, lang: _*)

    var anomalies:Dataset[WikiPage] = null

//    try {
//
//      anomalies = session.read.format("avro").load(anomalyPath).as[WikiPage]
//
//    } catch {
//      case e: AnalysisException => {

        executeCrossLinkAnalysis(dumpDir, lang: _*)
        anomalies = session.read.format("avro").load(anomalyPath).as[WikiPage]

//      }
//    }

    val expandAnomalies = anomalies.flatMap{ p =>
      var error = Seq[(Long,String,String,Long)]()

      val crossNet = p.crossNet.filter{ case (k,v) => v.size > 1 }

      crossNet.values.reduce( (a,b) => a++b ).foreach( id =>
        error = error :+ (p.sid,p.title,p.lang,id)
      )

      error.iterator
    }

    val anomalyTable = expandAnomalies.toDF("from", "from_title", "from_lang", "to")
      .join(
        pages.withColumnRenamed("sid", "to")
          .withColumnRenamed("title", "to_title")
          .withColumnRenamed("lang", "to_lang")
          .select('to, 'to_title, 'to_lang)
        , "to"
      ).select("from", "from_title", "from_lang","to_lang","to", "to_title")

    val count = anomalyTable.count().toInt

    println(s"Anomalies count: ${count}")
    anomalyTable.orderBy('from_lang,'from,'to_lang,'to).show( count, false )
  }

  def getCrossNet(dumpDir: String, lang: String*): Graph[WikiPage, Link] = {

    val pages = gb.getPages(dumpDir, false, lang: _* )
    val langLinks = gb.getLangLinks(dumpDir, lang: _*)

    val crossGraph = gb.getValidGraph(pages, langLinks, ElementType.LangLink.toString)

    def sendMessage(t: EdgeTriplet[WikiPage, Link]): Iterator[(VertexId, Neighborhood)] = {

      val all = if( t.srcAttr.lang == t.attr.fromLang && t.dstAttr.lang == t.attr.toLang  ) {

        val srcSet = t.srcAttr.crossNet.getOrElse(t.dstAttr.lang, Set()) + t.dstId
        val srcMap = t.srcAttr.crossNet + (t.dstAttr.lang -> srcSet)

        val dstSet = t.dstAttr.crossNet.getOrElse(t.srcAttr.lang, Set()) + t.srcId
        val dstMap = t.dstAttr.crossNet + (t.srcAttr.lang -> dstSet)

        mergeMaps(srcMap, dstMap)
      } else { Map[String, Set[Long]]() }

      val msg = Seq(
        (t.srcId, Neighborhood(all)),
        (t.dstId, Neighborhood(all))
      )

      msg.iterator

    }

    def mergeMessage(m1: Neighborhood, m2: Neighborhood) = {
      val vMap = mergeMaps(m1.net, m2.net)
      Neighborhood(vMap)
    }

    def vertexProgram(vertexId: VertexId, vInfo: WikiPage, message: Neighborhood) = {
      WikiPage(vInfo.sid, vInfo.id, vInfo.title, vInfo.lang, message.net)
    }

    val initialMessage = Neighborhood()

    Pregel(crossGraph, initialMessage, 1, activeDirection = EdgeDirection.Both)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)
  }

  def getHomologousNeighborhood( dumpDir:String , step: Int, article: WikiPage, lang: String*  ): RDD[(VertexId, WikiPage)] ={

    //get Homologous Nodes
    val homologousIDs = article.crossNet.values.reduce((a, b) => a ++ b)

    //get the internal neighborhood of the homologous nodes
    val graph = getInternalNet( dumpDir, step, homologousIDs, lang: _*)
    val homologousRDD = graph.vertices.filter(v => homologousIDs.contains(v._1))

    //obtain the candidates of the homologous nodes
    getCandidates(homologousRDD)
  }

  def getCandidatesNeighborhood(  dumpDir:String , step: Int, article: WikiPage, lang: String*   ): RDD[(VertexId, WikiPage)] ={

    //get Homologous Nodes
    val homologousIDs = article.crossNet.values.reduce((a, b) => a ++ b)
    val pages = getCrossPages(dumpDir, lang: _*)
    val homologousRDD = pages.filter( h => homologousIDs.contains( h.sid ) )


    //get All the candidates of all the homologous nodes
    var onlyCandidates = homologousRDD.map{ page => page.candidates.keySet }.reduce((a, b) => a ++ b ).map(_.toLong)

    onlyCandidates = onlyCandidates.take(10)

    //obtain the internal neighborhoods of the candidates
    val graph = getInternalNet( dumpDir, step, onlyCandidates, lang:_*)
    graph.vertices.filter(v => onlyCandidates.contains(v._1))

  }

  def rankCandidates(originalGraph:Graph[WikiPage,Link], homologousRDD: Dataset[WikiPage], candidatesRDD: Dataset[WikiPage] ): RDD[(VertexId, WikiPage)] ={
    val all = homologousRDD.union(candidatesRDD)

    //Obtain the characteristic vector of each of the homologous and candidates
    var vectors = Seq[(Long,Map[Long,Double])]()
    all.collect.foreach{ a =>
      vectors = vectors :+ (a.sid, getMapVector(originalGraph, a.stepNet.getOrElse(a.lang, Set()) ) )
    }

    val vectorsRDD = vectors.toDF("sid","vector")

    val articlePairs = homologousRDD.flatMap{ h =>
      var candidateSet =  Seq[(Long,Long)]()

      h.candidates.keySet.foreach{ c =>
        candidateSet = candidateSet :+ (h.sid, c.toLong )
      }
      candidateSet.iterator
    }.toDF("from", "to")

    val vectorPairs = articlePairs.join( vectorsRDD.withColumnRenamed("sid", "from")
      .withColumnRenamed("vector","fromVector"),
      "from" )
      .join( vectorsRDD.withColumnRenamed("sid", "to")
        .withColumnRenamed("vector","toVector"),
        "to" ).select("from","fromVector","to","toVector").as[JaccardVector]

    val jaccard = vectorPairs.map{ j =>  j.from -> Map( j.to.toString -> getJaccard( j.fromVector, j.toVector ) ) }.rdd
    val reduced = jaccard.reduceByKey( (a, b) => a++b ).toDF("sid", "ranked")

    val result = all.join(reduced, "sid" )
      .select("sid","id","title","lang","crossNet","stepNet","egoNet","ranked", "step").withColumnRenamed("ranked", "candidates").as[WikiPage]

    result.map{ x => (x.sid, x) }.rdd

  }

  def executeInternalLinkAnalysis( dumpDir:String , titleSearch: String , step: Int, lang: String* ) = {

    val pages = getCrossPages(dumpDir, lang: _*).persist()

    val search = pages.filter( 'title === titleSearch )

    if (search.count() > 0) {

      val article = search.first()
      val links = gb.getPageLinks(dumpDir, false, lang: _*).persist()

      var originalGraph = gb.getValidGraph(pages, links, ElementType.PageLink.toString)
      val homologousRDD = getHomologousNeighborhood(dumpDir, step, article, lang: _*)

      originalGraph = originalGraph.joinVertices(homologousRDD)((id, o, u) => WikiPage(o.sid, o.id, o.title, o.lang, u.crossNet, u.stepNet, u.egoNet, u.candidates))
      val result = originalGraph.vertices.map { case (vid, vInfo) => vInfo }.toDF().as[WikiPage]

      val outputPath = s"${dumpDir}/analysis/crosslinks_${lang.mkString("_")}"
      writeAvro(result.toDF(), outputPath)
    }
  }

  def executeCandidateAnalysis( dumpDir:String , titleSearch: String, step: Int, lang: String* ) = {

      val pages = getCrossPages(dumpDir, lang: _*).persist()

      val search = pages.filter( 'title === titleSearch )

      if (search.count() > 0) {

        val article = search.first()

        val links = gb.getPageLinks(dumpDir, false, lang: _*).persist()
        var originalGraph = gb.getValidGraph(pages, links, ElementType.PageLink.toString)

        val candidatesRDD = getCandidatesNeighborhood(dumpDir, step, article, lang: _*)

        //val ranked = rankCandidates(originalGraph, homologousRDD, candidatesRDD)

        originalGraph = originalGraph.joinVertices(candidatesRDD)((id, o, u) => WikiPage(o.sid, o.id, o.title, o.lang, u.crossNet, u.stepNet, u.egoNet, u.candidates))
        val result = originalGraph.vertices.map { case (vid, vInfo) => vInfo }.toDF().as[WikiPage]

        val outputPath = s"${dumpDir}/analysis/crosslinks_${lang.mkString("_")}"
        writeAvro(result.toDF(), outputPath)
      }
  }

  def executeRankAnalysis(dumpDir:String , titleSearch: String, step: Int, lang: String*  ) = {

    val pages = getCrossPages(dumpDir, lang: _*).persist()


    val search = pages.filter( 'title === titleSearch )

    if (search.count() > 0) {

      val article = search.first()

      val links = gb.getPageLinks(dumpDir, false, lang: _*).persist()
      var originalGraph = gb.getValidGraph(pages, links, ElementType.PageLink.toString)

      //get Homologous Nodes
      val homologousIDs = article.crossNet.values.reduce((a, b) => a ++ b)
      val homologousRDD = pages.filter( h =>  homologousIDs.contains( h.sid ) )

      //get All the candidates of all the homologous nodes
      val onlyCandidates = homologousRDD.map{ page => page.candidates.keySet }.reduce((a, b) => a ++ b ).map(_.toLong)
      val candidatesRDD = pages.filter( h =>  onlyCandidates.contains( h.sid ) )

      val ranked = rankCandidates(originalGraph, homologousRDD, candidatesRDD)

      originalGraph = originalGraph.joinVertices(ranked)((id, o, u) => WikiPage(o.sid, o.id, o.title, o.lang, u.crossNet, u.stepNet, u.egoNet, u.candidates))
      val result = originalGraph.vertices.map { case (vid, vInfo) => vInfo }.toDF().as[WikiPage]

      val outputPath = s"${dumpDir}/analysis/crosslinks_${lang.mkString("_")}"
      writeAvro(result.toDF(), outputPath)
    }
  }

  def getCandidates(homologousRDD: VertexRDD[WikiPage]): RDD[(VertexId, WikiPage)] = {
    val stepNeighborhoodUnionRDD = getStepNeighborhoodUnion(homologousRDD)

    homologousRDD.join( stepNeighborhoodUnionRDD ).map {
      case (id, (o, u)) =>

        val candidates = u.diff( o.egoNet )

        var candidateMap = Map[String,Double]()
        candidates.foreach( c =>  candidateMap += (c.toString -> 0.0) )

        (id, WikiPage(o.sid, o.id, o.title, o.lang, o.crossNet, o.stepNet, o.egoNet, candidateMap ) )
    }
  }

  def getStepNeighborhoodUnion(neighborhood: VertexRDD[WikiPage]) = {

    val kStepRdd = neighborhood.flatMap { case (vid, vInfo) =>
      var info = Seq[(Long, Set[Long])]()

      vInfo.crossNet.keys.foreach { l =>
        if (vInfo.lang != l) {
          val set = vInfo.stepNet.getOrElse(l, Set())
          if( !set.isEmpty )
            info = info :+ (vInfo.crossNet(l).last, set )
        }
      }

      info.iterator
    }.reduceByKey(_ ++ _)

    kStepRdd
  }


  def printCandidates( dumpDir:String, titleSearch:String, step: Int, lang: String* ) = {

    val pages = getCrossPages(dumpDir, lang: _*)

    val search = pages.filter( 'title === titleSearch )

    if (search.count() > 0) {

      val article = search.first()

//      //if (article.candidates.isEmpty || article.step < step) {
//
//        executeInternalLinkAnalysis(dumpDir, step, article, lang: _*)
//        pages = getCrossPages(dumpDir, lang: _*)
//
//      //}

      val homologous = article.crossNet.values.reduce((a, b) => a ++ b)
      val candidates = pages.filter( h => homologous.contains( h.sid ) ).flatMap { h =>

        var info = Seq[(Long, Long, Double)]()

        h.candidates.foreach{ c =>
          info = info :+ (h.sid, c._1.toLong, c._2 )
        }

        info.iterator
      }.toDF("from", "to", "jaccard")

      val from = pages.withColumnRenamed("sid", "from")
      .withColumnRenamed("title","from_title")
      .select('from, 'from_title, 'lang)

      val to = pages.withColumnRenamed("sid", "to")
      .withColumnRenamed("title","to_title")
      .select('to, 'to_title)

      val result = candidates.join( from, "from").join( to, "to").select('from, 'from_title, 'to,'to_title, 'jaccard, 'lang )
      val count = candidates.count().toInt

      println(s"Candidate recommendations: ${count}")
      result.orderBy('lang,'from, 'jaccard).show(count, false)
    }
  }

  def getInternalNet(dumpDir: String, steps: Int, ego: Set[Long], lang: String *): Graph[WikiPage, Link] = {

    val pages = getCrossPages(dumpDir, lang: _*)
    val links = gb.getPageLinks(dumpDir, false, lang: _* )

    val internalGraph = gb.getValidGraph(pages, links, ElementType.PageLink.toString )

    def sendMessage(t: EdgeTriplet[WikiPage, Link]): Iterator[(VertexId, Neighborhood)] = {

      val all = if (ego.contains(t.srcId) || mapContainsEgo(t.srcAttr.stepNet, ego)) {

        val srcMap = if (t.srcAttr.stepNet.isEmpty) t.dstAttr.crossNet else t.dstAttr.stepNet
        val dstMap = if (t.dstAttr.stepNet.isEmpty) t.srcAttr.crossNet else t.srcAttr.stepNet
        val srcSet = t.srcAttr.egoNet + t.srcId + t.dstId

        ( mergeMaps(srcMap, dstMap), srcSet )

      } else ( Map[String, Set[Long]](), Set[Long]())

      val msg = Seq(
        (t.srcId, Neighborhood(all._1, all._2 )),
        (t.dstId, Neighborhood(all._1, all._2))
      )

      msg.iterator

    }

    def mergeMessage(m1: Neighborhood, m2: Neighborhood) = {
      val vMap = mergeMaps(m1.net, m2.net)
      Neighborhood(vMap, m1.set ++ m2.set )
    }

    def vertexProgram(vertexId: VertexId, vInfo: WikiPage, message: Neighborhood) = {
      WikiPage(vInfo.sid, vInfo.id, vInfo.title, vInfo.lang, vInfo.crossNet, message.net, message.set)
    }

    val initialMessage = Neighborhood()

    Pregel(internalGraph, initialMessage, steps * 2, activeDirection = EdgeDirection.Out)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)
  }

  def getMapVector( originalGraph:Graph[WikiPage, Link], neighborhood: Set[Long] ) ={

    val map = if( !neighborhood.isEmpty ) {

      val neighborhoodGraph = originalGraph.subgraph(vpred = (id, attr) => neighborhood.contains(id))
      val pRankGraph = neighborhoodGraph.pageRank(0.001)

      pRankGraph.vertices.map { case (id, rank) =>
        Map(id -> rank)
      }.reduce((a, b) => a ++ b)
    }else{ Map[Long,Double]() }

    map
  }

  def getJaccard(m1: Map[Long, Double], m2: Map[Long, Double]): Double = {

    val keys = m1.keys ++ m2.keys
    val vector1 = DenseVector.zeros[Double](keys.size)
    val vector2 = DenseVector.zeros[Double](keys.size)

    var i = 0
    keys.foreach{ l =>
      vector1(i) = m1.getOrElse(l, 0.0)
      vector2(i) = m2.getOrElse(l, 0.0)
      i = i+1
    }

    val dotProduct = (vector1.t * vector2)
    val relatedness = dotProduct / ( math.pow( norm( vector1 ),2 ) + math.pow( norm( vector2 ),2 ) - dotProduct )

    relatedness
  }

}

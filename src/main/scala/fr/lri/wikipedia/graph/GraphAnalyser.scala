package fr.lri.wikipedia.graph

import java.nio.file.Paths

import breeze.linalg.{DenseVector, norm}
import fr.lri.wikipedia.{AvroWriter, CsvWriter, EgoNet, ElementType, JaccardVector, LangEgoNet, Link, Neighborhood, WikiLink, WikiPage}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{last, _}
import org.apache.spark.sql.expressions.Window

class GraphAnalyser(val session: SparkSession) extends Serializable with AvroWriter with CsvWriter{

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

    val pages = removeAnomalies( graph.vertices )
    writeAvro( pages.toDF(), s"${dumpDir}/analysis/crosslinks_${lang.mkString("_")}" )

    val anomalies = graph.vertices.filter{ case (vid, p) => !p.crossNet.isEmpty && p.crossNet.values.filter( s => s.size > 1 ).size > 0 }.map{ case (vid, p) => p }
    writeAvro( anomalies.toDF(), s"${dumpDir}/analysis/anomalies_${lang.mkString("_")}")
  }

//  def executeCrossLinkAnalysis(dumpDir:String, lang: String*)={
//    val graph = getCrossNet( dumpDir, lang: _*).persist()
//    var pages = graph.vertices.map{ case (sid, page) => page }.toDF().as[WikiPage]
//
//    writeAvro( pages.toDF(), s"${dumpDir}/analysis/crosslinks_${lang.mkString("_")}" )
//
//    val anomalies = graph.vertices.filter{ case (vid, p) => !p.crossNet.isEmpty && p.crossNet.values.filter( s => s.size > 1 ).size > 0 }.map{ case (vid, p) => p }
//    writeAvro( anomalies.toDF(), s"${dumpDir}/analysis/anomalies_${lang.mkString("_")}")
//
//
//    pages = removeAnomalies( dumpDir, lang: _* )
//    writeAvro( pages.toDF(), s"${dumpDir}/analysis/crosslinks_${lang.mkString("_")}" )
//
//  }

  def removeAnomalies( pages: VertexRDD[WikiPage] ): Dataset[WikiPage] ={

    val fixedAnomalies = pages.filter{ case (vid, p) => !p.crossNet.isEmpty && p.crossNet.values.filter( s => s.size > 1 ).size > 0 }
      .mapValues{ p =>
        val crossNet = p.crossNet.filter{ case (k,v) => v.size == 1 }
        WikiPage( p.sid, p.id, p.title, p.lang, crossNet )
      }

    val notAnomalies = pages.filter{ case (vid, p) => !p.crossNet.isEmpty && p.crossNet.values.filter( s => s.size > 1 ).size == 0 }

    fixedAnomalies.union(notAnomalies).map{ case (vid, p) => p}.toDF().as[WikiPage]
  }

//  def removeAnomalies(dumpDir:String, lang: String* ): Dataset[WikiPage] ={
//
//    val anomalyPath = Paths.get(s"${dumpDir}/analysis/anomalies_${lang.mkString("_")}").toString
//    val anomalies = session.read.format("avro").load(anomalyPath).as[WikiPage].persist()
//
//    anomalies.show()
//
//    //get Homologous Nodes
//    val srcIDs = anomalies.map{ p => p.sid }.collect.toSet
//    val dstIDs = anomalies.map{ p => p.crossNet.filter{ case (k,v) => v.size > 1 }
//                                                                         .map{ case (k,v) => v }
//                                                                         .reduce( (a,b) => a++b )
//                                }.reduce( (a,b) => a++b )
//
//    val srcRDD = getInternalNet(dumpDir, 1, srcIDs, lang: _*).map { case (vid, vInfo) => vInfo }.toDF().as[WikiPage]
//
//    val dstRDD = getInternalNet(dumpDir, 1, dstIDs, lang: _*).map { case (vid, vInfo) => vInfo }.toDF().as[WikiPage]
//
//    val toFix = srcRDD.map{ p => ( p.sid, p.vector, p.crossNet.filter{ case (k,v) => v.size > 1 }) }.toDF("from", "fromVector", "anomalies")
//
//    val toFixExploded = toFix.select('from, 'fromVector, explode('anomalies))
//                              .select('from, 'fromVector, explode('value))
//                              .withColumnRenamed("col", "to")
//
//    toFixExploded.show()
//
//    val vectorPairs = toFixExploded.join( dstRDD.select('sid,'vector)
//                                                .withColumnRenamed("sid", "to")
//                                                .withColumnRenamed("vector", "toVector"), Seq( "to" ))
//                                                .select('from,'fromVector,'to,'toVector)
//
//    vectorPairs.show(false)
//
//    val pages = getCrossPages(dumpDir, lang: _*)
//
//    pages
  //
//    val fixedAnomalies = pages.filter{ p => !p.crossNet.isEmpty && p.crossNet.values.filter( s => s.size > 1 ).size > 0 }
//                          .map{ p =>
//                            val crossNet = p.crossNet.filter{ case (k,v) => v.size == 1 }
//                            WikiPage( p.sid, p.id, p.title, p.lang, crossNet )
//                          }
//
//    val notAnomalies = pages.filter{ p => !p.crossNet.isEmpty && p.crossNet.values.filter( s => s.size > 1 ).size == 0 }
//
//    fixedAnomalies.union(notAnomalies).map{ p => p}.toDF().as[WikiPage]
//  }

  def printAnomalies(dumpDir:String, lang: String*)={

    val anomalyPath = Paths.get(s"${dumpDir}/analysis/anomalies_${lang.mkString("_")}").toString

    var pages:Dataset[WikiPage] = null
    var anomalies:Dataset[WikiPage] = null

    try {

      anomalies = session.read.format("avro").load(anomalyPath).as[WikiPage]
      pages = getCrossPages(dumpDir, lang: _*)

    } catch {
      case e: AnalysisException => {

        executeCrossLinkAnalysis(dumpDir, lang: _*)
        anomalies = session.read.format("avro").load(anomalyPath).as[WikiPage]
        pages = getCrossPages(dumpDir, lang: _*)

      }
    }

    val expandAnomalies = anomalies.flatMap{ p =>
      var error = Seq[(Long,String,String,Long)]()

      val crossNet = p.crossNet.filter{ case (k,v) => v.size > 1 }

      crossNet.values.reduce( (a,b) => a++b ).foreach( id =>
        error = error :+ (p.sid,p.title,p.lang,id)
      )

      error.iterator
    }.toDF("from", "from_title", "from_lang", "to")

    val anomalyTable = expandAnomalies.join(
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
      val vMap = mergeMaps(m1.kNet, m2.kNet)
      Neighborhood(vMap)
    }

    def vertexProgram(vertexId: VertexId, vInfo: WikiPage, message: Neighborhood) = {
      WikiPage(vInfo.sid, vInfo.id, vInfo.title, vInfo.lang, message.kNet)
    }

    val initialMessage = Neighborhood()

    Pregel(crossGraph, initialMessage, 1, activeDirection = EdgeDirection.Both)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)
  }

  def rankCandidates( homologousRDD: Dataset[WikiPage], candidatesRDD: Dataset[WikiPage] ): RDD[(VertexId, WikiPage)] ={
    val all = homologousRDD.union(candidatesRDD)

    var homologousExploded = homologousRDD.select('sid,'vector, explode('candidates))
                                    .withColumnRenamed("sid","from")
                                    .withColumnRenamed("vector","fromVector")

    homologousExploded = homologousExploded.withColumnRenamed("key","to")


    val vectorPairs = homologousExploded.withColumn("to", $"to" cast "Long")
                                        .join( candidatesRDD.withColumnRenamed("sid", "to")
                                         .withColumnRenamed("vector", "toVector"), "to" )
                                  .select("from","fromVector","to","toVector").as[JaccardVector]

    val jaccard = vectorPairs.map{ j =>  j.from -> Map( j.to -> getJaccard( j.fromVector, j.toVector ) ) }.rdd
    val reduced = jaccard.reduceByKey( (a, b) => a++b ).toDF("sid", "rankedCandidates")

    val result = all.join(reduced, "sid" )
      .select("sid","id","title","lang","crossNet","stepNet","egoNet","vector","rankedCandidates", "step","ranked").withColumnRenamed("rankedCandidates", "candidates").as[WikiPage]

    result.map{ x => (x.sid, x) }.rdd
  }

  def executeInternalLinkAnalysis( dumpDir:String , titleSearch: String , step: Int, lang: String* ) = {

    val pages = getCrossPages(dumpDir, lang: _*).persist()
    val search = pages.filter( 'title === titleSearch && 'lang === "en")

    if (search.count() > 0) {

      val article = search.first()

      if( article.candidates.isEmpty || article.step < step ) {

        val links = gb.getPageLinks(dumpDir, false, lang: _*).persist()
        var originalGraph = gb.getValidGraph(pages, links, ElementType.PageLink.toString).persist()

        //get Homologous Nodes
        val homologousIDs = article.crossNet.values.reduce((a, b) => a ++ b)
        val homologousRDD = getInternalNet(dumpDir, step,homologousIDs, lang:_* )

        originalGraph = originalGraph.joinVertices(homologousRDD)((id, o, u) => WikiPage(o.sid, o.id, o.title, o.lang, o.crossNet, u.stepNet, u.egoNet, u.vector,u.candidates, step, u.ranked ))
        val result = originalGraph.vertices.map { case (vid, vInfo) => vInfo }.toDF().as[WikiPage]

        val outputPath = s"${dumpDir}/analysis/crosslinks_${lang.mkString("_")}"
        writeAvro(result.toDF(), outputPath)

      }

      printCandidates(dumpDir, titleSearch, false, lang: _*)
    }
  }

  def executeRankAnalysis(dumpDir:String , titleSearch: String, step: Int, lang: String*  ) = {

    val pages = getCrossPages(dumpDir, lang: _*).persist()
    val search = pages.filter( 'title === titleSearch && 'lang === "en")

    if (search.count() > 0) {

      val article = search.first()

      val links = gb.getPageLinks(dumpDir, false, lang: _*).persist()
      var originalGraph = gb.getValidGraph(pages, links, ElementType.PageLink.toString)

      //get Homologous Nodes
      val homologousIDs = article.crossNet.values.reduce((a, b) => a ++ b)
      val homologousRDD = pages.filter( h =>  homologousIDs.contains( h.sid ) )

      //get All the candidates of all the homologous nodes
      val onlyCandidates = homologousRDD.map{ page => page.candidates.keySet }.reduce((a, b) => a ++ b ).map(_.toLong)

      if( !article.ranked ) {

        //obtain the internal neighborhoods of the candidates
        val candidatesRDD = getInternalNet(dumpDir, step, onlyCandidates, lang: _*).map { case (vid, vInfo) => vInfo }.toDF().as[WikiPage]
        val ranked = rankCandidates( homologousRDD, candidatesRDD)

        originalGraph = originalGraph.joinVertices(ranked)((id, o, u) => WikiPage(o.sid, o.id, o.title, o.lang, o.crossNet, o.stepNet, o.egoNet, o.vector, u.candidates, o.step, true))
        val result = originalGraph.vertices.map { case (vid, vInfo) => vInfo }.toDF().as[WikiPage]

        val outputPath = s"${dumpDir}/analysis/crosslinks_${lang.mkString("_")}"
        writeAvro(result.toDF(), outputPath)
      }
    }

    printCandidates(dumpDir, titleSearch, true, lang: _*)
  }

  def getCandidates(homologousRDD: RDD[(Long,WikiPage)]): RDD[(VertexId, WikiPage)] = {
    val stepNeighborhoodUnionRDD = getStepNeighborhoodUnion(homologousRDD)

    homologousRDD.join( stepNeighborhoodUnionRDD ).map {
      case (id, (o, u)) =>

        val candidates = u.diff( o.egoNet )

        var candidateMap = Map[String,Double]()
        candidates.foreach( c =>  candidateMap += (c.toString -> 0.0) )

        (id, WikiPage(o.sid, o.id, o.title, o.lang, o.crossNet, o.stepNet, o.egoNet, o.vector, candidateMap ) )
    }
  }

  def getStepNeighborhoodUnion(neighborhood: RDD[(Long,WikiPage)]) = {

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


  def printCandidates( dumpDir:String, titleSearch:String, ranked: Boolean,lang: String* ) = {

    val pages = getCrossPages(dumpDir, lang: _*)

    val search = pages.filter( 'title === titleSearch )

    if (search.count() > 0) {

      val article = search.first()

      val homologous = article.crossNet.values.reduce((a, b) => a ++ b)

      val candidates = pages.filter( h => homologous.contains( h.sid ) )
                      .select('sid, explode('candidates))
                      .toDF("from", "to", "jaccard")

      val from = pages.withColumnRenamed("sid", "from")
      .withColumnRenamed("title","from_title")
      .select('from, 'from_title, 'lang)

      val to = pages.withColumnRenamed("sid", "to")
      .withColumnRenamed("title","to_title")
      .select('to, 'to_title)

      if( ranked ) {

        val result = candidates.join( from, "from")
                                .join( to, "to")
                                .select('from, 'from_title, 'to,'to_title, 'jaccard, 'lang )

        val windowSpec = Window.partitionBy('lang).orderBy('jaccard.desc)

        val k = 10

        val topK = result.withColumn("rank", row_number().over(windowSpec)).filter('rank <= k)
        val nonTopK = result.withColumn("rank", row_number().over(windowSpec)).filter('rank > k)

        var stats = result.groupBy('from, 'from_title, 'lang).agg(max('jaccard), min('jaccard), avg('jaccard), stddev('jaccard),skewness('jaccard))

        val avgTopK = topK.groupBy('from, 'from_title, 'lang)
          .avg("jaccard")
          .withColumnRenamed("avg(jaccard)", s"avg top ${k}(jaccard)")

        val avgNonTopK = nonTopK.groupBy('from, 'from_title, 'lang)
          .avg("jaccard")
          .withColumnRenamed("avg(jaccard)", s"avg non top ${k}(jaccard)")

        println(s"Candidate ranking for ${titleSearch} in languages '${lang.mkString("_")}'")
        topK.show(k, false)
        writeCsv(topK.toDF(), s"${dumpDir}/analysis/articles/rank_top_${k}_${titleSearch}_${lang.mkString("_")}", coalesce = true )

        stats = stats.join( avgTopK, Seq("from", "from_title", "lang" ), "left")
        stats = stats.join( avgNonTopK, Seq("from", "from_title", "lang" ), "left")

        println(s"Candidate ranking stats ${titleSearch} in languages '${lang.mkString("_")}'")
        stats.show(10, false)
        writeCsv(stats.toDF(), s"${dumpDir}/analysis/articles/rank_stats_${titleSearch}_${lang.mkString("_")}", coalesce = true )

      } else {

        val result = candidates.join(from, "from")
          .join(to, "to")
          .select('from, 'from_title, 'to, 'to_title, 'lang)

        val table = result.orderBy('lang, 'from, 'to)

        val count = table.count().toInt

        println(s"Candidate recommendations for ${titleSearch} in languages '${lang.mkString("_")}': ${count}")
        table.show(count, false)
        writeCsv(table.toDF(), s"${dumpDir}/analysis/articles/recommendation_${titleSearch}_${lang.mkString("_")}", coalesce = true)

      }
    }
  }

  def getMapVector( originalGraph:Graph[WikiPage,Link], neighborhood: Set[Long] ):Map[String,Double]={

    val map = if( !neighborhood.isEmpty ) {

      val neighborhoodGraph = originalGraph.subgraph(vpred = (id,_) => neighborhood.contains(id))
      val pRankGraph = neighborhoodGraph.pageRank(0.01)

      pRankGraph.vertices.map { case (id, rank) =>
        Map(id.toString -> (1/rank) )
      }.reduce((a, b) => a ++ b)
    }else{ Map[String,Double]() }

    map
  }

  def getJaccard(m1: Map[String, Double], m2: Map[String, Double]): Double = {

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


  def getInternalNet(dumpDir: String, step: Int, egos: Set[Long], lang: String *): RDD[(VertexId, WikiPage)] ={

    val pages = getCrossPages(dumpDir, lang: _*).persist()
    val links = gb.getPageLinks(dumpDir, false, lang: _*)
    val graph = gb.getValidGraph(pages, links, ElementType.PageLink.toString).persist()

    //get the internal neighborhood for each ego node
    val stepNet = getKStepNeighborhood(graph, step, egos)

    //given the neighborhood of each ego node, calculate the representative vector
    var vectors = Seq[(Long,Map[String,Double])]()
    stepNet.collect.foreach{ page => vectors = vectors :+ ( (page.ego, getMapVector(graph, page.egoNet)) ) }

    val vectorsRDD = vectors.toDF("ego","vector")

    //given the neighborhood of each ego node, calculate the translated neighborhoods
    val langStepNet = getTranslatatedNeighborhood( pages, stepNet ).persist()

    //get the 1-step neighborhood for each ego node
    val egoNet = if(step == 1 ) stepNet else getKStepNeighborhood(graph, 1, egos)

    val filteredRDD = pages.filter(page => egos.contains(page.sid) ).select('sid, 'id,'title,'lang,'crossNet,'candidates,'step,'ranked)
    val joinedNet = langStepNet.join(egoNet,Seq("ego")).join( vectorsRDD, Seq("ego")).withColumnRenamed("ego","sid")

    val egosRDD = filteredRDD.join(joinedNet,Seq("sid")).as[WikiPage].rdd.map( r => (r.sid, r) )
    //obtain the candidates of the ego nodes
    getCandidates(egosRDD)
  }


  def getKStepNeighborhood( originalGraph:Graph[WikiPage,Link] , step: Int, homologousIDs:Set[Long] ):Dataset[EgoNet] ={

    //get the ego neighborhood of all the nodes
    val egoNet = originalGraph.collectNeighborIds(EdgeDirection.Out)
    //filter the homologous
    var previousLayer = egoNet.filter{ case (id,n) => homologousIDs.contains(id) }.toDF("ego","egoNet").as[EgoNet]
    var count = 1

    //expand the layers as needed by the step
    while( count < step ) {
      val explodedLayer = previousLayer.withColumnRenamed("egoNet", "to").select('ego, explode('to)).withColumnRenamed("col", "to")
      val explodedNextLayer = explodedLayer.join(egoNet.toDF("to", "egoNet"), "to").select('ego, 'egoNet).as[EgoNet]
      val homoNet = previousLayer.union(explodedNextLayer).map(n => (n.ego, n.egoNet)).rdd.reduceByKey((a, b) => a ++ b).toDF("ego", "egoNet").as[EgoNet]

      count += 1
      previousLayer = homoNet
    }

    previousLayer
  }

  def getTranslatatedNeighborhood(pages:Dataset[WikiPage], net: Dataset[EgoNet] ):Dataset[LangEgoNet]={

    val crossNet = pages.select('sid,'crossNet)
    val explodedNet = net.select('ego,explode('egoNet)).withColumnRenamed("col","sid")
    val explodedTranslation = explodedNet.join(crossNet, Seq("sid")).withColumnRenamed("crossNet", "stepNet").select("ego","stepNet").as[LangEgoNet]
    explodedTranslation.map( n => (n.ego, n.stepNet)).rdd.reduceByKey((a, b) => mergeMaps(a,b)).toDF("ego", "stepNet").as[LangEgoNet]

  }

}

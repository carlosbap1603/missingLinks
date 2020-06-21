package fr.lri.wikipedia

object ElementType extends Enumeration {
  val Page = Value("normalpage")
  val Category = Value("categorypage")
  val PageLink = Value("interlink")
  val LangLink = Value("crosslink")
  val CategoryLink = Value("categorylink")
  val RawLangLink = Value("rawlanglink")
  val CrossNet = Value("crossNet")
}

abstract class WikiElement extends Serializable

//==================== Classes for graph usage ===========================

case class Link( wType: String, fromLang: String, toLang: String )
case class Neighborhood(kNet: Map[String, Set[Long]] = Map(), oneNet: Set[Long] = Set() ) extends WikiElement
case class JaccardPair(from:Long, to:Long, coefficient:Double ) extends WikiElement
case class JaccardVector(from:Long, fromVector:Map[Long,Double], to:Long, toVector:Map[Long,Double] ) extends WikiElement
case class EgoNet(ego:Long, egoNet:Set[Long]) extends WikiElement
case class LangEgoNet(ego:Long, stepNet:Map[String, Set[Long]]) extends WikiElement


//==================== Classes for dumps ===========================

case class WikiPage(sid:Long = -1,
                    id:Long = -1,
                    title:String = "DEFAULT",
                    lang:String = "DEFAULT",
                    crossNet: Map[String, Set[Long]] = Map(),
                    stepNet: Map[String, Set[Long]] = Map(),
                    egoNet: Set[Long] = Set(),
                    candidates: Map[String, Double] = Map(),
                    step: Int = 0,
                    ranked: Boolean = false
                   ) extends WikiElement

case class WikiLink(from:Long, to:Long, fromLang:String, toLang:String ) extends WikiElement

case class DumpLangLink(sid:Long, toLang:String, title:String, lang:String) extends WikiElement




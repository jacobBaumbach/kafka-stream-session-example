package kstreamex

object Transformations {
  def buildPath(sessionWindow: Long)(k: String, v: String, agg: String): String =
    Path.fromString(sessionWindow, agg).addSite(k, v).toString

  def fst(k: String, v: String): String =
    k
  
  def parse(k: String, v: String): (String, String) = {
    val arr = k.split("\t")
    val key = arr.headOption.getOrElse("").toLowerCase
    val time = arr.lift(1).getOrElse("") 
    val page = arr.lift(2).getOrElse("").toLowerCase
    (key, time ++"~"++ page)
  }
}

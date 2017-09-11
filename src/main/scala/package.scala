package kstreamex

case class Path(sessionWindow: Long,
  id: String = "",
  start: Option[Long] = None,
  end: Option[Long] = None,
  path: String = "") {
  override def toString: String =
    List(sessionWindow,
         id,
         start.getOrElse(""),
         end.getOrElse(""),
         path).mkString("~")

  def addSite(k: String, v: String): Path ={
    val arr = v.split("~")
    val v1 = arr.lift(0).map(_.toLong)
    val v2 = arr.lift(1)
    (start, end) match{
      case (start1@Some(_), Some(e)) if(v1.map(j => j-e < sessionWindow).getOrElse(false)) =>
        Path(sessionWindow, k, start1, v1, path ++ "|" ++ v2.getOrElse(""))
      case _ => Path(sessionWindow, k, v1, v1, v2.getOrElse(""))
    }
  }
    
}

object Path {
  def fromString(sessionWindow: Long, str: String): Path = {
    val arr = str.split("~")
    val oPath =
      for{
        id <- arr.lift(1)
        start <- arr.lift(2)
        startL = start.toLong
        end <- arr.lift(3)
        endL = end.toLong
        path <- arr.lift(4)
      }yield Path(arr.head.toLong, id, Some(startL), Some(endL), path)
    oPath.getOrElse(Path(sessionWindow))
  }
}

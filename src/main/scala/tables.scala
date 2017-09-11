package kstreamex

import Implicits._
import Transformations._

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, KTable, Reducer}

object Tables {
  def getPath(sessionWindow: Long, inputTopic: String, outputTopic: String,
    builder: KStreamBuilder = new KStreamBuilder()): KStreamBuilder = {

    val stringSerdes: Serde[String] = Serdes.String()
    val data = builder.stream(stringSerdes, stringSerdes, inputTopic)

    val pathBuilder: KTable[String, String] =
      data.map[String, String](parse _)
          .groupBy(fst, stringSerdes, stringSerdes)
          .aggregate(Path(sessionWindow).toString, buildPath(sessionWindow) _, stringSerdes, "pathBuilder")

    pathBuilder.print(stringSerdes, stringSerdes)
    pathBuilder.to(stringSerdes, stringSerdes, outputTopic)
    builder
  }

  def getLast(inputTopic: String, outputTopic: String,
    builder: KStreamBuilder = new KStreamBuilder()): KStreamBuilder = {
    getPath(1L, inputTopic, outputTopic, builder)
  }
}

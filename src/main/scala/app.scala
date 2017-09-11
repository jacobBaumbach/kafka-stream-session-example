package kstreamex

import Tables.getLast

import java.util.Properties

import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object App{
  def main(args: Array[String]): Unit = {
    val bootstrapServers = args.headOption.getOrElse("localhost:9092")
    val inputTopic = args.lift(1).getOrElse("input")
    val outputTopic = args.lift(2).getOrElse("output")
    val streamConfig = new Properties()

    streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "table-ex")
    streamConfig.put(StreamsConfig.CLIENT_ID_CONFIG, "table-client")
    streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    val builder = getLast(inputTopic, outputTopic)

    val stream = new KafkaStreams(builder, streamConfig)
    stream.start()
  }
}

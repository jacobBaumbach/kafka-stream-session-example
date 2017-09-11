package kstreamex
package test

import Tables.getPath

import net.manub.embeddedkafka.Codecs._
import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.streams._

import org.scalatest._
import org.scalatest.compatible.Assertion

class GetPathSpec extends WordSpec with Matchers with EmbeddedKafkaStreamsAllInOne {
  val messages = Seq(
    "1a\t1\tpage0",
    "1b\t1\tpage0",
    "1a\t2\tpage1",
    "1b\t2\tpage1",
    "1a\t3\tpage2",
    "1b\t3\tpage2",
    "1a\t4\tpage3",
    "1b\t4\tpage3",
    "1a\t5\tpage4"
  )

  def testGen(listAssertions: Seq[(Seq[(String, String)] => Assertion)],
    messages: Seq[String] = messages,
    sessionWindow: Long = 10L,
    inputTopic: String = "input",
    outputTopic: String = "outputTopic") = {

    val builder = getPath(sessionWindow, inputTopic, outputTopic)
    runStreams(Seq(inputTopic, outputTopic),builder){
        messages.foreach(message =>
          publishToKafka(topic = inputTopic, key = message, message = message))
        
        withConsumer[String, String, Unit]{ consumer =>
          val a: Stream[(String, String)] = consumer.consumeLazily(outputTopic)
          listAssertions.map(_(a))
        }
    }
  }
  def filt(fKey: String, idx: Int, eq: String): Seq[(String, String)] => Assertion = {
    a => {
      val b = a.filter(_._1 == fKey)
      b.head._2.split("~")(idx) shouldEqual eq
    }
  }

  "getPath KTable" should {
    "key should be the user id" in {
      val f: Seq[(String, String)] => Assertion = a=> {
        val b = a.map(_._1)
        b.contains("1a") shouldEqual true
        b.contains("1b") shouldEqual true
      }
        
      testGen(Seq(f))      
    }

    "messages should contain the full path for the session" in {
      testGen(Seq(filt("1a", 4, "page0|page1|page2|page3|page4<-null"), filt("1b", 4, "page0|page1|page2|page3<-null")))
    }

    "messages should have the start time" in {
      testGen(Seq(filt("1a", 2, "1"), filt("1b", 2, "1")))      
    }

    "messages should have the end time of the path" in {
      testGen(Seq(filt("1a", 3, "5"), filt("1b", 3, "4")))      
    }

    "should accurately track session window" in {
      val message1 = Seq(
        "1a\t1\tpage0",
        "1a\t2\tpage1",
        "1a\t3\tpage2",
        "1a\t15\tpage0",
        "1a\t17\tpage3",
        "1a\t20\tpage4"
      )
      testGen(Seq(filt("1a", 4, "page0|page3|page4<-null"), filt("1a", 2, "15"), filt("1a", 3, "20")), message1)
    }
  }
}

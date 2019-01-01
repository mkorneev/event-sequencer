package com.mkorneev.bz

import java.util

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.prop.TableFor2
import org.scalatest.{FunSuite, Matchers}
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}

class EventSequencerAppTest extends FunSuite with Matchers {
  implicit val system: ActorSystem = ActorSystem("EventSequencerApp")
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  test("testEventFlow") {
    val examples_stream = getClass.getResourceAsStream("/test_examples.yml")
    val yml = new Yaml().load(examples_stream).asInstanceOf[java.util.Map[String, util.List[util.HashMap[String, String]]]]
    val tuples = yml.get("examples").toList.map(e => (e("input"), e("output").stripLineEnd))

    val examples = new TableFor2[String, String](heading = ("input", "expectedOutput")) ++ tuples

    val flowUnderTest = EventSequencerApp.buildEventFlow(60)

    forAll (examples) { (input: String, expectedOutput: String) =>
      val source = Source.single(ByteString(input))
      val future = source.via(flowUnderTest).map(s => s.utf8String.stripLineEnd).runWith(Sink.seq)
      val result = Await.result(future, 3.seconds)
      result should equal (expectedOutput.split("\n"))
    }
  }

}

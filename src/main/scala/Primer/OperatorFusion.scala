package Primer

import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}

object OperatorFusion extends App {
  implicit val actorSystem = ActorSystem("OperatorFusion")
  implicit val actorMaterializer = ActorMaterializer()
  val simpleSource = Source(1 to 1000)
  val simpleFlow = Flow[Int].map(_ + 1)
  val simpleFlow2 = Flow[Int].map(_ * 10)
  val simpleSink = Sink.foreach[Int](println)

  // This runs on the same ACTOR
  //simpleSource.via(simpleFlow).via(simpleFlow2).to(simpleSink).run()

  // operator/component FUSION

  // "Equivalent" behaviour below by default:
  //  class SimpleActor extends Actor {
  //    override def receive: Receive = {
  //      case x: Int =>
  //        //flow operations
  //        val x2 = x + 1
  //        val y = x2 * 10
  //        //sink operation
  //        println(y)
  //    }
  //  }
  //  val simpleActor = actorSystem.actorOf(Props[SimpleActor])
  //  (1 to 1000).foreach(simpleActor ! _)
  // this is good if flows are quick

  val complexFlow = Flow[Int].map { x =>
    // Simulating a long computation
    Thread.sleep(1000)
    x + 1
  }
  val complexFlow2 = Flow[Int].map { x =>
    // Simulating a long computation
    Thread.sleep(1000)
    x * 10
  }

  //simpleSource.via(complexFlow).via(complexFlow2).to(simpleSink).run()
  // there will be a time gap of 2 seconds between processing of 2 elements from the stream because all operations
  //  are happening on the same actor.

  // Async boundary
  //  simpleSource.via(complexFlow).async // runs on one actor
  //    .via(complexFlow2).async // runs on another actor
  //    .to(simpleSink) // runs on third actor
  //    .run
  // now gap will be reduced to 1 second and we have increased throughput by creating async boundaries
  // it breaks operator Fusion which akka streams enable by default

  // ordering guarantees
  Source(1 to 3)
    .map(element => {
      println(s"Flow A: $element")
      element
    }).async
    .map(element => {
      println(s"Flow B: $element")
      element
    }).async
    .map(element => {
      println(s"Flow C: $element")
      element
    }).async
    .runWith(Sink.ignore)
}

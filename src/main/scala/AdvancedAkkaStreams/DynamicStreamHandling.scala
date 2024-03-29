package AdvancedAkkaStreams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{BroadcastHub, Keep, MergeHub, Sink, Source}
import akka.stream.{ActorMaterializer, FlowShape, Graph, KillSwitches, UniqueKillSwitch}

object DynamicStreamHandling extends App {
  implicit val actorSystem = ActorSystem("DynamicStreamHandling")
  implicit val materializer = ActorMaterializer()

  // #1: Kill Switch
  val killSwitchFlow: Graph[FlowShape[Int, Int], UniqueKillSwitch] = KillSwitches.single[Int]

  import scala.concurrent.duration._

  val counter = Source(Stream.from(1)).throttle(1, 1 second).log("counter")
  val sink = Sink.ignore

  //val killSwitch: UniqueKillSwitch = counter.viaMat(killSwitchFlow)(Keep.right).to(sink).run()

  import actorSystem.dispatcher

  //  actorSystem.scheduler.scheduleOnce(3 seconds) {
  //    killSwitch.shutdown()
  //  }

  val anotherCounter = Source(Stream.from(10)).throttle(2, 1 second).log("another-counter")
  val sharedKillSwitch = KillSwitches.shared("oneButtonToRuleThemAll")

  //counter.via(sharedKillSwitch.flow).runWith(Sink.ignore)
  //anotherCounter.via(sharedKillSwitch.flow).runWith(Sink.ignore)

  //  actorSystem.scheduler.scheduleOnce(3 seconds) {
  //    sharedKillSwitch.shutdown()
  //  }

  // MergeHub
  val dynamicMerge: Source[Int, Sink[Int, NotUsed]] = MergeHub.source[Int]
  val materializedSink: Sink[Int, NotUsed] = dynamicMerge.to(Sink.foreach[Int](println)).run()

  // use this sink anytime we like
  //  Source(1 to 10).runWith(materializedSink)
  //  counter.runWith(materializedSink)

  // Broadcast Hub
  val dynamicBroadcast: Sink[Int, Source[Int, NotUsed]] = BroadcastHub.sink[Int]
  val materializedSource: Source[Int, NotUsed] = Source(1 to 100).runWith(dynamicBroadcast)

  //  materializedSource.runWith(Sink.ignore)
  //  materializedSource.runWith(Sink.foreach[Int](println))

  //  val s = Source(1 to 100)
  //  s.to(Sink.foreach(println)).run()
  //  s.to(Sink.foreach(println)).run()

  /**
    * Challenge - combine a mergeHub and a broadcastHub.
    *
    * A publisher-subscriber component
    */
  val merge = MergeHub.source[String]
  val bcast = BroadcastHub.sink[String]
  val (publisherPort, subscriberPort) = merge.toMat(bcast)(Keep.both).run()
  subscriberPort.runWith(Sink.foreach(x => println(s"I received the element: $x")))
  subscriberPort.map(string => string.length).runWith(Sink.foreach(n => println(s"I got a number: $n")))
  Source(List("Akka", "is", "amazing")).runWith(publisherPort)
  Source(List("I", "love", "scala")).runWith(publisherPort)
  Source(List("Awesome")).runWith(publisherPort)
}

package Graphs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.stream.scaladsl.{Balance, Broadcast, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source, Zip}

object Basics extends App {
  implicit val actorSystem = ActorSystem("GraphBasics")
  implicit val actorMaterializer = ActorMaterializer()

  val input = Source(1 to 1000)
  val incrementer = Flow[Int].map(_ + 1)
  val multiplier = Flow[Int].map(_ * 10)
  val output = Sink.foreach[(Int, Int)](println)

  // Step 1 - Setting up the fundamentals for the graph
  val graph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      // Step 2 - add the necessary components of this graph
      val broadcast = builder.add(Broadcast[Int](2)) // fan-out operator

      val zip = builder.add(Zip[Int, Int]) // fan-in operator

      // Step 3 - tying up the components
      input ~> broadcast

      broadcast.out(0) ~> incrementer ~> zip.in0
      broadcast.out(1) ~> multiplier ~> zip.in1

      zip.out ~> output

      // Step 4 - return a closed shape
      ClosedShape // FREEZE the builder's shape
      // immutable shape after this point
      // must return Shape object
    } // must be graph
  ) // runnable graph

  //graph.run() // run the graph and materialize it

  /**
    * excercise 1: feeds a source into 2 sinks
    */
  val sink1 = Sink.foreach[Int](x => println(s"Sink1: $x"))
  val sink2 = Sink.foreach[Int](x => println(s"Sink2: $x"))

  val graphWith2Sinks = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      val broadcast = builder.add(Broadcast[Int](2)) // fan-out operator

      input ~> broadcast ~> sink1 // implicit port numbering
      broadcast ~> sink2

      ClosedShape
    }
  )

  //graphWith2Sinks.run()

  /**
    * excercise 2: feeds a source into 2 sinks
    */

  import scala.concurrent.duration._

  val fastSource = input.throttle(5,1 second)
  val slowSource = input.throttle(2, 1 second)
/*  val firstSink = Sink.foreach[Int](x => println(s"Sink from Fast source: $x"))
  val secondSink = Sink.foreach[Int](x => println(s"Sink from Slow source: $x"))*/

  val firstSink = Sink.fold[Int,Int](0)((count, _) => {
    println(s"Sink 1 number of elemenets: $count")
    count + 1
  })

  val secondSink = Sink.fold[Int,Int](0)((count, _) => {
    println(s"Sink 2 number of elemenets: $count")
    count + 1
  })

  val graphWithMergeAndBalance = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      val merge = builder.add(Merge[Int](2))

      val balance = builder.add(Balance[Int](2))

      fastSource ~> merge ~> balance ~> firstSink
      slowSource ~> merge;   balance ~> secondSink

      ClosedShape
    }
  )

  graphWithMergeAndBalance.run()  // Number of elements going through Sink1 & Sink2 are almost equal at every point of time
  // Number of elements going through sink1 & sink2 are balanced
  // Balance Graph: Takes 2 sources emitting at very different speeds and evens out the rate of production of the
  //  elements in between these 2 sources, and splits them equally in between the 2 sinks
}

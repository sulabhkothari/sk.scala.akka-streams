package Graphs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, FlowShape, SinkShape, SourceShape}
import akka.stream.scaladsl.{Broadcast, Concat, Flow, GraphDSL, RunnableGraph, Sink, Source}

object OpenGraphs extends App {
  implicit val actorSystem = ActorSystem("OpenGraphs")
  implicit val materializer = ActorMaterializer()

  /*
     A composite source that concatenates 2 sources
      - emits ALL elements from the first source
      - then ALL the elements from the second
   */

  val firstSource: Source[Int, NotUsed] = Source(1 to 10)
  val secondSource = Source(42 to 1000)

  val sourceGraph = Source.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      val concat = builder.add(Concat[Int](2))

      firstSource ~> concat
      secondSource ~> concat

      SourceShape(concat.out)
    }
  )

  //sourceGraph.to(Sink.foreach(println)).run()

  /*
    Complex Sink
   */

  val sink1 = Sink.foreach[Int](x => println(s"Meaningful thing 1: $x"))
  val sink2 = Sink.foreach[Int](x => println(s"Meaningful thing 2: $x"))

  val sinkGraph = Sink.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      val broadcast = builder.add(Broadcast[Int](2))
      broadcast ~> sink1
      broadcast ~> sink2
      SinkShape(broadcast.in)
    }
  )

  //firstSource.to(sinkGraph).run()

  /**
    * Challenge - complex flow
    * Write your own flow that's composed of two other flows
    * - one that adds 1 to a number
    * - one that does number * 10
    */

  val incrementer = Flow[Int].map(_ + 1)
  val mulitplier: Flow[Int, Int, NotUsed] = Flow[Int].map(_ * 10)

  val flowGraph = Flow.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // everything operates on Shape not components
      // add takes Graph[FlowShape[Int, Int], NotUsed] which is extended by mulitplier: Flow[Int, Int, NotUsed]
      // def add[S <: Shape](graph: Graph[S, _]): S
      // this means that add returns FlowShape[Int, Int]
      val incrementerShape = builder.add(incrementer)
      val multiplierShape = builder.add(mulitplier)

      // Implicit conversion for Flows
      //     implicit def flow2flow[I, O](f: FlowShape[I, O])(implicit b: Builder[_]): PortOps[O] =
      //      new PortOpsImpl(f.out, b)
      // PortOps extends CombinerBase
      incrementerShape ~> multiplierShape
      FlowShape(incrementerShape.in, multiplierShape.out)
    } // static graph
  ) // component

  //firstSource.via(flowGraph).to(Sink.foreach(x => println(s"Flow Graph Processor Results: $x"))).run()

  /**
    * Excercise: flow from a sink and a source!
    */

  // Think of it like a flow depends on another stream with source & sink, kind of like a upstream system
  def fromSinkAndSource[A, B](sink: Sink[A, _], source: Source[B, _]): Flow[A, B, _] = {
    Flow.fromGraph(
      GraphDSL.create() { implicit builder =>
        val sourceShape = builder.add(source)
        val sinkShape = builder.add(sink)
        FlowShape(sinkShape.in, sourceShape.out)
      }
    )
  }

  // Available in Akka
  // fromSinkAndSourceCoupled can send termination and backpressure signals between these two otherwise unconnected components
  val f = Flow.fromSinkAndSourceCoupled(Sink.foreach(println), Source(10000 to 10007))
  Source(1 to 5).via(f).to(Sink.foreach(println)).run()

}

object ImplicitExperiment extends App {

  trait Y {
    def X(z: Z)
  }

  trait Z

  case class S()

  implicit class M(s: S) extends Y {
    override def X(z: Z): Unit = {}
  }

  implicit class M1(m: M) {
    def G = {}
  }

  implicit class N(s: S) extends Z

  val s1 = S()
  val s2 = S()

  s1.X(s2)

  // doesn't compile: recursive implicit conversions
  //s1.G

  new M(s1).G
}

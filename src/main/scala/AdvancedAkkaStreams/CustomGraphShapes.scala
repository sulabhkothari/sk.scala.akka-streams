package AdvancedAkkaStreams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Balance, GraphDSL, Merge, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape, Graph, Inlet, Outlet, Shape}

import scala.collection.immutable

object CustomGraphShapes extends App {
  implicit val system = ActorSystem("CustomGraphShapes")
  implicit val materializer = ActorMaterializer()

  // balance 2x3 shape
  case class Balance2x3(
                         in0: Inlet[Int],
                         in1: Inlet[Int],
                         out0: Outlet[Int],
                         out1: Outlet[Int],
                         out2: Outlet[Int]
                       ) extends Shape {
    override val inlets: immutable.Seq[Inlet[_]] = List(in0, in1)

    override val outlets: immutable.Seq[Outlet[_]] = List(out0, out1, out2)

    override def deepCopy(): Shape = Balance2x3(
      in0.carbonCopy(),
      in1.carbonCopy(),
      out0.carbonCopy(),
      out1.carbonCopy(),
      out2.carbonCopy())
  }

  val balance2x3Impl = GraphDSL.create() {
    implicit builder =>
      import GraphDSL.Implicits._
      val merge = builder.add(Merge[Int](2))
      val balance = builder.add(Balance[Int](3))
      merge ~> balance
      Balance2x3(merge.in(0), merge.in(1), balance.out(0), balance.out(1), balance.out(2))
  }


  /**
    * Excercise: Generalize the balance component, make it M x N
    */

  case class BalanceMxN[T](override val inlets: List[Inlet[T]], override val outlets: List[Outlet[T]]) extends Shape {
    override def deepCopy(): Shape = BalanceMxN(inlets.map(_.carbonCopy()), outlets.map(_.carbonCopy()))
  }

  object BalanceMxN{
    def apply[T](m: Int, n: Int): Graph[BalanceMxN[T], NotUsed] = GraphDSL.create() {
      implicit builder =>
        import GraphDSL.Implicits._
        val merge = builder.add(Merge[T](m))
        val balance = builder.add(Balance[T](n))
        merge ~> balance
        BalanceMxN[T](merge.inlets.toList, balance.outlets.toList)
    }
  }

  val balanceMxNGraph = RunnableGraph.fromGraph {
    GraphDSL.create() {
      implicit builder =>
        import GraphDSL.Implicits._
        import scala.concurrent.duration._
        val slowSource = Source(Stream.from(1)).throttle(1, 1 second)
        val fastSource = Source(Stream.from(1)).throttle(2, 1 second)

        def createSink(index: Int) = Sink.fold(0)((count: Int, element: Int) => {
          println(s"[sink $index] Received $element, current count is $count")
          count + 1
        })

        val sink1 = builder.add(createSink(1))
        val sink2 = builder.add(createSink(2))
        val sink3 = builder.add(createSink(3))
        val balanceMxN = builder.add(BalanceMxN[Int](2,3))

        slowSource ~> balanceMxN.inlets(0)
        fastSource ~> balanceMxN.inlets(1)
        balanceMxN.outlets(0) ~> sink1
        balanceMxN.outlets(1) ~> sink2
        balanceMxN.outlets(2)~> sink3

        ClosedShape
    }
  }

  balanceMxNGraph.run()

}

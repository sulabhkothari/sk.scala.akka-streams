package Graphs

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, OverflowStrategy, UniformFanInShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, MergePreferred, RunnableGraph, Sink, Source, Zip, ZipWith}

object GraphCycles extends App {
  implicit val actorSystem = ActorSystem("GraphCycles")
  implicit val materializer = ActorMaterializer()

  val accelerator = GraphDSL.create() {
    implicit builder =>
      import GraphDSL.Implicits._
      val sourceShape = builder.add(Source(1 to 100))
      val mergeShape = builder.add(Merge[Int](2))
      val incrementShape = builder.add(Flow[Int].map(x => {
        println(s"Accelerating $x")
        x + 1
      }))

      // Elements in the input keep on increasing & Causes buffering and then back pressured and stopped
      // Graph Cycle deadlock
      sourceShape ~> mergeShape ~> incrementShape
      mergeShape <~ incrementShape

      ClosedShape
  }

  //RunnableGraph.fromGraph(accelerator).run()
  /*
    Solution 1: Merge Preferred
    It has preferential input where it will take this input irrespective of what is available on the other port
   */
  val actualAccelerator = GraphDSL.create() {
    implicit builder =>
      import GraphDSL.Implicits._
      val sourceShape = builder.add(Source(1 to 100))

      // merge preferred has its preferred port plus one additional input port we specify
      val mergeShape = builder.add(MergePreferred[Int](1))
      val incrementShape = builder.add(Flow[Int].map(x => {
        println(s"Accelerating $x")
        x + 1
      }))

      // Elements in the input keep on increasing & Causes buffering and then back pressured and stopped
      // Graph Cycle deadlock
      sourceShape ~> mergeShape ~> incrementShape
      mergeShape.preferred <~ incrementShape

      ClosedShape
  }
  //RunnableGraph.fromGraph(actualAccelerator).run()

  /*
    Solution 2: Buffers
   */
  val bufferedRepeater = GraphDSL.create() {
    implicit builder =>
      import GraphDSL.Implicits._
      val sourceShape = builder.add(Source(1 to 100))

      val mergeShape = builder.add(Merge[Int](2))
      val repeaterShape = builder.add(Flow[Int].buffer(10, OverflowStrategy.dropHead).map(x => {
        println(s"Accelerating $x")
        Thread.sleep(100)
        x
      }))

      // Elements in the input keep on increasing & Causes buffering and then back pressured and stopped
      // Graph Cycle deadlock
      sourceShape ~> mergeShape ~> repeaterShape
      mergeShape <~ repeaterShape

      ClosedShape
  }
  //RunnableGraph.fromGraph(bufferedRepeater).run()

  /*
    cycles risk deadlocks especially when they are unbounded
      - add bounds to the number of elements in the cycle

      boundedness vs liveness
   */

  /**
    * Excercise: create a fan-in shape
    * - two inputs which will be fed with EXACTLY one number
    * - output will emit an INFINITE FIBONACCI SEQUENCE based off those 2 numbers
    * 1,2,3,5,8,...
    *
    * Hint: Use ZipWith and cycles
    */

  val fibonacciStream = GraphDSL.create() {
    implicit builder =>
      import GraphDSL.Implicits._
      val mergeShape1 = builder.add(MergePreferred[BigInt](1))
      val mergeShape2 = builder.add(MergePreferred[BigInt](1))
      val adder = builder.add(ZipWith[BigInt, BigInt, BigInt]((a, b) => a + b))
      //val max2 = builder.add(ZipWith[Int, Int, Int]((a, b) => a + b))

      val broadcast1 = builder.add(Broadcast[BigInt](2))
      val broadcast2 = builder.add(Broadcast[BigInt](2))

      mergeShape1 ~> broadcast1;
      broadcast1.out(0) ~> adder.in0
      mergeShape2 ~> broadcast2;
      broadcast2.out(0) ~> adder.in1
      broadcast2.out(1) ~> mergeShape1.preferred
      adder.out ~> mergeShape2.preferred
      UniformFanInShape(broadcast1.out(1), mergeShape1.in(0), mergeShape2.in(0))
  }

  val fibonacciGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      val sink = builder.add(Sink.foreach[BigInt](x => {
        Thread.sleep(1000)
        println(x)
      }))
      val fStreamShape = builder.add(fibonacciStream)
      val num1 = builder.add(Source.single(BigInt(1)))
      val num2 = builder.add(Source.single(BigInt(2)))
      num1 ~> fStreamShape.in(0)
      num2 ~> fStreamShape.in(1)
      fStreamShape ~> sink
      ClosedShape
    }
  )

  //fibonacciGraph.run()

  val fibonacciStream1 = GraphDSL.create() {
    implicit builder =>
      import GraphDSL.Implicits._
      val mergeShape1 = builder.add(MergePreferred[BigInt](1))
      val mergeShape2 = builder.add(MergePreferred[BigInt](1))
      val adder = builder.add(ZipWith[BigInt, BigInt, BigInt]((a, b) => a + b))
      //val max2 = builder.add(ZipWith[Int, Int, Int]((a, b) => a + b))

      val broadcast = builder.add(Broadcast[BigInt](3))
      //val broadcast2 = builder.add(Broadcast[BigInt](2))

      mergeShape1 ~> broadcast;
      broadcast.out(1) ~> adder.in0
      mergeShape2 ~> adder.in1
      broadcast.out(2) ~> mergeShape2.preferred
      adder.out ~> mergeShape1.preferred
      UniformFanInShape(broadcast.out(0), mergeShape1.in(0), mergeShape2.in(0))
  }

  val fibonacciGraph1 = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      val sink = builder.add(Sink.foreach[BigInt](x => {
        Thread.sleep(1000)
        println(x)
      }))
      val fStreamShape = builder.add(fibonacciStream1)
      val num1 = builder.add(Source.single(BigInt(1)))
      val num2 = builder.add(Source.single(BigInt(1)))
      num1 ~> fStreamShape.in(0)
      num2 ~> fStreamShape.in(1)
      fStreamShape ~> sink
      ClosedShape
    }
  )

  //fibonacciGraph1.run()

  val fibonacciStream2 = GraphDSL.create() {
    implicit builder =>
      import GraphDSL.Implicits._
      val mergeShape = builder.add(MergePreferred[(BigInt, BigInt)](1))
      val zipperShape = builder.add(Zip[BigInt, BigInt])

      val fiboLogic = builder.add(Flow[(BigInt, BigInt)].map(x => (x._2, x._1 + x._2)))
      val resultLogic = builder.add(Flow[(BigInt, BigInt)].map(_._1))
      val broadcast = builder.add(Broadcast[(BigInt, BigInt)](2))
      zipperShape.out ~> mergeShape ~> fiboLogic ~> broadcast ~> resultLogic
                            mergeShape.preferred <~ broadcast

      UniformFanInShape(resultLogic.out, zipperShape.in0, zipperShape.in1)
  }

  val fibonacciGraph2 = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      val sink = builder.add(Sink.foreach[BigInt](x => {
        Thread.sleep(1000)
        println(x)
      }))
      val fStreamShape = builder.add(fibonacciStream2)
      val num1 = builder.add(Source.single(BigInt(1)))
      val num2 = builder.add(Source.single(BigInt(1)))
      num1 ~> fStreamShape.in(0)
      num2 ~> fStreamShape.in(1)
      fStreamShape ~> sink
      ClosedShape
    }
  )

  fibonacciGraph2.run()

}

package Graphs

import java.util.Date

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, FanOutShape2, UniformFanInShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source, ZipWith}

object MoreOpenGraphs extends App {
  implicit val actorSystem = ActorSystem("MoreOpenGraphs")
  implicit val materializer = ActorMaterializer()

  /**
    * Example: Max 3 operator
    *  - 3 inputs of type int
    *  - the maximum of the 3
    */

  val max3StaticGraph = GraphDSL.create() {
    implicit builder =>
      import GraphDSL.Implicits._
      val max1 = builder.add(ZipWith[Int, Int, Int]((a, b) => Math.max(a, b)))
      val max2 = builder.add(ZipWith[Int, Int, Int]((a, b) => Math.max(a, b)))

      max1.out ~> max2.in0

      UniformFanInShape(max2.out, max1.in0, max1.in1, max2.in1)
  }

  val source1 = Source(1 to 10)
  val source2 = Source(1 to 10).map(_ => 5)
  val source3 = Source((1 to 10).reverse)

  val maxSink = Sink.foreach[Int](x => println(s"Max is: $x"))
  val max3RunnableGraph = RunnableGraph.fromGraph(
    GraphDSL.create() {
      implicit builder =>
        import GraphDSL.Implicits._

        // Using custom component
        val max3Shape = builder.add(max3StaticGraph)
        source1 ~> max3Shape.in(0)
        source2 ~> max3Shape.in(1)
        source3 ~> max3Shape.in(2)
        max3Shape.out ~> maxSink
        ClosedShape
    }
  )

  //max3RunnableGraph.run()

  // same for UniformFanOutShape
  // they are called uniform because all of the inputs receive elements of the same type

  /**
    * Non-uniform fan out shape
    * Processing bank transactions
    * Transaction suspicious if amount is more than 10000
    *
    * Streams component for transactions
    *  - output1: let the transaction go through unmodified
    *  - output2: suspicious transaction ids
    */

  val bankProcessor = Sink.foreach[Transaction](println)
  val suspiciousAnalysisService = Sink.foreach[String](txnId => println(s"Suspicious Txn Id: $txnId"))
  case class Transaction(id: String, source: String, recipient: String, amount: Int, date: Date)

  val transactionSource = Source(
    List(
      Transaction("3485384756", "Paul", "Jim", 100, new Date),
      Transaction("8885384799", "Daniel", "Jim", 10000, new Date),
      Transaction("9985384756", "Jim", "Alice", 7000, new Date)))

  val suspiciousTransactionStaticGraph =
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val broadcast = builder.add(Broadcast[Transaction](2))
      val suspiciousTransactionFilter = builder.add(Flow[Transaction].filter(_.amount > 1000))
      val txnIdExtractor = builder.add(Flow[Transaction].map[String](_.id))

      broadcast.out(0) ~> suspiciousTransactionFilter ~> txnIdExtractor

      new FanOutShape2(broadcast.in, broadcast.out(1), txnIdExtractor.out)
    }

  val suspiciousTransactionRunnableGraph = RunnableGraph.fromGraph(
    GraphDSL.create(){ implicit builder =>
      import GraphDSL.Implicits._

      val suspiciousTxnShape = builder.add(suspiciousTransactionStaticGraph)
      transactionSource ~> suspiciousTxnShape.in
      suspiciousTxnShape.out0 ~> bankProcessor
      suspiciousTxnShape.out1 ~> suspiciousAnalysisService

      ClosedShape
    }
  )

  suspiciousTransactionRunnableGraph.run()

}

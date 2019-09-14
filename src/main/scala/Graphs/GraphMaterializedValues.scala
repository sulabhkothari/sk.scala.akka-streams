package Graphs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, FlowShape, SinkShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Sink, Source}

import scala.concurrent.Future
import scala.util.{Failure, Success}

object GraphMaterializedValues extends App {
  implicit val actorSystem = ActorSystem("GraphMaterializedValues")
  implicit val materializer = ActorMaterializer()

  val wordSource = Source(List("Akka", "is", "awesome", "rock", "the", "jvm"))
  val printer = Sink.foreach(println)
  val counter: Sink[String, Future[Int]] = Sink.fold[Int, String](0)((count, _) => count + 1)

  /*
    A composite component (sink)
    - prints out all the strings which are lowercase
    - COUNTS the strings that are short (< 5)
   */

  val complexWordSink: Sink[String, Future[Int]] = Sink.fromGraph(
    GraphDSL.create(printer, counter)((printerMatValue, counterMatValue) => counterMatValue) {
      implicit builder =>
        (printerShape, counterShape) =>
          import GraphDSL.Implicits._
          val broadcast = builder.add(Broadcast[String](2))
          val lowercaseFilter = builder.add(Flow[String].filter(word => word == word.toLowerCase))
          val shortStringFilter = builder.add(Flow[String].filter(_.length < 5))
          broadcast.out(0) ~> lowercaseFilter ~> printerShape
          broadcast.out(1) ~> shortStringFilter ~> counterShape
          SinkShape(broadcast.in)
    }
  )

  val shortStringCountFuture = wordSource.toMat(complexWordSink)(Keep.right).run()

  import scala.concurrent.ExecutionContext.Implicits._

  shortStringCountFuture.onComplete {
    case Success(value) => println(s"The total number of short strings: $value")
    case Failure(exception) => println(s"The total number of short strings failed: $exception")
  }

  /**
    * Excercise
    */
  def enhanceFlow[A, B](flow: Flow[A, B, _]) = {
    /*
      Hint: use a broadcast and a Sink.fold
     */
    val sink = Sink.fold[Int, B](0)((count, _) => count + 1)
    Flow.fromGraph(
      GraphDSL.create(sink) {
        implicit builder =>
          sinkShape =>
            import GraphDSL.Implicits._
            val broadcast = builder.add(Broadcast[B](2))
            val flowShape = builder.add(flow)
            flowShape ~> broadcast ~> sinkShape
            FlowShape(flowShape.in, broadcast.out(1))

            // Exception thrown with below: Outlets [Map.out] were returned in the resulting shape but were already connected.
            //FlowShape(flowShape.in, flowShape.out)
      }
    )
  }

  val simpleSource = Source(1 to 42)
  val simpleFlow = Flow[Int].map(x => x)
  val simpleSink = Sink.ignore
  val enhancedFlowCountFuture: Future[Int] = simpleSource.viaMat(enhanceFlow(simpleFlow))(Keep.right).toMat(simpleSink)(Keep.left).run()
  enhancedFlowCountFuture.onComplete{
    case Success(count) => println(s"Count elements went through the enhanced flow: $count")
    case Failure(ex) => println(ex)
  }
}

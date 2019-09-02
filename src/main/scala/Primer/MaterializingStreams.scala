package Primer

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}

import scala.concurrent.Future
import scala.util.{Failure, Success}

object MaterializingStreams extends App {
  implicit val system = ActorSystem("MaterializingStreams")
  implicit val materializer = ActorMaterializer()

  //val simpleGraph = Source(1 to 10).to(Sink.foreach(println))
  //val simpleMaterializedValue = simpleGraph.run()

  val source = Source(1 to 10)
  val sink: Sink[Int, Future[Int]] = Sink.reduce[Int]((a, b) => a + b)

  import system.dispatcher

  val sumFuture: Future[Int] = source.runWith(sink)
  sumFuture.onComplete {
    case Success(value) => println(s"The sum of all elements is: $value")
    case Failure(ex) => println(s"The sum of the elements could not be computed: $ex")
  }
  // When we create a graph with via and to, when we connect flows and sinks, by default the leftmost materialised value
  //  is kept

  // choosing materialized value
  val simpleSource = Source(1 to 10)
  val simpleFlow = Flow[Int].map(x => x + 1)
  val simpleSink = Sink.foreach[Int](println)
  //val graph: Source[Int, NotUsed] = simpleSource.viaMat(simpleFlow)((sourceMat, flowMat) => flowMat)
  // Same as below
  // Future[Done] because of println which doesn't return a type
  val graph: RunnableGraph[Future[Done]] = simpleSource.viaMat(simpleFlow)(Keep.right).toMat(simpleSink)(Keep.right) //Keep.left, Keep.both (returns Tuple), Keep.None
  graph.run().onComplete {
    case Success(_) => println("Stream processing finished")
    case Failure(ex) => println(s"Stream processing failed with: $ex")
  }

  // sugars
  val r: Future[Int] = Source(1 to 10).runWith(Sink.reduce[Int](_ + _)) //equivalent to source.to(Sink.reduce)(Keep.right)
  val r2: Future[Int] = Source(1 to 10).runReduce(_ + _) //Same as above

  // backwards
  Sink.foreach[Int](println).runWith(Source.single(42)) //source(..).to(sink...).run()
  // both ways
  val r3: (NotUsed, Future[Done]) = Flow[Int].map(_ * 2).runWith(simpleSource, simpleSink)

  /**
    * - return the last element out of a source  (use Sink.last)
    * - compute the total word count out of a stream of sentences
    *  - map, fold, reduce
    */
  Source(1 to 19).runWith(Sink.last).onComplete {
    case Success(value) => println(s"Last Element1: $value")
    case _ => println("Failed LE1")
  }

  Source(1 to 19).runWith(Sink.reduce[Int]((x, y) => y)).onComplete {
    case Success(value) => println(s"Last Element2: $value")
    case _ => println("Failed LE2")
  }

  //val rs: RunnableGraph[Future[Done]] = Source(1 to 19).viaMat(Flow[Int].reduce((x, y) => y))(Keep.right).toMat(Sink.foreach(println))(Keep.right)
  //  val rs = Source(1 to 19).toMat(Sink.reduce[Int]((x, y) => y))(Keep.right).run().onComplete {
  //    case Success(_) =>
  //    case _ =>
  //  }

  val sentenceSource = Source(List(
    "A simple sentence",
    "Sailor's feedback of the seas",
    "Akka or actor model with RUST"
  ))

  sentenceSource.runWith(Sink.fold(0)((agg, x) => agg + x.split(" ").length)).onComplete {
    case Success(value) => println(s"Word Count1: $value")
    case _ => println("Failed Word Count1")
  }

  sentenceSource.map(_.split(" ").length).runWith(Sink.reduce[Int](_ + _)).onComplete {
    case Success(value) => println(s"Word Count2: $value")
    case _ => println("Failed Word Count2")
  }

  //val rp: Future[Int] = sentenceSource.runFold(0)((agg, s) => agg + s.split(" ").length)
  //val v: Future[Int] = sentenceSource.map(_.split(" ").length).runReduce(_ + _)
  val wordCountFlow = Flow[String].fold[Int](0)((agg, s) => agg + s.split(" ").length)
  sentenceSource.via(wordCountFlow).toMat(Sink.head)(Keep.right).run().onComplete {
    case Success(value) => println(s"Word Count3: $value")
    case _ => println("Failed Word Count3")
  }

  //val m: Future[Int] = wordCountFlow.runWith(sentenceSource, Sink.head)._2
}

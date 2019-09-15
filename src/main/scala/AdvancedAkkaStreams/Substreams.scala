package AdvancedAkkaStreams

import AdvancedAkkaStreams.Substreams.wordsSource
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, RunnableGraph, Sink, Source, SubFlow}

import scala.concurrent.Future
import scala.util.{Failure, Success}

object Substreams extends App {
  implicit val actorSystem = ActorSystem("Substreams")
  implicit val materializer = ActorMaterializer()

  // 1 - grouping a stream by a certain function
  val wordsSource = Source(List("AKka", "is", "amazing", "learning", "substreams"))
  val groups: SubFlow[String, NotUsed, wordsSource.Repr, RunnableGraph[NotUsed]] =
    wordsSource.groupBy(30, word => if (word.isEmpty) '\0' else word.toLowerCase.charAt(0))
  groups.to(Sink.fold(0)((count, word) => {
    val newCount = count + 1
    println(s"I just received $word, count is $newCount")
    newCount
  }))
    .run()

  // 2 - merge substream back
  val textSource = Source(List(
    "I love Akka streams",
    "this is amazing",
    "learning from Rock the jvm"
  ))

  val totalCharacterCountFuture: Future[Int] = textSource.groupBy(2, string => string.length % 2)
    .map(_.length) // do your expensive computation here
    .mergeSubstreamsWithParallelism(2) // 2 is a cap on the number of substream that can be merged at any given time
    // if you want to add more substreams but your cap is lower than the amount of substreams that you have, those substreams
    // that are starting to be merged must complete before other streams can be merged in as well. So this risks deadlock
    // if your substreams are infinite. If your substreams are unbounded then simply use mergeSubstreams method which
    // is equivalent to mergeSubstreamsWithParallelism(Int.Max_Value)
    .toMat(Sink.reduce[Int](_ + _))(Keep.right)
    .run()

  import actorSystem.dispatcher

  totalCharacterCountFuture.onComplete {
    case Success(value) => println(s"Total char count: $value")
    case Failure(ex) => println(s"Char computation failed: $ex")
  }

  // 3 - splitting a stream into substreams, when a condition is met
  val text = "I love Akka streams\n" + "this is amazing\n" + "learning from Rock the jvm\n"
  val anotherCharCountFuture = Source(text.toList)
    .splitWhen(c => c == '\n')
    .filter(_ != '\n')
    .map(_ => 1)
    .mergeSubstreams
    .toMat(Sink.reduce[Int](_ + _))(Keep.right)
    .run()

  anotherCharCountFuture.onComplete {
    case Success(value) => println(s"Total char count alternative: $value")
    case Failure(ex) => println(s"Char computation failed: $ex")
  }

  //4 - flattening
  val simpleSource = Source(1 to 5)
  //simpleSource.flatMapConcat(x => Source(x to 3*x)).runWith(Sink.foreach(println))
  simpleSource.flatMapMerge(2, x => Source(x to 3*x)).runWith(Sink.foreach(println))

}

package Primer

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.Future

object FirstPrinciples extends App {
  implicit val system = ActorSystem("FirstPrinciples")

  // It allows runnning of akka streams components
  implicit val materializer = ActorMaterializer()

  // sources
  val source = Source(1 to 10)

  // sink
  val sink = Sink.foreach[Int](println)

  // connect Source & Sink
  // this expression is called a graph
  val graph = source.to(sink)

  graph.run()

  // flows transform elements
  val flow = Flow[Int].map(_ + 1)
  val sourceWithFlow = source.via(flow)
  val flowWithSink = flow.to(sink)

  //  sourceWithFlow.to(sink).run()
  //  source.to(flowWithSink).run()
  //  source.via(flow).to(sink).run()

  // Sources can emit any object as long as they are immutable and serializable much like akka actor messages

  // nulls are not allowed
  //Source.single[String](null).to(Sink.foreach(println)).run()
  // use Options

  // Various kinds of sources
  val finiteSource = Source.single(1)
  val anotherFiniteSource = Source(List(1, 2, 3))
  val emptySource = Source.empty[Int]
  val infiniteSource = Source(Stream.from(1)) //do not confuse an Akka stream with a "collection" stream

  import scala.concurrent.ExecutionContext.Implicits.global

  val futureSource = Source.fromFuture(Future(42))

  // sinks
  val theMostBoringSink = Sink.ignore
  val foreachSink = Sink.foreach[String](println)
  val headSink = Sink.head[Int] // retrieves the head and then closes the stream
  val foldSink = Sink.fold[Int, Int](0)((a, b) => a + b)

  // flows - usually mapped to collection operators
  val mapFlow = Flow[Int].map(_ * 2)
  val takeFlow = Flow[Int].take(5)
  // drop, filter
  // NOT have flatMap

  // source -> flow -> flow -> ... -> sink
  val doubleFlowGraph = source.via(mapFlow).via(takeFlow).to(sink)

  // Syntactic sugar
  val mapSource = Source(1 to 10).map(_ * 2) // Source(1 to 10).via(Flow[Int].map(_ * 2))
  // run streams directly
  mapSource.runForeach(println) // mapSource.to(Sink.foreach[Int](println)).run()

  // OPERATORS = Components

  // Take first 2 names with length > 5 characters
  val sourceOfNames = Source(List("Jack", "Jill", "Richard", "Doug", "Lampert", "Shawn")) //.filter(_.length > 5).take(2).runForeach(println)
  val filterFlow = Flow[String].filter(_.length > 5)
  val limitFlow = Flow[String].take(2)
  val graphForNames = sourceOfNames.via(filterFlow).via(limitFlow).to(Sink.foreach(println))

  graphForNames.run()

}

package Primer

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}

object BackPressureBasics extends App {
  implicit val actorSystem = ActorSystem("BackPressureBasics")
  implicit val actorMaterializer = ActorMaterializer()

  val fastSource = Source(1 to 1000)
  val slowSink = Sink.foreach[Int] {
    x =>
      Thread.sleep(1000)
      println(s"Sink: $x")
  }

  // fastSource.to(slowSink).run() // fusing operators
  // fastSource.async.to(slowSink).run()
  // backpressure: slowing down the producer

  // backpressure is propagated via all components
  val simpleFlow = Flow[Int].map { x =>
    println(s"Incoming: $x")
    x + 1
  }

  //  fastSource.async
  //    .via(simpleFlow).async
  //    .to(slowSink).run()
  // Output contains batch of incoming and then few incremental processes at the sink
  // this is backpressure in action
  // Slow sink sends a backpressure signal to flow, which instead of propagating the signal to source, internally
  //  buffers number of elements (default buffer in akka stream is 16 elements). Once this buffer is full flow will send
  //  backpressure signal to fast source, and wait for some more incoming demand from sink. Once sink processes some
  //  elements, simpleflow allowed some more elements to go through, and it buffered them again.
  // An Akka stream components can have multiple reactions to backpressure signals and buffering is one of them.

  /*
    reactions to backpressure (in order):
    - try to slow down if possible
    - buffer elements until there is more demand
    - drop down elements from the buffer if it overflows (can be controlled by programmer)
    - tear down or kill the whole stream (failure)
   */

  // dropHead will drop the oldest element in the buffer to make way for new one
  val bufferedFlow = simpleFlow.buffer(10, overflowStrategy = OverflowStrategy.dropBuffer)
  fastSource.async
    .via(bufferedFlow).async
    .to(slowSink)
    .run()
  // Flow buffers the last 10 elements and drops all others because of dropHead strategy
  // Sink buffers the first 16 elements
  // So, it prints first 16 and then last 10

  /*
    1-16: nobody is backpressured
    17-26: flow will buffer, flow will start dropping at the next element
    26-1000: flow will always drop the oldest element
    => 991 - 1000 => 992-1001 => sink
   */

  /*
    Overflow Strategies:
    - drop head = oldest
    - drop tail = drops the youngest element from the buffer to make space for new one (prints 2-26,1001)
    - drop new = exact element to be added = keeps the buffer (prints 2-27)
    - drop the entire buffer when new element arrives and buffer is full (prints 2-17,998-1001)
    - backpressure signal (default behaviour)
    - fail (2-17 & actor gets failure)
   */

  //throttling
  //  import scala.concurrent.duration._
  //
  //  fastSource.throttle(10, 1 second).runWith(Sink.foreach(println))
}

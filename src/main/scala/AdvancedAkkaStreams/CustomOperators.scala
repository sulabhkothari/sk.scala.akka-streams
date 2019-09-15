package AdvancedAkkaStreams

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, Attributes, FlowShape, Inlet, Outlet, SinkShape, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Random, Success, Try}

object CustomOperators extends App {
  implicit val system = ActorSystem("CustomOperators")
  implicit val materializer = ActorMaterializer()

  // 1 - a custom source which emits random numbers until cancelled
  class RandomNumberGenerator(max: Int) extends GraphStage[ /*Step 0: define the shape*/ SourceShape[Int]] {
    // Step 1: define the ports and component-specific members
    val outport = Outlet[Int]("randomGenerator")
    val random = new Random

    // Step 2: construct a new shape
    override def shape: SourceShape[Int] = SourceShape(outport)

    // Step 3: create the logic
    // This method will be called by Akka when this component is materialized
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      // Step 4: define the mutable state and set the handler
      setHandler(outport, new OutHandler {
        // Called when there is demand from downstream
        override def onPull(): Unit = {
          //emit a new element
          val nextNumber = random.nextInt(max)

          //push it out from outport
          push(outport, nextNumber)
        }
      })
    }
  }

  val randomGeneratorSource = Source.fromGraph(new RandomNumberGenerator(100))

  //randomGeneratorSource.runWith(Sink.foreach(println))

  // 2 - Custom sink that prints elements in batches of a given size
  class Batcher(batchSize: Int) extends GraphStage[SinkShape[Int]] {
    val inport = Inlet[Int]("batcher")

    override def shape: SinkShape[Int] = SinkShape(inport)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      // lifecycle method - implement logic to ask for an element to send the first signal
      //  this will start the stream with this first signal
      override def preStart(): Unit = pull(inport)

      val batch = new mutable.Queue[Int]
      setHandler(inport, new InHandler {
        // when the upstream wants to send an element
        override def onPush(): Unit = {
          val nextElement = grab(inport)
          batch.enqueue(nextElement)

          // Backpressure signal will still be sent automatically even with these custom components
          // "Slow consumer backpressure a fast producer"
          // assume some complex computation here
          Thread.sleep(100)

          if (batch.size >= batchSize) {
            println(s"New batch: ${batch.dequeueAll(_ => true).mkString("[", ",", "]")}")
          }
          pull(inport) //send demand upstream
        }

        override def onUpstreamFinish() = {
          if (batch.nonEmpty) {
            println(s"New batch: ${batch.dequeueAll(_ => true).mkString("[", ",", "]")}")
            println("Stream finished")
          }
        }
      })
    }
  }

  val batchSink = Sink.fromGraph(new Batcher(10))

  //randomGeneratorSource.to(batchSink).run()

  /**
    * Excercise: a custom flow - a simple filter flow
    * - 2 ports: an input port and an output port
    */
  case class FilterFlow[T](predicate: T => Boolean) extends GraphStage[FlowShape[T, T]] {
    val inport = Inlet[T]("FilterFlowInport")
    val outport = Outlet[T]("FilterFlowOutport")

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      setHandler(inport, new InHandler {
        override def onPush(): Unit = {
          val nextElement = grab(inport)

          def processElement(predicateValue: Boolean, element: T) =
            if (predicateValue) {
              push(outport, nextElement) // pass it on
            }
            else {
              pull(inport) // ask for another element
            }

          Try(predicate(nextElement)) match {
            case Success(flag) => processElement(flag, nextElement)
            case Failure(exception) => failStage(exception)
          }

        }
      })

      setHandler(outport, new OutHandler {
        override def onPull(): Unit = {
          pull(inport)
        }
      })
    }

    override def shape: FlowShape[T, T] = FlowShape(inport, outport)
  }

  def predicate(n: Int) = {
    if (n % 2 == 0) true
    else if (n == 11) throw new RuntimeException
    else false
  }

  // backpressure works out of the box
  //randomGeneratorSource.via(FilterFlow(predicate)).to(new Batcher(3)).run()

  /**
    * Materialized values in graph stages
    */

  // 3 - a flow that counts the number of elements that go through it
  case class CounterFlow[T]() extends GraphStageWithMaterializedValue[FlowShape[T, T], Future[Int]] {
    val inport = Inlet[T]("CounterIn")
    val outport = Outlet[T]("CounterOut")

    override val shape: FlowShape[T, T] = FlowShape(inport, outport)

    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Int]) = {
      val promise = Promise[Int]
      val logic = new GraphStageLogic(shape) {
        // mutable state
        var counter = 0
        setHandler(inport, new InHandler {
          override def onPush(): Unit = {
            val nextElement = grab(inport)
            counter += 1
            push(outport, nextElement)
          }

          override def onUpstreamFinish(): Unit = {
            promise.success(counter)
            super.onUpstreamFinish()
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            promise.failure(ex)
            super.onUpstreamFailure(ex)
          }
        })
        setHandler(outport, new OutHandler {
          override def onPull(): Unit = {
            pull(inport)
          }

          override def onDownstreamFinish(): Unit = {
            promise.success(counter)
            super.onDownstreamFinish()
          }
        })
      }
      (logic, promise.future)
    }
  }

  import system.dispatcher

  val counterFlow = Flow.fromGraph(CounterFlow[Int])
  val count: Future[Int] = Source(1 to 10)
    //.map(n => if (n == 7) throw new RuntimeException("gotcha") else n)
    .viaMat(counterFlow)(Keep.right)
    .to(Sink.foreach(n => if (n == 7) throw new RuntimeException("gotcha") else n))
    //.toMat(Sink.foreach(println))(Keep.left)
    .run()
  count.onComplete {
    case Success(value) => println(s"Count: $value")
    case Failure(ex) => println(s"Count failed: $ex")
  }

  // Handler callbacks are never called concurrently, so mutable state can be safely accessed inside the handlers
  // DO NOT EXPOSE mutable state outside the handlers, otherwise it will break component encapsulation.
}

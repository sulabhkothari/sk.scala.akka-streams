package AkkaStreamsTechniquesAndPatterns

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.Timeout

object IntegrateWithActors extends App {
  implicit val actorSystem = ActorSystem("IntegratingWithActors")
  implicit val materializer = ActorMaterializer()

  class SimpleActor extends Actor with ActorLogging {
    override def receive: Receive = {
      case s:String =>
        log.info(s"Just received a string: $s")
        sender() ! s"$s$s"
      case n:Int =>
        log.info(s"Just received a number: $n")
        sender() ! 2 * n
      case _ =>
    }
  }

  val simpleActor = actorSystem.actorOf(Props[SimpleActor], "SimpleActor")

  val numberSource = Source(1 to 10)

  import scala.concurrent.duration._
  implicit val timeout = Timeout(2 seconds)
  // actor as a flow
  // based on ask pattern on this actor
  val actorBasedFlow = Flow[Int].ask[Int](parallelism = 4)(simpleActor)
  numberSource.via(actorBasedFlow).to(Sink.ignore).run()
  numberSource.ask[Int](parallelism = 4)(simpleActor).to(Sink.ignore).run()

  /*
    Actor as a source
   */
  val actorPoweredSource: Source[Int, ActorRef] =
    Source.actorRef[Int](bufferSize = 10, overflowStrategy = OverflowStrategy.dropHead)

  val materializedActorRef =
    actorPoweredSource.to(Sink.foreach(number => println(s"Actor Powered flow got number: $number"))).run()

  materializedActorRef ! 10
  materializedActorRef ! akka.actor.Status.Success("complete")

  /*
    Actor as a destination/sink
    - an init message
    - an acknowledge message to confirm the reception (absence of this ack message is interpreted as backpressure)
    - complete message
    - a function to generate a message in case the stream throws an exception
   */

  case object StreamInit
  case object StreamAck
  case object StreamComplete
  case class StreamFail(ex: Throwable)

  class DestinationActor extends Actor with ActorLogging{
    override def receive: Receive = {
      case StreamInit =>
        log.info("Stream initialized")
        sender() ! StreamAck
      case StreamComplete =>
        log.info("Stream complete")
        context.stop(self)
      case StreamFail(ex) =>
        log.warning(s"Stream failed: $ex")
      case message =>
        log.info(s"Message has come to its final resting point: $message")
        sender() ! StreamAck
    }
  }

  val destinationActor = actorSystem.actorOf(Props[DestinationActor], "DestinationActor")
  val actorPoweredSink = Sink.actorRefWithAck[Int](
    destinationActor,
    onInitMessage = StreamInit,
    ackMessage = StreamAck,
    onCompleteMessage = StreamComplete,
    onFailureMessage = throwable => StreamFail(throwable) // optional as it has default value
  )

  Source(1 to 10).to(actorPoweredSink).run()

  // Sink.actorRef() - not recommended, unable to backpressure
}

package AkkaStreamsTechniquesAndPatterns

import akka.actor.ActorSystem
import akka.stream.Supervision.{Resume, Stop}
import akka.stream.{ActorAttributes, ActorMaterializer}
import akka.stream.scaladsl.{RestartSource, Sink, Source}

import scala.util.Random

object FaultTolerance extends App {
  implicit val actorSystem = ActorSystem("FaultTolerance")
  implicit val materializer = ActorMaterializer()

  // 1 - logging
  val faultySource = Source(1 to 10).map(e => if (e == 6) throw new RuntimeException else e)
  //faultySource.log("trackingElements").to(Sink.ignore).run()

  // 2 - Gracefully terminating a stream
  //  faultySource.recover {
  //    case _: RuntimeException => Int.MinValue
  //  }.log("Graceful source")
  //    .to(Sink.ignore).run()

  // 3 - Recover with another stream
  //  faultySource.recoverWithRetries(3, {
  //    case _: RuntimeException => Source(90 to 99)
  //  }).log("Recover with Retries")
  //    .to(Sink.ignore).run()

  // 4 - Backoff supervision
  import scala.concurrent.duration._

  val restartSource = RestartSource.onFailuresWithBackoff(
    minBackoff = 1 second,
    maxBackoff = 30 seconds,
    randomFactor = 0.2
  )(() => {
    val randomNumber = new Random().nextInt(20)
    Source(1 to 10).map(elem => if (elem == randomNumber) throw new RuntimeException else elem)
  })

  //  restartSource.log("restart backoff")
  //    .to(Sink.ignore).run()

  // 5 - Supervision Strategy on Streams
  val numbers = Source(1 to 20).map(e => if (e == 13) throw new RuntimeException("great number") else e).log("supervision")
  val supervisionNumbers = numbers.withAttributes(ActorAttributes.supervisionStrategy {
    /*
      Resume = skips the faulty element
      Stop = stops the stream
      Restart = resume + clears internal state (for example folds or any component which accumulates internal state)
     */
    case _: RuntimeException => Resume
    case _ => Stop
  })

  supervisionNumbers.to(Sink.ignore).run()
}

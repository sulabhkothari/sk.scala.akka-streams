package AkkaStreamsTechniquesAndPatterns

import java.util.Date

import akka.NotUsed
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}

import scala.concurrent.Future

object IntegratingWithExternalServices extends App {
  implicit val actorSystem = ActorSystem("IntegratingWithExternalServices")
  implicit val materializer = ActorMaterializer()

  def genericExtService[A, B](data: A): Future[B] = ???

  // simplified PagerDuty
  case class PagerEvent(application: String, description: String, date: Date)

  val eventSource = Source(List(
    PagerEvent("AkkaInfra", "Infrastructure Broke", new Date),
    PagerEvent("FastDataPipeline", "Illegal elements in the data pipeline", new Date),
    PagerEvent("AkkaInfra", "A service stopped responding", new Date),
    PagerEvent("SuperFrontEnd", "A button doesn't work", new Date)
  ))

  //import actorSystem.dispatcher
  implicit val dispatcher = actorSystem.dispatchers.lookup("dedicated-dispatcher")

  class PagerActor extends Actor with ActorLogging {
    private val engineers = List("John", "Daniel", "Jimmy")
    private val emails = Map(
      "Jimmy" -> "Jimmy@rockthejvm.com",
      "Daniel" -> "daniel@rockthejvm.com",
      "John" -> "John@rockthejvm.com"
    )

    private def processEvent(pagerEvent: PagerEvent) = {
      val enginnerIndex: Long = pagerEvent.date.toInstant.getEpochSecond / (24 * 3600) % engineers.length
      val engineer = engineers(enginnerIndex.toInt)
      val engineerEmail = emails(engineer)

      println(s"Sending engineer $engineerEmail a high priority notification: $pagerEvent")
      Thread.sleep(1000)

      engineerEmail
    }

    override def receive: Receive = {
      case pagerEvent: PagerEvent =>
        sender() ! processEvent(pagerEvent)
    }
  }

  val infraEvents = eventSource.filter(_.application == "AkkaInfra")
  //  val pageEngineerEmails: Source[String, NotUsed] =
  //    infraEvents.mapAsync(parallelism = 3)(event => PagerService.processEvent(event))
  // mapAsync guarantees the relative order of elements regardless of which future is faster or slower
  //  or use mapAsyncUnordered (is faster)
  // Parallelism increases throughput

  val pagedEmailSink = Sink.foreach[String](email => println(s"Successfully sent notification to email: $email"))
  //pageEngineerEmails.to(pagedEmailSink).run()
  val pagerActor = actorSystem.actorOf(Props[PagerActor], "PagerActor")

  import akka.pattern.ask
  import scala.concurrent.duration._
  import akka.util.Timeout
  implicit val timeout = Timeout(2 seconds)
  val alternativePagedEngineerEmails = infraEvents.mapAsync(parallelism = 4)(event => (pagerActor ? event).mapTo[String])
  alternativePagedEngineerEmails.to(pagedEmailSink).run()

  // do not confuse mapAsync with async (ASYNC boundary)
}

package Graphs

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

object MoreOpenGraphs extends App {
  implicit val actorSystem = ActorSystem("MoreOpenGraphs")
  implicit val materializer = ActorMaterializer()

}

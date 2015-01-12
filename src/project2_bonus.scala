import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.ActorSystem

object project2_bonus extends App{
	
	var myActorSystem = ActorSystem("Gossip")
	var masterActor = myActorSystem.actorOf(Props(new Master(args(0).toInt, args(2))))
	masterActor ! (args(1))
}
import akka.actor.ActorSystem;
import akka.actor.Props;

import com.typesafe.config.ConfigFactory

object project3 extends App{
	
	val myActorSystem= ActorSystem("Pastry")
	
	/**
	 * Create a master actor
	 */
	var masterActor = myActorSystem.actorOf(
	    Props(new Master(args(0).toInt, args(1).toInt)), name = "MasterActor")
	    
	masterActor ! "CreatePeers"

}


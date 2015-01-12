import akka.actor.ActorSystem
import akka.actor.Props
import com.typesafe.config.ConfigFactory
//import spray.can.Http
//import akka.io.IO
import akka.io.IO
import spray.can.Http


object project4server extends App{
  
	 implicit val myActorSystem = ActorSystem("TwitterServer", ConfigFactory.load("application.conf"))
	
	/**
	 * Create a master actor
	 */
    var noOfUsers=args(0).toLong
    
    /**
   * Create a listener which will listen to the incoming http requests
   */
   val listener = myActorSystem.actorOf(Props(new Listener(noOfUsers)), name = "listener")
  
  
  /**
   * Create a http server
   */

   IO(Http) ! Http.Bind(listener, interface = "192.168.0.38", port = 8080)
    
	
}

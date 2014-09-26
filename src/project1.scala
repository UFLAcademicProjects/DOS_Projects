import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import com.typesafe.config.ConfigFactory

object project1 {
/**
 * This file creates the actor system on every machine within the distributed system 
 * and creates BigBoss actor on Remote.
 * It also creates Master actors on all the machines within the distributed system.
 * 
 */
	def main(args: Array[String]) {
  
		/*Create a system*/
		
  
		/*These are the number of zeros to be checked in the hash for bitcoins*/
		
		
		//println("Remote system")
  
		/*If the argument passed is the number of zeros then it is the Remote system.
		 * Argument passed for the local machines is the IP address of the Remote system*/
		if(args(0).length()==1)
		{
		  val config = ConfigFactory.load()
		  var system = ActorSystem("bitcoinServer",config.getConfig("bitcoinServer"))
		  var noOfZeros:Int=0
			noOfZeros=Integer.parseInt(args(0))
			//println("Number of zeros to be checked :: " + noOfZeros)
			/*Create the big boss actor on the remote system*/
			val bigBoss=system.actorOf(Props(new BigBoss(noOfZeros)),name = "BigBoss")
			/*Create the master for the system*/
			var master = system.actorOf(Props(new Master(system)),"master")
			bigBoss.tell("Give me work", master)
			/**
			 * Create timer only on controlling machine
			 */
			var timer = system.actorOf(Props[Timer], "timer")
			timer.tell("start", bigBoss)
		}
		else
		{
		  	  val config = ConfigFactory.load()
		  var system = ActorSystem("bitcoinServer",config.getConfig("bitcoinClient"))
		  var noOfZeros:Int=0
			//println("Local system")
			/**
			 * Since Big Boss is created only on one system, we just need to perform a lookup for it. 
			 * We need not create it again on every local machine
	   		*/
			val bigBoss=system.actorSelection("akka.tcp://bitcoinServer@" + args(0)+ ":2552/user/BigBoss");
			var master = system.actorOf(Props(new Master(system)),"master")
			bigBoss.tell("Give me work", master)			
		}
		
    
	}
  
}

import akka.actor.Actor
import akka.actor.Props

import scala.util.Random

class Master(val numOfPeers: BigInt, val numOfRequests:Long) extends Actor{
  
  /**
   * This map contains the x,y locations of the peers stored against the actor number
   */
  var actorLocations: Map[BigInt,String] = Map()
  var random:Random=new Random
  
  
  def receive={
    
    case "CreatePeers" =>
      
      	println("Create Peers")
      	
        var start:BigInt = 0
        var xcor:BigInt = 0
        var ycor:BigInt = 0
        /**
         * 	Create peers one by one
         */        
        for(count <- start until numOfPeers){
          
          /**
           * Generate random x,y locations
           */
          xcor = random.nextLong()
          ycor = random.nextLong()
          
          /**
           * Generate actor
           */
          var actorName:String = "Peer" + count.toString;
          /**
           * We are using a specific delimiter 
           * to combine and store x,y locations of the peer
           */
          var combinedLocation:String = xcor.toString + "::" + ycor.toString
          var peerActor = context.actorOf(Props(new Peer(count, xcor, ycor)), name = actorName);
          /**
           * We need to make sure that we consider 
           * only THOSE neighbors whose state tables are initialized completely.
           * Hence immediately after the creation of the peer, send a message to the peer to
           * create state tables.
           */
          peerActor ! ("Create State Tables", actorLocations)
          /**
           * After sending peer the map containing previous peers, 
           * add this peer info in the map and proceed with the creation of next peer
           */
          actorLocations +=  (count -> combinedLocation)
        }
    
    case _=>
    	println("-----------Default case Master");
      
  }
  
  
}
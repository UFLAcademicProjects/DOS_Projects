import akka.actor.Actor
import akka.actor.Props
import scala.util.Random
import akka.actor.ActorSelection

class Master(val numOfPeers: BigInt, val numOfRequests:Long) extends Actor{
  
  /**
   * This map contains the x,y locations of the peers stored against the actor number
   */
 // var actorLocations: Map[BigInt,String] = Map()
  /**
   * Contains all the actors created
   */
  var actorList:List[BigInt] = List()
  var random:Random=new Random
  
  
  
  def receive={
    
    case "CreatePeers" =>
      
      	println("Create Peers")
      	
      	/*Because 0th peer is created separately*/
        var start:BigInt = 1
        var xcor:BigInt = 0
        var ycor:BigInt = 0
        
        /**
         * Create the initial peer
         */
        var zeroActorName:BigInt = 0
        var zeroPeerActor = context.actorOf(Props(new Peer(zeroActorName, numOfPeers)), name = 0.toString);
      	      	
        self ! ("State Tables Ready", zeroActorName)
      	
    case ("State Tables Ready", peerNumber:BigInt) =>
        /**
         * Once the peer is ready with the state tables, it sends it back to the master
         */
      	actorList = actorList :+ peerNumber
      	
      	if(actorList.size < numOfPeers){
      	  
      	  var count:BigInt =  actorList.size
      	  /**
      	   * Create the subsequent peer
         */
      	  var actorName:BigInt = count
          var newCreatedPeer = context.actorOf(Props(new Peer(actorName, numOfPeers)), name = count.toString);
      	
      	  var randomNeighbor = random.nextInt(actorList.size)
      	//  println("Value of random neighbor is " + actorList(randomNeighbor))
     	  var actorSel : ActorSelection = context.actorSelection(actorList(randomNeighbor).toString)
      	  newCreatedPeer ! ("Create State Tables", actorSel)
      	}
      	else{
      	  context.actorSelection("698") ! ("Route", (BigInt)(11), 0)
      	  context.actorSelection("398") ! ("Route", (BigInt)(11), 0)
      	  context.actorSelection("39") ! ("Route", (BigInt)(110), 0)
      	  context.actorSelection("20") ! ("Route", (BigInt)(27), 0)
      	  context.actorSelection("40") ! ("Route", (BigInt)(33), 0)
      	  //context.actorSelection("698") ! ("Route", (BigInt)(11), 0)
      	  //context.actorSelection("698") ! ("Route", (BigInt)(11), 0)
      	}
    
    case _=>
    	println("-----------Default case Master");
      
  }
  
  
}
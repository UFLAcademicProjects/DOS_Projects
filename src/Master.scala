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
  //var peerList:
  /**
   * This list represents those nodes 
   * who have notified the master that they have sent all the requests.
   */
  var peersFinishedWork: List[BigInt] = List()
  /**
   * Total number of messages which will be floating in the network will be 
   * number of requests * total no of peers in the network
   */
  val totalMessagesInNetwork:BigInt = numOfPeers * numOfRequests
  
  var notificationReceivedFrom :BigInt = 0;
  
  var totalHopCounts:BigInt = 0;
  
  var printFlag:Boolean = true;
  
  def receive={
    
    case "CreatePeers" =>
      
     // 	println("Create Peers")
      	
      	/*Because 0th peer is created separately*/
        var start:BigInt = 1
        var xcor:BigInt = 0
        var ycor:BigInt = 0
        
        /**
         * Create the initial peer
         */
        var zeroActorName:BigInt = 0
        var zeroPeerActor = context.actorOf(Props(new Peer(zeroActorName, numOfPeers, numOfRequests)), name = 0.toString);
      	      	
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
//      	  var actorName=random.nextInt(50000)
//      	  while(actorList.contains(actorName))
//      	    actorName=random.nextInt(50000)
//      	  
      	  
      	  var actorName=count
          var newCreatedPeer = context.
            actorOf(Props(new Peer(actorName, numOfPeers, numOfRequests)), name = actorName.toString);
      	  var randomNeighbor = random.nextInt(actorList.size)
     	  var actorSel : ActorSelection = context.
     	  	actorSelection(actorList(randomNeighbor).toString)
      	  newCreatedPeer ! ("Create State Tables", actorSel)
      	  
      	}else{
      	  /**
      	   * Notify peers to start scheduler
      	   */
      	  
      	 // println(actorList)
      	  for (peer <- actorList){
      	    var actorSel : ActorSelection = context.actorSelection(peer.toString)
      	    actorSel ! ("Start Scheduler",actorList)
      	    // actorSel ! ("debug")
      	    
      	   // Thread.sleep(100);
      	  }
      	  
      	}
      	
    case ("Received the message", hopCount:Int) =>
      
      if(notificationReceivedFrom < totalMessagesInNetwork-1){
          
    	  totalHopCounts = totalHopCounts + hopCount;
    	  notificationReceivedFrom  += 1;
    	//  println("Notification received from "+ notificationReceivedFrom);
      }else{
         totalHopCounts = totalHopCounts + hopCount;
    	 notificationReceivedFrom  += 1;
        /**
         * Total number of messages which were supposed 
         * to float in the network are finished, we need to terminate the system
         */
    	// println("hopcount is "+totalHopCounts+ " notifs received "+notificationReceivedFrom)
    	 if(printFlag){
    		 println("Average number of hopcount is " + totalHopCounts.doubleValue/notificationReceivedFrom.doubleValue);
    		 printFlag = false;
    	 }
         
         context.system.shutdown;        
      }
      
    case _=>
    	println("-----------Default case Master");
      
  }
  
}
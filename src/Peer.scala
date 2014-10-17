import akka.actor.Actor
import javax.xml.crypto.dsig.keyinfo.KeyValue

class Peer(val peerNumber:BigInt, val xcor:BigInt, val ycor:BigInt) extends Actor{

  var pathString:String = "../";
  
  def receive ={
    
    case ("Create State Tables", currentPeers:Map[BigInt, String]) =>
      
      println("Inside Create state tables" + currentPeers + "for the peer " + peerNumber);
      if(currentPeers.size > 0){
    	  findGeographicallyClosestPeer(currentPeers)
      }
      
    case ("Join", keySentByPeer:BigInt) =>
    
      println("Join sent by " + keySentByPeer + " to the peer " + peerNumber)
      
    case _ =>
      println("Default Peer case");
    
    
  }
  
  /**
   * This function calculates geographically 
   * closest neighbor for this peer
   */
  def findGeographicallyClosestPeer(currentPeers:Map[BigInt, String]){
    
    /**
     * Iterate over the map and try to find closest neighbor
     */
    var fellowPeerNum:BigInt = 0
    var fellowPeerLocation:String = null
    var shortestDistance:Double = 0
    var isFirst:Boolean = true
    /**
     * Till the time, no closest peer is found, 
     * peer is closest to itself, hence the initialization
     */
    var closestPeerNum = peerNumber 
    
    currentPeers.foreach {
      	
    	case (key, value) => 
    	  	fellowPeerNum = key; 
    	  	fellowPeerLocation = value;
    	  	
    	  	/**
    	  	 * Separate x, y coordinates which are separated by "::"
    	  	 */
    	  	
    	  	var locationCoordinates:Array[String] = fellowPeerLocation.split("::");
    	  	  
    	  	var xDistanceSquare:Double =  Math.abs(locationCoordinates(0).toDouble - xcor.doubleValue) * Math.abs(locationCoordinates(0).toDouble - xcor.doubleValue)
    	  	var yDistanceSquare:Double =  Math.abs(locationCoordinates(1).toDouble - ycor.doubleValue) * Math.abs(locationCoordinates(1).toDouble - ycor.doubleValue)
    	  	
    	  	var squareRootdistanceValue:Double = Math.sqrt(xDistanceSquare + yDistanceSquare);
    	  
    	  	println("For Peer " + peerNumber  + " Shortest distance is " + shortestDistance + " current distance is " + squareRootdistanceValue)
    	  	
    	  	/**
    	  	 * To kick start
    	  	 */
    	  	if(isFirst){
    	  	  shortestDistance = squareRootdistanceValue
    	  	  closestPeerNum = fellowPeerNum
    	  	  isFirst = false
    	  	  println("****  For Peer number " +  peerNumber  +" Closest Peer is " + closestPeerNum + " and shortest distance is "+ shortestDistance);
    	  	}
    	  	
    	  	if(squareRootdistanceValue < shortestDistance){
    	  	  /**
    	  	   * If the distance is less than the shortest found so far
    	  	   * then update the shortest distance 
    	  	   */
    	  		shortestDistance = squareRootdistanceValue;
    	  		closestPeerNum = fellowPeerNum;
    	  		println("--------------For Peer number " +  peerNumber  +" Closest Peer is " + closestPeerNum + " and shortest distance is "+ shortestDistance);
    	  	}
    	  	
    }
    /**
     * After the loop ends, peer must have found its closest neighbour.
     * So it should send a "Join" message to that peer with its own key.
     */
     
     var temp = context.actorSelection(pathString + "Peer" +closestPeerNum.intValue())
     temp ! ("Join", peerNumber ) 		 
    
  }

  
  
}
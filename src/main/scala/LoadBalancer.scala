import scala.concurrent._

import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout

case class tweet(userId:Long,message:String)
case class followerList(userId:Long, followersList:List[Long])

class LoadBalancer(var ownId:Int, var noOfUSers:Long, var serverLocations:Map[String, ActorRef]) extends Actor {
  
  /**
   * This list maps user id to a map.
   * The value map maps server id to the follower 
   */
  var userFollowers: Map[Long,Map[Int, List[Long]]] = Map()
  
  var noOfServers = serverLocations.size
  //var blockSize = (noOfUSers /noOfServers).intValue() 
  var inCount=0
  println("Created load balancer successfully");

  def receive = {
    
    /**
     * This message will be sent by users.
     */
    case (userId:Long, followersLists:List[Long]) =>
      
      	//println("Follower id list received " + followersList + " from user " + userId + "in load balancer " + ownId);
        
    	var followersList=followersLists:+userId
      
      
      	/**
      	 * Each server stores tweets of fixed range of users.
      	 * So when we get a user id, we need to map it to the appropriate server and
      	 * send an actorReference of the user to that server so that it can send messages 
      	 * to that user in the future.
      	 */
    	//var serverNo:Int=((userId.doubleValue()/blockSize .doubleValue()).ceil).intValue
    	var serverNo:Int= (userId.intValue % noOfServers)+1
    	/**
    	 * Last server instance will have some more users than other servers
    	 */
    	
    	
    	/**
    	 * This map stores the mapping of server to followers.
    	 * We keep this mapping so that when a tweet is received, 
    	 * it can be redirected to appropriate server which maintains a tweet queue of those followers.
    	 */
    	var serverFollowerMap:Map[Int, List[Long]] = Map()
        /**
         * Iterate over followers to find out 
         * the server which will store the tweets for them
         */
    	for(iterateFollower <- followersList){
    	  /**
    	   * Find the server who stores tweets of this follower
    	   */
    	  //var serverForFollower:Int = ((iterateFollower.doubleValue()/blockSize .doubleValue()).ceil).intValue
    	  var serverForFollower:Int = (iterateFollower.intValue() % noOfServers) + 1
    	  
    	  if(serverFollowerMap.contains(serverForFollower)){
    	    /**
    	     * If the list for that server already exists then push the element into that
    	     */
//    	    println()
    	    var sampleFollowerList:List[Long] = serverFollowerMap(serverForFollower)
    	    sampleFollowerList = sampleFollowerList :+ iterateFollower
    	    serverFollowerMap += (serverForFollower -> sampleFollowerList)
    	    
    	  }else{
    	    var sampleFollowerList:List[Long] = List()
    	    sampleFollowerList = sampleFollowerList :+ iterateFollower
    	    serverFollowerMap += (serverForFollower -> sampleFollowerList)
    	  }
    	  
    	}
      
    	userFollowers += (userId -> (serverFollowerMap))
    	
    /**
     * This message is received by the user.
     */
    	
    case ("getMyTweets",userid:Long)=>
       
      // println("user followers \n"+userFollowers )
       
        /**
		  * Map the user id to the appropriate server and send a message to fetch tweets.
		  */
		  //var serverNo:Int=((userid.doubleValue()/blockSize .doubleValue()).ceil).intValue
    	  var serverNo:Int= userid.intValue % noOfServers+1
		 
		  
/*
 *     send out a future to fetch tweet list from the servers
 */		   
        implicit val timeout = Timeout(1 second)
    	var future = serverLocations ("server"+serverNo.toString()) ? (userid)
    	var result = Await.result(future, timeout.duration).asInstanceOf[List[String]]
        //inCount=inCount+1
    	sender ! result
  
  
  
  
    case ("storeMyTweet",userId:Long, message:String) =>
             	  
       // println("Tweet received from user id "+ userId);
        	  
	     /**
	       * If we do not find a specific string,
	       * then it is a tweet posted by user
	       */
        	  
		 /**
		   * Get the followers of this user
		   */
        	var serverFollowerMap = userFollowers(userId)
        /**
         * Iterate over this map and add the message to the queue of each follower 
         * on the respective server
         */
        	for((serverKey, followerListVal) <- serverFollowerMap){         
        /**
         * Server follower map stores server id as a key.
         * Convert it into name to get actor location from serverlocation map 
         * and then send a message.
         */
        		serverLocations ("server"+serverKey.toString()) ! (message, followerListVal)  
        	
        	inCount=inCount+1 
        	
        	}
      
      case "ping"=>
        
      sender ! "kya bana rahe ho"
      
  	case _=>println ("loadBalancer default case")
    
  }

}

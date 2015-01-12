import akka.actor.Actor
import akka.actor.ActorRef
import scala.collection.mutable.Queue
import akka.actor.PoisonPill

case class subscribedTweetList(tweetList:List[String])
class Server(var ownId:Int, var noOfUSers:Long) extends Actor {

	var maxAllowedQueueSize:Int = 100
	/**
	 * User ids versus the subscribed tweets queue
	 */
	var userTweets:Map[Long, Queue[String]] = Map()
	/**
	 * Users to their locations mapping
	 */
	
	/**
	 * No. of tweets processed
	 */
	var count:Long=0
	var highCount:Long = 0
	var mediumCount:Long = 0
	var lowCount:Long = 0
	var incomingCount:Long = 0
	var incomingHighCount:Long = 0
	var incomingMediumCount:Long = 0
	var incomingLowCount:Long = 0
	
	println("Created server successfully");
	
	def receive = {
	  
	  /*
	   * case of get my tweets from the server  
	   */
	  case (userId:Long) => 
	    
	  //  println("inside server "+ ownId + " , for user "+userId)
	    if(!userTweets.contains(userId))
	        {
	          var temp:Queue[String]=Queue()
	          userTweets +=(userId->temp)
	        }
	        var sampleTweetQueue:Queue[String] = userTweets(userId)
	        count=count+sampleTweetQueue.size
	        sender ! sampleTweetQueue.toList
	        
	    
	    case (tweetMessage:String, followerIdList:List[Long]) =>
	      
	   //   println("In case to post tweet in server");
	      /**
	       * Iterate over the follower list and add the tweet 
	       * to their queue
	       */
	      for(iterateFollower <- followerIdList){
	        
	        if(!userTweets.contains(iterateFollower))
	        {
	          var temp:Queue[String]=Queue()
	          userTweets +=(iterateFollower->temp)
	        }
	        var sampleTweetQueue:Queue[String] = userTweets(iterateFollower)
	        /**
	         * Tweet queue has a limited size. so if the size is reached,
	         * dequeue the oldest element and add new one in the queue.
	         */
	        if(sampleTweetQueue.size == maxAllowedQueueSize){
	        	sampleTweetQueue.dequeue
	        	sampleTweetQueue.enqueue(tweetMessage)
	        }else{
	          	/**
	          	 * Push the tweet message to users which had subscribed for it
	          	 */
	        	sampleTweetQueue.enqueue(tweetMessage)
	        }
	      
	        
	        /**
	         * Increment the number of tweets
	         */
	       
	        if(tweetMessage.equalsIgnoreCase("high")){
	          highCount = highCount + 1
	        }else if(tweetMessage.equalsIgnoreCase("medium")){
	          mediumCount = mediumCount + 1
	        }else if(tweetMessage.equalsIgnoreCase("low")){
	          lowCount = lowCount + 1
	        }
	       
	      }
	      /**
	       * Increment the number of tweets processed additional 2 times 
	       * to account for incoming and outgoing tweet at the sender side itself.
	       */
	      incomingCount=incomingCount+2
	      if(tweetMessage.equalsIgnoreCase("high")){
	          incomingHighCount  = incomingHighCount + 2
	        }else if(tweetMessage.equalsIgnoreCase("medium")){
	          incomingMediumCount  = incomingMediumCount + 2
	        }else if(tweetMessage.equalsIgnoreCase("low")){
	          incomingLowCount  = incomingLowCount + 2
	        }
	   
	      
	    case _=>println("Server default case")
	    
	}
	
	override def postStop(){
	  
	  /**
	   * Send poison pill to 
	   * all the users
	   */

	  println("Outgoing count is " + count);
	  println("Outgoing highcount is " + highCount);
	  println("Outgoing mediumcount is " + mediumCount);
	  println("Outgoing lowcount is " + lowCount);
	  
	  println("Incoming count is " + incomingCount);
	  println("Incoming highcount is " + incomingHighCount);
	  println("Incoming mediumcount is " + incomingMediumCount);
	  println("Incoming lowcount is " + incomingLowCount);
	  context.parent ! (count + incomingCount , "Final Stats")
	  
	}
}
import akka.actor._
/*import akka.actor.Props
import akka.actor.ActorRef
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.PoisonPill
import akka.io.IO
import spray.can.Http
import spray.routing._*/
import spray.routing.HttpService
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import org.json4s.JsonAST.JObject
import spray.httpx.Json4sSupport
import org.json4s.Formats
import org.json4s.DefaultFormats
import scala.util.Random
import spray.routing._
import akka.actor.Cancellable
import scala.concurrent.ExecutionContext.Implicits.global


case class TweetWrapper(postedBy:Long, contentString: String){}

/**
     * This master actor creates all the load balancer actors and server actors
     */
//class Listener(var noOfUsers:Long) extends Actor with TwitterService{
  class Listener(var noOfUsers:Long) extends {var noOfUsers1 = noOfUsers}  with Actor with TwitterService{
  
  def actorRefFactory = context
    
  def receive = runRoute(rootRoute)
	
}
trait TwitterService extends HttpService with Actor with Json4sSupport{
  
	
	
  
	implicit def json4sFormats: Formats = DefaultFormats
	
	
	
	
	
	
	var minCount:Int=0
	var cores = Runtime.getRuntime().availableProcessors();
  	/**
  	 * This map stores server ids versus server actor references.
  	 * We need that because we need to send Poison pill to servers at the end.
  	 */
  	var serverLocations: Map[String, ActorRef] = Map()
	/**
	 * We have load balancer and servers in the proportion of 1:3 of total number of cores available
	 */
	var noOfUsers1:Long
	var noOfLoadBalancers:Int = cores/4
	var noOfServers = cores - noOfLoadBalancers
	var totalCount:Long=0
	println("Final val is "+ noOfUsers1);
	
  	/**
  	 * Create server actors
  	 */
	for(i <- 1 to noOfServers){
	    var serverName:String = "server"+ i.toString
	    var serverLocation:ActorRef = context.actorOf(Props(new Server(i,noOfUsers1)), name = serverName);
	    serverLocations = serverLocations + (serverName -> serverLocation)
	}
  	/**
  	 * Create load balancers actors
  	 */
  	var loadBalancers: List[ActorRef] = List()
	for(i<- 1 to noOfLoadBalancers){
	  var temp=context.actorOf(Props(new LoadBalancer(i,noOfUsers1, serverLocations)), name = "LB"+i.toString);
	  loadBalancers=loadBalancers:+temp
	}
   
  	/**
  	 * Create users
  	 */
  	
  	val high = 5
  	val medium = 12
  	val low = 83

  	var randomObj : Random=new Random()
  	var actorRef: ActorRef=null
  	var startRange:Long = 0 
	var endRange:Long = noOfUsers1 - 1
	
	var userFollowerMap:Map[Long, List[Long]] = Map[Long, List[Long]]()

	//Create High profile users and their follower list
	var iteratorEnd:Long = startRange+((endRange- startRange)*high/100)
 
  
	println("start high profile users: "+startRange)
	println("End high profile users: "+iteratorEnd)
  
	for(i<- startRange to iteratorEnd )
	{
		 
		  var numFollowers:Long = Utils.randomRange(1,10)
		  var followerList : List[Long] = List()
		  for(j<- 1.toLong to numFollowers)
		  {
			    var random:Long=0;
			  		do {
			  			random = Utils.nextLong(randomObj, noOfUsers1)+1
			  		}while (random == i || followerList.contains(random) )
			    followerList = (random)::followerList
			    userFollowerMap += i->followerList
		  }
		 // userFollowerMap.foreach(println);
	  }

  		//Create Medium profile users and their follower list  
		var iteratorStart = iteratorEnd+1
		iteratorEnd = iteratorStart+ ((endRange- startRange)*medium/100)
		println("start medium range: "+iteratorStart)
		println("End medium range: "+iteratorEnd)
  
		for(i<- iteratorStart to iteratorEnd )
		{
		  
	  	  //follower creation
		  var numFollowers = Utils.randomRange(1 ,50)
		  var followerList : List[Long] = List()
		  for(j<- 1 to numFollowers)
		  {
			    var random:Long=0;
			  		do {
			  			random = Utils.nextLong(randomObj, noOfUsers1)+1
			  		}while (random == i || followerList.contains(random) )
			    followerList = (random)::followerList
			    userFollowerMap += i->followerList
		  }
		 //userFollowerMap.foreach(println);
		}
  
		println("*****Created Medium profile users*****")
		
		//Create Low profile users and their follower list
		iteratorStart = iteratorEnd + 1
		println("start low range: "+iteratorStart)
		println("End low range: "+endRange)
		for(i<- iteratorStart to endRange)
		{
			var numFollowers = Utils.randomRange(1, 5)
			var followerList : List[Long] = List()
			for(j<- 1 to numFollowers)
			{
			    var random:Long=0;
			  		
			  		do {
			  			random = Utils.nextLong(randomObj, noOfUsers1)+1
			  		}while (random == i || followerList.contains(random))
			    followerList = (random)::followerList
			    userFollowerMap += i->followerList
			}
			//userFollowerMap.foreach(println);
		}
  
		println("*****Created Low profile users*****")
 
  
		//Create Low profile users and their follower list
		iteratorStart = iteratorEnd + 1
		println("start low range: "+iteratorStart)
		println("End low range: "+endRange)
		for(i<- iteratorStart to endRange)
		{
			var numFollowers = Utils.randomRange(1, 5)
			var followerList : List[Long] = List()
			for(j<- 1 to numFollowers)
			{
			    var random:Long=0;
			  		
			  		do {
			  			random = Utils.nextLong(randomObj, noOfUsers1)+1
			  		}while (random == i || followerList.contains(random))
			    followerList = (random)::followerList
			    userFollowerMap += i->followerList
			}
			//userFollowerMap.foreach(println);
	  }
		
	for(iterateLoadBalancersList <- loadBalancers){
	  
	  for((key, value) <- userFollowerMap){
		  	
		  	iterateLoadBalancersList ! (key, value)
	    
	  }
	  
	  
	}	
	
	
	
	context.system.scheduler.schedule(1.seconds, 10.seconds){	
	
	println("the count per second for the min is :          "+(totalCount/10))
	totalCount=0
	
	
	}
	
	
	

  	def pingRoute = path("tweet"/LongNumber) {(id)=>
    
	    get {
	    	implicit val timeout = Timeout(5 minutes)
			/**
	         * Select a load balancer randomly
	         */
	    	var random:Random = new Random
	    	var selectedLoadBalancer = random.nextInt(noOfLoadBalancers)
	    
	    	val future = loadBalancers(selectedLoadBalancer) ? ("getMyTweets",id)
	    	var result = Await.result(future, timeout.duration).asInstanceOf[List[String]]
	    	
	    	totalCount=totalCount+result.size
	    	var resultString = new StringBuilder("{\"GetYourTweets\":[")
	    	
	    	for(listMember <- result){
	    	  
	    	  resultString.append(listMember+",")
	    	}
	    	
	    	resultString.append("]}")
//	    	println("get")
	    	complete(resultString.toString)    	
	    }   
   
  }

  def postTweetRoute = path("posttweet") {
	
    var postedByUser:Long =0
	  var contentStr:String = ""
	  var selectedLoadBalancer=0
   
	  post {
	    
	    entity(as[JObject]){
    	    tweetJson => 
    	      complete{
    	
    	        val tweetWrapperObj:TweetWrapper = tweetJson.extract[TweetWrapper]
    	        /**
    	         * Select a load balancer randomly
    	         */
    	         postedByUser = tweetWrapperObj.postedBy
    	         contentStr= tweetWrapperObj.contentString
    	         var random:Random = new Random
	  	 selectedLoadBalancer = random.nextInt(noOfLoadBalancers)
	  	// println("testing here post count before"+totalCount)
	  	 totalCount=totalCount+1
	  	// println("testing here post count after"+totalCount)
//	  	println("post")
	  	 loadBalancers(selectedLoadBalancer) ! ("storeMyTweet", postedByUser, contentStr)		 
	  	 tweetWrapperObj
    	        
    	      }
    	      
    	}
    	         
     }
	  			
  }
  
  def pongRoute = path("pong") {
    
    get { 
    println("in pong")
      println("\n\ntweets processed are:" +totalCount)
      complete(totalCount.toString()) }
  }

  
  
  	def defaultRoute = path("default"/LongNumber) {(id)=>
    
	    get {
	    	println("in default")
	    	println("current count is :"+totalCount)
	    	complete(totalCount.toString)    	
	    }   
   
  }

  
  
  
  
  
  def rootRoute = pingRoute ~ pongRoute ~ postTweetRoute ~ defaultRoute
  
  object Utils{
	  def random(number:Int):Int={
		var random= Random.nextInt(number)
		return random
  }
  
  def randomRange(min :Int , max :Int) : Int =
  {
    return Random.nextInt(max- min +1)+min
  }
  
  def nextLong(random: Random, max: Long) : Long= {
 
		var bits:Long=0
		var value : Long=0
		do {
			bits = (random.nextLong() << 1) >>> 1;
			value = bits % max;
		} while (bits-value+(max-1) < 0L);
   		return value;
  	}
  
  }
  

}

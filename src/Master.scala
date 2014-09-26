import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.actor.ActorSystem


class Master(var mysystem:ActorSystem) extends Actor{
  
/**
   * Keep the bigBoss reference
   */
  var boss:ActorRef = null
  /**
   * Range variables
   */
  var start:BigInt=0
  var end:BigInt=0
  /*This seed is specific to every master*/
  var extraSeed:String=""
    
  var globalBitCoins:Map[String,String] = Map();
  
  var numberOfCores:Int = Runtime.getRuntime().availableProcessors();
  var numberOfWorkers:Int = 1;
  /*Unit of work per worker*/
  var batch:BigInt =25000
  
  /*This flag makes sure that workers are instantiated only once per master*/
  var workerFlag=true
  
  var noOfChildren=numberOfCores*numberOfWorkers
  
  var noOfdeadChildren=0
  
  var processedRecords:BigInt=0
  
  //println("master is created")
  
  
  def receive={
    
    /*This case is executed when worker asks for work from master*/
    case (bitcoinMap:Map[String,String],finishedRecords:BigInt)=>
      
      //bitcoinMap.foreach{ case (key, value) => println(key + "\t" + value)}
     processedRecords +=finishedRecords
      if(!bitcoinMap.isEmpty)
      {
    	  	globalBitCoins=globalBitCoins++bitcoinMap
    	  	
      }
      
      if(start + batch < end)
      {
    	  sender ! (start, start+batch, extraSeed)
    	  start =  start + batch
    	  /**
    	   * This ensures that master gets the work from BigBoss 
    	   * when it finishes the range allocated to it
    	   */
    	  if(start + batch >= end)
    	  {
    	   // println ("Asked BigBoss for work")
    	    boss ! "Give me work"
    	  }

      }else
      {
    	  sender ! "wait worker"
      }
      
      /**
       * This case is executed when BigBoss gives the work
       */
    	case (x:BigInt,y:BigInt,z:String,noOfZeros:Int)=>
  
    		boss = sender
    		start=x
    		end=y
    		extraSeed=z
    	//	println("Start :: " + start+" End :: "+end+" ExtraSeed :: "+extraSeed)
  
      
    		//workers instantiation only once
    		if(workerFlag){
    		  
    			for(i<- 0 until (noOfChildren)){
    				context.actorOf(Props(new Worker(noOfZeros)))
    			}
    			workerFlag=false
    	  
    		}
    		/**
    		 * This case is executed when program is stopped
    		 */
    	case "kill children" =>
    	  
    			for(child<-context.children){
    				child ! PoisonPill
    			}
    		/**
    		 * This case is executed every time any worker dies
    		 */
    	case "dead" =>
    		noOfdeadChildren+=1
    		if(noOfdeadChildren==noOfChildren)
    		{
    			boss! (globalBitCoins,processedRecords )    			
    		}
    	
    	case "shutdown" =>
    	  	/**
    	  	 * Shut the entire system down
    	  	 */
    	  	mysystem.shutdown
    		
    	case _=>println("-------------------Master Default Case")
		
  	}

}

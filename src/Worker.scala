import akka.actor.Actor

import akka.actor.ActorSelection
import scala.util.Random
import scala.util.control.Breaks
import scala.collection.mutable.Queue
import akka.actor.Cancellable
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class Worker(actorNumber:Int, totalActors:BigInt) extends Actor{

  var neighbourList:List[ActorSelection] = List()
  var actorList:List[Int] = List()
  var pathString:String = "../"
  var random:Random=new Random
  var startGossip=true
  var gossipCount=0
  var cancellable:Cancellable=null
  var flag=true
  /**
   * Sum and weight values
   */
  var mySumValue:BigDecimal = actorNumber
  var myWeightValue:BigDecimal = 1
  var recentMostValue:BigDecimal = 0
  var convergenceValue:BigDecimal = 0.00000000001
  var valueMatchedCount:Int = 0
  
  def receive ={
        
    case ("Create Neighbours", topologyType:String)=>
      
      if(topologyType.equalsIgnoreCase("Complete")){
        var start:BigInt = 0
        for(count <- start until totalActors){
          if(count!=actorNumber)
          {
            /**
             * All the actors except for the own number are neighbours.
             */
            var temp = context.actorSelection(pathString +count.intValue())
            neighbourList = neighbourList :+ temp
            actorList = actorList :+ count.intValue
          }
        }
        
      }else if (topologyType.equalsIgnoreCase("Line")){
        
        if(actorNumber-1 == -1){
          /**
           * If the actor number is 0, then just add actor with number 1 as neighbour.
           */
          var temp1 = context.actorSelection( pathString + (actorNumber+1).intValue())
          neighbourList = neighbourList :+ temp1
          actorList = actorList :+ (actorNumber+1)
          
        }else if(actorNumber+1 == totalActors){
          /**
           * If the actor number is n-1, then just add actor with number n-2 as neighbour.
           */
          var temp1 = context.actorSelection(pathString + (actorNumber-1).intValue())
          neighbourList = neighbourList :+ temp1
          actorList = actorList :+ (actorNumber-1)
        }else{
          /**
           * If the actor number is neither 0 nor n-1, then add both neighbours.
           */
          var temp1 = context.actorSelection( pathString + (actorNumber-1).intValue())
          neighbourList = neighbourList :+ temp1
          actorList = actorList :+ (actorNumber-1)
          var temp2 = context.actorSelection( pathString + (actorNumber+1).intValue())
          neighbourList = neighbourList :+ temp2
          actorList = actorList :+ (actorNumber+1)
        }
        
      }else if (topologyType.equalsIgnoreCase("2-D")){
        
    	  var squareRoot:Double = Math.ceil(Math.sqrt(totalActors.doubleValue)) 
        
         /**
         * Deciding top neighbour
         */
        if(actorNumber - squareRoot.intValue() >= 0 ){
        	var temp1 = context.actorSelection(pathString+ (actorNumber-squareRoot).intValue())
        	neighbourList = neighbourList :+ temp1
        	actorList = actorList :+ (actorNumber-squareRoot).intValue
        }
    	 /**
         * Deciding bottom neighbour
         */
    	if(actorNumber + squareRoot.intValue() < totalActors){
    		var temp1 = context.actorSelection(pathString+ (actorNumber+squareRoot).intValue())
        	neighbourList = neighbourList :+ temp1
        	actorList = actorList :+ (actorNumber+squareRoot).intValue
    	}
    	 /**
         * Deciding left neighbour
         */
    	if(actorNumber % squareRoot.intValue() != 0){
    		var temp1 = context.actorSelection(pathString+ (actorNumber-1).intValue())
        	neighbourList = neighbourList :+ temp1
        	 actorList = actorList :+ (actorNumber-1)
    	}
    	 /**
         * Deciding right neighbour
         */
    	if(actorNumber % squareRoot.intValue() != squareRoot-1){
    		var temp1 = context.actorSelection(pathString+ (actorNumber+1).intValue())
        	neighbourList = neighbourList :+ temp1
        	actorList = actorList :+ (actorNumber+1)
    	}  
    	  
    	  
      }else if (topologyType.equalsIgnoreCase("Imperfect_2-D")){
          var squareRoot:Double = Math.ceil(Math.sqrt(totalActors.doubleValue)) 
        
         /**
         * Deciding top neighbour
         */
        if(actorNumber - squareRoot.intValue() >= 0 ){
        	var temp1 = context.actorSelection(pathString+ (actorNumber-squareRoot).intValue())
        	neighbourList = neighbourList :+ temp1
        	actorList = actorList :+ (actorNumber-squareRoot).intValue
        }
    	 /**
         * Deciding bottom neighbour
         */
    	if(actorNumber + squareRoot.intValue() < totalActors){
    		var temp1 = context.actorSelection(pathString+ (actorNumber+squareRoot).intValue())
        	neighbourList = neighbourList :+ temp1
        	actorList = actorList :+ (actorNumber+squareRoot).intValue
    	}
    	 /**
         * Deciding left neighbour
         */
    	if(actorNumber % squareRoot.intValue() != 0){
    		var temp1 = context.actorSelection(pathString+ (actorNumber-1).intValue())
        	neighbourList = neighbourList :+ temp1
        	actorList = actorList :+ (actorNumber-1).intValue
    	}
    	 /**
         * Deciding right neighbour
         */
    	if(actorNumber % squareRoot.intValue() != squareRoot-1){
    		var temp1 = context.actorSelection(pathString+ (actorNumber+1).intValue())
        	neighbourList = neighbourList :+ temp1
        	actorList = actorList :+ (actorNumber+1).intValue
    	}
    	/**
    	 * Pick up a random neighbour
    	 */
    	//var randomNumber:Random = new Random
    	var randomActor:Int = 0
    	var loop=new Breaks
		loop.breakable {
	    	while(true){
	    		randomActor = random.nextInt(totalActors.intValue)
	    		if(randomActor == actorNumber){
	    		  
	    		}else if(!actorList.contains(randomActor)){
	    		  loop.break
	    		}
	    	}
	    }
    	var temp1 = context.actorSelection(pathString+ (randomActor))
        neighbourList = neighbourList :+ temp1
        actorList = actorList :+ (randomActor).intValue
        
      }else{
        println("Please learn to spell")
      }
 //     print("Printing neighbours for " + actorNumber + "-->"+ actorList)

      context.parent ! "I created my neighbours"
      
  
    case ("Receive_Push_Sum", sumValue:BigDecimal, weightValue:BigDecimal)=>
      
      //println("Receive push sum case");
      /**
       * This case handles the code when 
       * a part of sum and weight is received by fellow worker.
       */
      	mySumValue += sumValue
      	myWeightValue += weightValue
      	
      	/**
      	 * Select a fellow actor randomly and send part of new sum and weight to it 
      	 * provided convergence has not reached.
      	 */
      	
      	if(Math.abs((mySumValue/myWeightValue).doubleValue - recentMostValue.doubleValue)  <= convergenceValue){
	      valueMatchedCount += 1
	    }
      	else{
      	  valueMatchedCount = 0
      	}
      	recentMostValue = (mySumValue/myWeightValue) 
      	
      	/**
      	 * Check if the count is 3. If yes then 3 recent sum values 
      	 * were almost the same and thus convergence is achieved.
      	 */
      	if(valueMatchedCount == 3){
      	  /**
      	   * Report to the master that convergence has reached.
      	   * 
      	   */
      	  
      	  println("Push-sum Convergence has reached " )
      	  context.parent ! ("Push-sum Convergence is reached", actorNumber, mySumValue, myWeightValue)

      	}else{
      	  /**
      	   * Since convergence is not reached, we need to store this value.
      	   */
      	  recentMostValue  = (mySumValue/ myWeightValue)
      	  mySumValue = mySumValue /2
    	  myWeightValue = myWeightValue /2
    	
    	   /**
      	   * Pick up a random neighbour
      	   */
    	  var randomNumber:Random = new Random
    	  var randomActor:Int = random.nextInt(neighbourList.size)
    	  var temp1=neighbourList (randomActor)
    	  temp1 ! ("Receive_Push_Sum", mySumValue, myWeightValue)
      	  
      	}
      	
      	if(flag)
      	{
      	  cancellable=context.system.scheduler.schedule(0 milliseconds, 100 milliseconds){
      	  var randomActor:Int = random.nextInt(neighbourList.size)
    	  var temp1=neighbourList (randomActor)
    	  temp1 ! ("Receive_Push_Sum", mySumValue, myWeightValue)
      	  }
      	  flag=false
      	  
      	}
    
    case ("Gossip msg",gossip:String) =>

      if(startGossip)
      {
//import context.dispatcher
        cancellable= context.system.scheduler.schedule(100 milliseconds,20 milliseconds)
		  {
			  var neighbor=random.nextInt(neighbourList.size)
		      var temp = neighbourList(neighbor)
		   	  temp ! ("Gossip msg",gossip)		   	  
		  }
        context.parent!("gossip received",actorNumber)
        startGossip=false
      }
      
      gossipCount+=1
      
      if(gossipCount==10)
      {
    	 cancellable.cancel
      }
      
    case _ =>
      	println("Default Worker")
      	
  }
  
  override def postStop(){
    //println("Actor number being killed is "+ actorNumber)
    if(cancellable!=null)
    	cancellable.cancel
  	} 
  
}

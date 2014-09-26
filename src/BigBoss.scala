import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem

/**
 * This actor is the main actor which resides only on the one machine 
 * which allocates work to all the masters who further allocate work to all workers
 */
class BigBoss(noOfZeros:Int) extends Actor{

	var masterSeed="A"
    var masterList:List[ActorRef]=List()
    var masterCount:BigInt = 0
    var globalBitCoinCount:BigInt = 0
    var totalRecords:BigInt=0
    var data:String=""
    var bits:String=""
    def receive={
	   	
    	case (bitcoinMap:Map[String,String],finishedRecords:BigInt)=>
          
    	 
    	  totalRecords +=finishedRecords
    	  	globalBitCoinCount += bitcoinMap.size
    		bitcoinMap.foreach{ case (key, value) => println(key + "\t" + value)}
    		masterCount+=1
    		 data+="records processed by master "+masterCount +":-"+finishedRecords+"\n"
    		 bits+="bitcoins found by master "+masterCount +":-"+bitcoinMap.size+"\n"

    		 /**
    		 * If this equals then all the masters have returned their bitcoins 
    		 * and it is time to shutdown
    		 */
    		if(masterCount == masterList.length){
//    		    println("\nTotal bitcoins mined were "+ globalBitCoinCount)
//    		    println("\ntotal no of records processed "+totalRecords )
//    		    println("\n"+data)
//    		    println("\n"+bits)
    			for( master <- masterList ){
    			  master ! "shutdown"
    			}
    		}
	  	  
    	case "Give me work"=>
    	/** This case is executed when Master on one machine has allocated 
    	 *  the entire range to all its workers and needs more work.
    	 * 
    	 */
    	//println("Master has asked for work")
    	/**
    	 * This is range of strings from BigBoss
    	 * */
    	var start:BigInt=0
    	var end:BigInt=999999999
    	//var end:BigInt=99999
    	sender ! (start,end,masterSeed,noOfZeros)
    	/**
    	 * Seed needs to be modified for every master
    	 */
    	masterSeed+="A"
    	/**
    	 * Keeping master references so that all of them can be notified when program is stopped 
    	 */
    	if(!masterList.contains(sender))
    	{
    		masterList =  masterList:+sender
    	}
    	
    	case "time is up" => 
    	  
    	  for(master <- masterList){
    		  /**
    		   * This is to shutdown the workers.
    		   * We can't kill the masters yet because they haven't yet reported back the bit coins
    		   */
    		  master ! "kill children"
    	  }
    	
    	case _=>println("----------------BigBoss Default Case")
		}
}
import akka.actor.Actor
import java.security.MessageDigest
import scala.util.control.Breaks

class Worker(val k:Int) extends Actor{

	var bitCoinMap:Map[String,String]=Map()
	var digest = MessageDigest.getInstance("SHA-256");
	var seed="sanyogita.ranade"
	var processedRecords:BigInt=0
	  
	//println("Worker created")
	  
	  /**
	   * This is used to ask the work from master
	   */
	context.parent ! (bitCoinMap,processedRecords)
	
def receive={
  /**
   * This is executed when worker gets work from master
   */
  case ( start:BigInt, end:BigInt,extraSeed:String)=>
    
    	bitCoinMap = bitCoinMap.empty
    	for(i<-start to end)
    	{
    		calculateHash(seed+extraSeed+i)
    	}
    	/**
    	 * Send all the bitcoins found in a batch
    	 */
    	processedRecords =end-start
        sender ! (bitCoinMap,processedRecords) 
        
  /**
   *   This is executed in case master does not have work to allocate
   */  
  case "wait worker" => 
        //println("in wait state")
        var waitStart=System.currentTimeMillis()
        while(System.currentTimeMillis()<waitStart+30000){
    
        }
        /**
         * Ask for work again after wait
         */
        bitCoinMap = bitCoinMap.empty
        sender ! bitCoinMap
   
  case _=>println("----------------------------Worker Default Case")
		
        
	}
	

def calculateHash(inputString:String)={
	digest.update(inputString.getBytes("UTF-8"));
	var hash=digest.digest();
	var hashString:String= hash.map( "%02x".format(_) ).mkString("")
	
			var flag=true
			var loop=new Breaks

			loop.breakable {
				for(i<-0 until k){
					if(hashString.charAt(i)!='0')
					{
						flag=false
						loop.break
					}

				}
				
	}
	if(flag){
		bitCoinMap+=(inputString-> hashString)
	}

}

override def postStop{
  context.parent!"dead"
}


}

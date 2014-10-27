import akka.actor.Actor
import javax.xml.crypto.dsig.keyinfo.KeyValue
import akka.actor.ActorSelection
import scala.collection.immutable.TreeSet
import akka.actor.ActorRef
import akka.actor.Cancellable
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random
import scala.util.control.Breaks

class Peer(val ownPeerNumber:BigInt, val totalNumOfPeers:BigInt, var totalNumOfRequests:BigInt) extends Actor{

  var pathString:String = "../";
  var actorNamePrefix:String = "Peer";
  /**
   * Values quoted in the research paper
   */
  var leafSetSize:Int = 16;
  /**
   * Values quoted in the research paper
   */
  var neighborhoodSetSize:Int = 16;
  /**
   * This set contains names of peers which are logically closer to this peer
   */
  var leafList:List[BigInt]= List();
  leafList=leafList:+ownPeerNumber
  /**
   * This set contains names of peers which are geographically closer to this peer
   */
  var neighbourhoodSet:Set[BigInt] = Set();
  
  /**
   * Calculate the number of rows for a routing table.
   * The formula is (log numOfPeers base (2^b)) where b = 4, as quoted in the research paper
   */
  
  var numOfRowsRouting:Int = 16
  /**
   * Routing table contains hexadecimal representation of the actor number.
   * The number of columns is equal to the base used to represent the string
   */
  var numOfColumns:Int = 16
  /**
   * This 2-D contains the hexadecimal representation of the actor name.
   * The reason to have it as hexadecimal is ease of prefix matching.
   */
  var routingTable = Array.ofDim[String](numOfRowsRouting.intValue(), numOfColumns)
  initializeSpecialEntriesRoutingTable();
  /**
   * This list caches all the neighbors on the route from randomly chosen neighbor to logically closest
   */
  var joinPathNeighbors: List[ActorRef] = List();
  /**
   * This indicates how many requests were sent by this peer.
   */
  var numRequestsSent:BigInt = 0;
  var cancellable:Cancellable=null;
  var random:Random = new Random;
    
  def receive ={
    
    case ("Create State Tables", entryPoint:ActorSelection) =>
   
        entryPoint ! ("Join", ownPeerNumber)
      
    case ("Join", keySentByPeer:BigInt) =>
      
      var checkRouting=checkNodeForLeafSet(keySentByPeer)
      if(!checkRouting)
      {
        var routingFlag=checkInRouting(keySentByPeer)
        if(!routingFlag){
        	closestMatch(keySentByPeer)
        }
      }
    /**
     *   This message is sent by one of the nodes in the path to redirect the original node to further node
     */  
    case ("Join_nextHop",nextHopRef:ActorSelection, leafSetOfHop:List[BigInt], routingTablesOfHop: Array[Array[String]], senderPeer:BigInt) =>
      /**
       * Add the sender node to the list so that later on, state tables can be sent to it
       */
      joinPathNeighbors = joinPathNeighbors :+ sender
      /**
       * Update state tables as per received values
       */
      updateStateTables(leafSetOfHop, routingTablesOfHop, senderPeer)
      /**
       * Send the join message to next peer
       */
      nextHopRef ! ("Join", ownPeerNumber)
    
    case ("update", leafSetOfHop:List[BigInt], routingTablesOfHop: Array[Array[String]], senderPeer: BigInt) =>
      /**
       * Update state tables as per received values
       */
      updateStateTables (leafSetOfHop , routingTablesOfHop, senderPeer)
      
    case ("Route", destination:BigInt, hopCount:Int) =>
      
       var checkRouting=routeMessageViaLeafSet(destination,hopCount)
       if(!checkRouting)
       {
    	   var routingFlag=routeMessageViaRoutingTable(destination, hopCount)
    	   if(!routingFlag){
        	routeToClosestMatch(destination, hopCount)
           }
       }
      
    case ("Terminate" , leafSetOfHop:List[BigInt], routingTablesOfHop: Array[Array[String]],neighbor:BigInt) =>  
      
      /**
       * Send updated state tables 
       * to all the nodes on the joining paths
       */
      updateStateTables(leafSetOfHop, routingTablesOfHop, neighbor)
      for(onPath<-joinPathNeighbors ){
        onPath ! ("update",leafList ,routingTable,ownPeerNumber)
      }
      /**
       * Send the updated state tables to all neighbors 
       * from leaf set as well as routing table
       */
      var finalSet=leafList.toSet
      for(row<-0 until 16){
        for(col<-0 until 16){
          if(routingTable (row)(col)!=null && !routingTable (row)(col).equalsIgnoreCase("SPECIAL"))
          finalSet= finalSet+(Integer.parseInt(routingTable(row)(col), 16))
        }
      }
      
      for(dest<-finalSet){
        var destRef=context.actorSelection("../"+dest)
        destRef ! ("update",leafList ,routingTable,ownPeerNumber)
      }
      /**
       * Notify master that my state tables are ready
       */
      context.parent ! ("State Tables Ready", ownPeerNumber)
    
      
    case ("Start Scheduler",actorList:List[BigInt]) =>
       /**
       * Start the scheduler to send the request for every second
       */
       cancellable= context.system.scheduler.schedule(random.nextInt(100) milliseconds,1000 milliseconds){
    	  
          if(numRequestsSent < totalNumOfRequests){
           
        	  /**
               * Choose a random destination
               */
              var loop=new Breaks
              loop.breakable {
            	  while(true){
            		  var destination:BigInt = actorList(random.nextInt(actorList.size))
            		  if(destination != ownPeerNumber){
            			  /**
            			   * Send a route message to route the request
            			   */
            			  self ! ("Route", destination, 0)
            			  loop.break
            		  }
            	  }
              }
    	      numRequestsSent += 1
    	      
    	     
    	  }else{
    	    //println("Num of requests sent are "+ numRequestsSent);
    	    /**
    	     * Stop the scheduler
    	     */
    	    cancellable.cancel
    	  }
       }
    
    case _ =>
      println("Default Peer case");
    
  }
  
  /**
   * This function pre-appends the number of zeros and 
   * ensures that decimal is converted to 16 digit long hex.
   * The reason to have 16 digit long hex string is, Long in scala
   * can represent 2^63 whose hexadecimal representation is 16 digit.
   */
  def prependZeros(inputNumber:BigInt):String = {
    
    var hexString:String = inputNumber.longValue().toHexString;
    
    if(hexString.length() < 16){
	          
    	var numZerosToAppend = 16 - hexString.length()
	    for(count <- numZerosToAppend until 0 by -1){
	    	hexString = "0" + hexString 
	    }
	          
	}
   // println("hexString is " + hexString);
    return hexString;
    
  }
  
  /**
   * This function decides if the newly joined peer is a good candidate for leaf set.
   * If it is then this function replaces some not so good candidate with this one. 
   */
  def checkNodeForLeafSet(newPeer:BigInt):Boolean={
    
   // println("Join leaf set sent by "+ newPeer + " to " + ownPeerNumber + " and set is " + leafList);
    
    var sortedList:List[BigInt] = leafList.sorted
    
    /**
     * split the list into 2 at the point of own node id
     */
    
    var (left,right)=sortedList.splitAt(sortedList.indexOf(ownPeerNumber))
    right = right.drop(1)				//drop the original peer no.
    
    
     /* check in left and right if the key needs to be added*/
     
    if(newPeer<ownPeerNumber)
    {
      if(left.size<8)
      {
        left=left:+newPeer
      }
      else if(newPeer>left.min){
        left = left.drop(1)
        left=left:+newPeer
        
      }
      
    }
    else if(newPeer>ownPeerNumber){
    	if(right.size<8)
    	{
    	  right=right:+newPeer
    	}
    	else if(newPeer<right.max){

    	  right = right.dropRight(1)
    	  right=right:+newPeer
    	}
    }
    
     /* join the updated leafset and sort it*/
     
    
    var temp=left++right:+ownPeerNumber
    sortedList=temp.sorted
    
    
    /*
     * check if the key lies in the leafset range
     * if yes then go through the entire leafset to find the closest logical equivalent ands send a message to 
     * the next hop
     * and return true
     * else
     * return false
     * 
     */
 if(newPeer>=sortedList.min && newPeer<=sortedList.max)
 {
   /*
    * code to find the closest logical equivalent
    */
    var nextHop:BigInt=ownPeerNumber
    var minDiff:BigInt=(newPeer-ownPeerNumber).abs
    for(leafCheck<-sortedList){
		if(leafCheck!=newPeer){
		 
		var temp=(leafCheck-newPeer).abs
			if(temp<minDiff){
			  minDiff=temp;
			  nextHop=leafCheck
			}
		}
	 }
    
    /*
     * check if the closest is curr node or some other node in the leafset
     */
    leafList=sortedList
    if(nextHop!=ownPeerNumber){
      
      var nextHopRef=context.actorSelection("../"+nextHop)
    //  println("Join Leaf set , next hop is different");
      sender ! ("Join_nextHop",nextHopRef, leafList, routingTable, ownPeerNumber)
      
    }else{
      /**
       * This is the terminating condition since you found the closest neighbor.
       * So send terminate message and your state tables to the new node..... sender !
       */
    //  println("Join Leaf set , next hop is same");
      sender ! ("Terminate", leafList, routingTable,ownPeerNumber)
    }
   
    return true
 }  
    /*
     * return false in case the the key is out of the leafset range
     */
    return false
  }
  /*
   * checks in the routing table if the longest prefix math exists.
   */
  
  def checkInRouting(newPeer:BigInt):Boolean= {
	var row=prefixMatch(ownPeerNumber, newPeer)
    var newKeyString=prependZeros(newPeer)
    var col:Int=newKeyString.charAt(row)
    if(col>='a'){
      col=col-'a'+10
    }else{
      col=col-'0'
    }
	var dest=routingTable(row)(col)
	if(dest!=null){
	  var temp=Integer.parseInt(dest,16)
	  var destRef=context.actorSelection("../"+temp)
	 // println("Join routing found the hop " + temp + "for newly joined peer " + newPeer);
	  sender ! ("Join_nextHop",destRef, leafList , routingTable , ownPeerNumber)
	  return true
	}
	return false
  }
  
  /**
   * Returns the row where two IDs start to distinguish
   */
  def prefixMatch(selfKey:BigInt,newPeer:BigInt):Int={
    var selfKeySting=prependZeros(selfKey)
    var newKeyString=prependZeros(newPeer)
    
    for(count<-0 until 16){
      if(selfKeySting.charAt(count)!=newKeyString.charAt(count))
        return count
    }
    return 15
  }
  
  def closestMatch(newPeer:BigInt){
    /**
     * Take union of Leaf set and routing table and find the 
     * numerically closest in that 
     */
      var unionSet = leafList.toSet
     /**
     * Convert hex entries to decimal and add the data to union set
     */
	  
      for(count1 <- 0 until numOfRowsRouting){
        
        for(count2 <- 0 until numOfColumns){
          if(routingTable (count1)(count2)!=null && !routingTable (count1)(count2).equalsIgnoreCase("SPECIAL"))
          unionSet = unionSet.+(Integer.parseInt(routingTable(count1)(count2), 16))
          
        }
      }
      
      var nextHop:BigInt = ownPeerNumber 
      var mindiff:BigInt = (newPeer-ownPeerNumber).abs 
      
      /**
       * Now that the sets are combined, find the numerically closer
       */
      for(iterateSet <- unionSet){
        
        if(iterateSet != newPeer){
          
          var diff:BigInt = (newPeer-iterateSet).abs
          if(diff < mindiff){
            
            mindiff = diff
            nextHop = iterateSet
            
          }
          
        }
        
      }
    //  println("Third case join, destination is  "+ nextHop + "for peer " + newPeer);
      
      var destRef=context.actorSelection("../"+nextHop)
	  sender ! ("Join_nextHop", destRef, leafList, routingTable, ownPeerNumber)
      
  }
  /**
   * This function updates the leaf set and routing table depending upon the state tables received.
   */
  def updateStateTables(leafSetHop : List[BigInt], routingTablesOfHop: Array[Array[String]], senderPeer:BigInt){
        
    /**
     * Combine both the leaf sets
     */
    var unionSet:Set[BigInt] = (leafList ++ leafSetHop).toSet
    
    var sortedList:List[BigInt] = unionSet.toList.sorted
    
     /**
     * split the list into 2 at the point of own node id
     */
    
    var (left,right)=sortedList.splitAt(sortedList.indexOf(ownPeerNumber))
    right = right.drop(1)				//drop the original peer no.
    
    if(left.size > 8){
      /**
       * We need to chop remaining elements from left since size should be 8
       */
      left = left.drop(left.size - 8)
      
    }
    if(right.size > 8){
       /**
       * We need to chop remaining elements from right since size should be 8
       */
      right = right.dropRight(right.size - 8)
      
    }
   // println("Size of the left " + left.size + "  and right " + right.size + " and own peer number is "+ ownPeerNumber);
    leafList = left :+ ownPeerNumber
    leafList = leafList ++ right
    leafList = leafList.sorted
 // println("Size of the leaf set is " + leafList+"    my peer id is "+ownPeerNumber);
    
    /**
     * Copy the rows from the routing table ONLY till the point the prefix matches
     */
    var row = prefixMatch(ownPeerNumber, senderPeer)
    
    /**
     * Because prefixMatch returns the position number from when they start to distinguish
     */
   
    
    for(count1 <- 0 until row){
      
      for(count2 <- 0 until 16){
        
        /**
         * If the routing table entry is null ONLY then copy the entry
         */
        if(routingTable(count1)(count2) == null ){
        //  if(routingTablesOfHop(count1)(count2)!=null && !routingTablesOfHop(count1)(count1).equalsIgnoreCase("SPECIAL"))
          routingTable (count1)(count2) = routingTablesOfHop(count1)(count2)
          
        }
        
      }
      
    }
    /**
     * Since the hop will not have its own id in its routing table,
     * there is a possibility that entry corresponding to that prefix is empty.
     * If so, then add the sender peer id into that
     */
     
     var columnToPutSenderEntry:Int = prependZeros(senderPeer).charAt(row)
     /**
      * This variable represents the column in which we can put sender peer entry
      */
     if(columnToPutSenderEntry >='a'){
    	 columnToPutSenderEntry = columnToPutSenderEntry -'a' + 10
     }else{
    	 columnToPutSenderEntry = columnToPutSenderEntry -'0'
     }
    // println("My peer number is " + ownPeerNumber + " row :: " + row + " col :: "+ columnToPutSenderEntry + " and entry is " + routingTable(row)(columnToPutSenderEntry))
   //  if(routingTable (row)(columnToPutSenderEntry) == null){
       
       routingTable (row)(columnToPutSenderEntry) = prependZeros(senderPeer)
     //}
    
  }
  
  /**
   * This function fills the routing table entries 
   * with special if the prefix exactly matches to own peer entry.
   */
  def initializeSpecialEntriesRoutingTable(){
    
    
    for(count <- 0 until numOfColumns){
      
       var columnToPutEntry:Int = prependZeros(ownPeerNumber).charAt(count)
       /**
        * This variable represents the column in which we can put sender peer entry
        */
      if(columnToPutEntry >='a'){
    	 columnToPutEntry = columnToPutEntry -'a' + 10
      }else{
    	 columnToPutEntry = columnToPutEntry -'0'
      }
      
      routingTable(count)(columnToPutEntry) = "SPECIAL"
      
    }
    
  }
  /**
   * newPeer is the destination peer here.
   */
  def routeMessageViaLeafSet(newPeer:BigInt, hopCount:Int):Boolean = {
    
   // println("Checking in routeMessageViaLeafSet for peer " + newPeer + " in leaf set of "+ ownPeerNumber + " leaf set is "+ leafList);
    var sortedList = leafList.sorted
     /*
     * check if the key lies in the leafset range
     * if yes then go through the entire leafset to find the closest logical equivalent ands send a message to 
     * the next hop
     * and return true
     * else
     * return false
     * 
     */
 if(newPeer>=sortedList.min && newPeer<=sortedList.max)
 {
   /*
    * code to find the closest logical equivalent
    */
    var nextHop:BigInt=ownPeerNumber
    var minDiff:BigInt=(newPeer-ownPeerNumber).abs
    for(leafCheck<-sortedList){
		//if(leafCheck!=newPeer){
		 
		var temp=(leafCheck-newPeer).abs
			if(temp<minDiff){
			  minDiff=temp;
			  nextHop=leafCheck
			}
		//}
	 }
    
    /*
     * check if the closest is curr node or some other node in the leafset
     */
    leafList=sortedList
    if(nextHop!=ownPeerNumber){
      
      var nextHopRef=context.actorSelection("../"+nextHop)
      var newHopCount = hopCount + 1
   //   println("Sent by " + ownPeerNumber + " to " + nextHop + " with hop count" + newHopCount)
      
      nextHopRef ! ("Route",newPeer, newHopCount)
      
      
    }else{
      /**
       * This is the terminating condition since you found the closest neighbor.
       * So send terminate message and your state tables to the new node..... sender !
       */
      
    //  println("Message received with number of hop count" + hopCount);
      
      context.parent ! ("Received the message", hopCount);
      
    }
   
    return true
 }  
    return false
    
  }
  
  def routeMessageViaRoutingTable(newPeer:BigInt, hopCount:Int):Boolean ={
    var row=prefixMatch(ownPeerNumber, newPeer)
    var newKeyString=prependZeros(newPeer)
    var col:Int=newKeyString.charAt(row)
    if(col>='a'){
      col=col-'a'+10
    }else{
      col=col-'0'
    }
	var dest=routingTable(row)(col)
	if(dest!=null){
	  var temp=Integer.parseInt(dest,16)
	  var destRef=context.actorSelection("../"+temp)
	  var newHopCount = hopCount + 1
	  //println("In routing Sent by " + ownPeerNumber + " to " + temp + " with hop count" + newHopCount + "for peer " + newPeer)
	  destRef ! ("Route",newPeer, newHopCount)
	  return true
	}
	return false
    
  }
  
  def routeToClosestMatch(newPeer:BigInt,hopCount:Int){
    /**
     * Take union of Leaf set and routing table and find the 
     * numerically closest in that 
     */
      var unionSet = leafList.toSet
     /**
     * Convert hex entries to decimal and add the data to union set
     */
	  
      for(count1 <- 0 until numOfRowsRouting){
        
        for(count2 <- 0 until numOfColumns){
          if(routingTable (count1)(count2)!=null && !routingTable (count1)(count2).equalsIgnoreCase("SPECIAL"))
          unionSet = unionSet+(Integer.parseInt(routingTable(count1)(count2), 16))
          
        }
      }
      
      var nextHop:BigInt = ownPeerNumber 
      var mindiff:BigInt = (newPeer-ownPeerNumber).abs 
      
      /**
       * Now that the sets are combined, find the numerically closer
       */
      for(iterateSet <- unionSet){
        
        if(iterateSet != newPeer){
          
          var diff:BigInt = (newPeer-iterateSet).abs
          if(diff < mindiff){
            
            mindiff = diff
            nextHop = iterateSet
            
          }
          
        }
        
      }
      if(nextHop != ownPeerNumber){
	      var destRef=context.actorSelection("../"+nextHop)
	      var newHopCount=hopCount+1
	     // println("In third case Sent by " + ownPeerNumber + " to " + nextHop + " with hop count" + newHopCount + "for peer "+ newPeer)
		  destRef ! ("Route",newPeer,newHopCount)
	  }else{
	    /**
       * This is the terminating condition since you found the closest neighbor.
       * So send terminate message and your state tables to the new node..... sender !
       */
      
//		  println("In third case Message received with number of hop count" + hopCount);
//		  
//		  println("dest is "+newPeer+ "curr id "+ownPeerNumber)
//		  
//		  println("leafset is "+leafList)
      
		  context.parent ! ("Received the message", hopCount);
      
	  }
      
  }
  
  
}
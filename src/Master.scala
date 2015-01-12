import akka.actor.Actor
import akka.actor.Props
import scala.util.Random
import akka.actor.PoisonPill

class Master(var totalActors:BigInt, var typeOfAlgorithm:String) extends Actor{
	
  var topologyConfirmation:BigInt = 0
  var startTime:BigInt = 0
  var endTime:BigInt = 0
  var gossipCount=0
  
  def receive={
    
    case "Line" =>
      createWorkers(totalActors , "Line")
      
    case "Complete" => 
     createWorkers(totalActors , "Complete")
     
    case "2-D" => 
       var squareRoot:Double = Math.ceil(Math.sqrt(totalActors.doubleValue)) 
       var perfectSquare = squareRoot * squareRoot
       createWorkers(perfectSquare.intValue , "2-D")
       
    case "Imperfect_2-D" => 
       var squareRoot:Double = Math.ceil(Math.sqrt(totalActors.doubleValue)) 
       var perfectSquare = squareRoot * squareRoot
       createWorkers(perfectSquare.intValue , "Imperfect_2-D")
       
    case "I created my neighbours" =>
      
      topologyConfirmation += 1
      
      if(topologyConfirmation == context.children.size){
        /**
         * Every worker has created the neighbours
         */
        println("Topology Ready")
        var tempSum:BigDecimal = 0
        var tempWeight:BigDecimal = 0
        startTime = System.currentTimeMillis();
        
        println("Size of children list is "+ context.children.size)
         
        /**
          * Select a random worker to start the algorithm
         */
 
        
        var random:Random = new Random
          
        var luckyActor = random.nextInt(context.children.size)
        
        if(typeOfAlgorithm.equalsIgnoreCase("Gossip")){
          
          context.children.take(luckyActor).last ! ("Gossip msg","lets gossip")
          
        }else if(typeOfAlgorithm.equalsIgnoreCase("Push-sum")){

          context.children.take(luckyActor).last ! ("Receive_Push_Sum", tempSum, tempWeight)
          randomKill(250)
        
        }
        
      }
    
    case ("gossip received", sender:Int)=>
      
      gossipCount+=1
      //println("gossip received to "+ sender +"    \t count is "+gossipCount)
      if(gossipCount==context.children.size)
      {
        endTime =System.currentTimeMillis()
        println("Total time taken is "+ (endTime-startTime) + " miliseconds")
        context.system.shutdown
      }
      
    case ("Push-sum Convergence is reached", actorNumer, sumValue:BigDecimal, weightValue:BigDecimal) =>
      endTime  = System.currentTimeMillis();
      println("Actor number is " + actorNumer + " Sum is "+ sumValue + " Weight is " + weightValue + " Convergence is " + (sumValue/weightValue))
      
      println("Total time taken is "+ (endTime-startTime) + " miliseconds")
      context.system.shutdown
      
    case _ =>
      	println("Default Master")
  }
  
  def createWorkers(totalNumberOfActors:BigInt, typeOfTopology:String){
    for(i<- 0 until totalNumberOfActors.intValue){
        context.actorOf(Props(new Worker(i, totalNumberOfActors)), i.toString)        
      }
       for(child <- context.children){
    	   child ! ("Create Neighbours", typeOfTopology)
      }
  }
 
  def randomKill(noToKill:Int){
    var random:Random=new Random
    var killedWorkers:List[Int]=List()
    for(i<-0 until noToKill){
      var temp=random.nextInt(context.children.size-2)+1
      while(killedWorkers.contains(temp)){
        temp=random.nextInt(context.children.size-2)+1
      }
      killedWorkers=killedWorkers:+temp
      println("killing---------- "+temp)
 context.children.take(temp).last!PoisonPill
    }
  }
}
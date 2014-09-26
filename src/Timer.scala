import akka.actor.Actor

class Timer extends Actor{

  def receive={
    
    case "start" => 
      
      var startTime = System.currentTimeMillis()
    //  println("timer started")
      /**
       * Run loop for 5 minutes
       */
      while(System.currentTimeMillis() < startTime + (60000*5)){
        
      }
     // println ("Sending back the timer")
      sender ! "time is up"
      
    case _=> println("Timer default case")
    
    
  }
}

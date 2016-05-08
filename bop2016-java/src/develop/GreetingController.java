package develop;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class GreetingController { 
    private static  String result="";
    @RequestMapping("/greeting")
    public String greeting(@RequestParam(value="id1", defaultValue="") String id1,
    			@RequestParam(value="id2", defaultValue="") String id2)
    {
    	if(id1==""||id2==""){
    		return "Empty query";
    	}
    	try{
	    	long startId=Long.parseLong(id1);
	    	long endId=Long.parseLong(id2);
	    	result = MTGraph.solve(startId, endId);
	    	return result;
    	}catch (Exception e){
    		return e.toString();
    	}    	
    	
    }
    
}
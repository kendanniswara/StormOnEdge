package perf_test;

import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.generated.Grouping;
import backtype.storm.hooks.info.BoltAckInfo;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.hooks.info.SpoutAckInfo;
import backtype.storm.task.TopologyContext;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SOEBasicHook extends BaseTaskHook {

	 @Override
	    public void prepare(Map conf, TopologyContext context) {
	 
	        //Map from stream id to component id to the Grouping used.
	        Map<String, Map<String, Grouping>> targets = context.getThisTargets();
	        for (Map.Entry<String, Map<String, Grouping>> entry : targets.entrySet()) {
	            for (String componentId : entry.getValue().keySet()) {
	            	
	            }
	        }
	        
	    }
	
	 @Override
	    public void spoutAck(SpoutAckInfo info) {
	        
	        if (info.completeLatencyMs != null) {
	            //do something here
	        }
	    }
	 
	    @Override
	    public void emit(EmitInfo info) {

	    }
}

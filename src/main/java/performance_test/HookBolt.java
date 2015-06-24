package performance_test;

import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.BoltAckInfo;
import backtype.storm.hooks.info.BoltExecuteInfo;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.hooks.info.SpoutAckInfo;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;

public class HookBolt extends BaseTaskHook {

	//private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SOEBasicHook.class);
	
	long now;
	long timeStamp;
	long cycle = 5000;
	long counter = 0;
	long printCycle = 4;
	ArrayList<Long> latencyCompleteTimeList;
	HashMap<String, Integer> streamSendingMap;
	StringBuilder latencyResultString;
	
	long Ackcounter = 0;
	
	int port;
	int taskID;
	String componentID;
	
	@Override
    public void prepare(Map conf, TopologyContext context) {
	 	/*
        //Map from stream id to component id to the Grouping used.
        Map<String, Map<String, Grouping>> targets = context.getThisTargets();
        for (Map.Entry<String, Map<String, Grouping>> entry : targets.entrySet()) {
            for (String componentId : entry.getValue().keySet()) {
            	
            }
        }
        */
		now = System.currentTimeMillis();
		timeStamp = now;
		latencyCompleteTimeList = new ArrayList<Long>();
		streamSendingMap = new HashMap<String, Integer>();
		latencyResultString = new StringBuilder();
		
		port = context.getThisWorkerPort();
		taskID = context.getThisTaskId();
		componentID = context.getThisComponentId();
    }
	
	@Override
	public void boltExecute(BoltExecuteInfo info) {
		
		if (info.tuple != null) {
        	timeStamp = System.currentTimeMillis();
        	
        	latencyCompleteTimeList.add(timeStamp - info.tuple.getLong(2));
        	Ackcounter++;
        	
        	if(timeStamp-now > cycle)
        	{
        		counter++;
        		long timeMod = timeStamp - (timeStamp % cycle);
        		double ltAverage = average(latencyCompleteTimeList);
        		
        		latencyResultString.append(componentID + ","+ timeMod + ",Average," + ltAverage + "\n");
        		
        		now = timeStamp;
        		latencyCompleteTimeList.clear();
        		Ackcounter = 0;
        		
        		if(counter >= printCycle)
        		{
        			try {
        				FileWriter writer = new FileWriter("/home/kend/bolt-LatencyHook.csv", true);
        				writer.write(latencyResultString.toString());
        				writer.close();
        				
        			}catch(Exception e){ }
        			        			
        			latencyResultString.setLength(0);
        			counter = 0;
        		}
        	}
        }
		
		/*
		if(info.tuple != null)
		{
			Tuple tuple = info.tuple;
			
			if(streamSendingMap.containsKey(tuple.getString(1)))
			{
				int value = streamSendingMap.get(tuple.getString(1));
				value++;
				streamSendingMap.put(tuple.getString(1), new Integer(value));
			}
			else			
				streamSendingMap.put(tuple.getString(1), 1);			
			
			
			timeStamp = System.currentTimeMillis();
        	if(timeStamp-now > cycle)
        	{
        		counter++;
        		long timeMod = timeStamp - (timeStamp % cycle);
        		
        		for(String key : streamSendingMap.keySet())
        		{
        			counterResultString.append(timeMod + "," + componentID + "," + taskID + ","+ key + "," + streamSendingMap.get(key) + "\n");
        		}
        		
        		now = timeStamp;
        		streamSendingMap.clear();
        		
        		if(counter >= printCycle)
        		{
        			try {
        				
        				FileWriter writer = new FileWriter("/home/kend/tupleMovements.csv", true);
        				writer.write(counterResultString.toString());
        				writer.close();
        			}catch(Exception e){ }
        			        			
        			counterResultString.setLength(0);        			
        			counter = 0;
        		}
        	}
		}
		*/
		
		super.boltExecute(info);
	}
	
	public double average(ArrayList<Long> list) {
	        // 'average' is undefined if there are no elements in the list.
	        if (list == null || list.isEmpty())
	            return 0.0;
	        // Calculate the summation of the elements in the list
	        long sum = 0;
	        int n = list.size();
	        // Iterating manually is faster than using an enhanced for loop.
	        for (int i = 0; i < n; i++)
	            sum += list.get(i);
	        // We don't want to perform an integer division, so the cast is mandatory.
	        return ((double) sum) / n;
	    }
	
}

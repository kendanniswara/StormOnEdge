package performance_test;

import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.BoltAckInfo;
import backtype.storm.hooks.info.BoltExecuteInfo;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.hooks.info.SpoutAckInfo;
import backtype.storm.task.TopologyContext;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;

public class HookFinalBolt extends BaseTaskHook {

	//private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SOEBasicHook.class);
	
	long now;
	long timeStamp;
	long cycle = 2000;
	long counter = 0;
	long printCycle = 5;
	ArrayList<Long> latencyCompleteTimeList;
	StringBuilder latencyResultString;
	StringBuilder counterResultString;
	
	long Ackcounter = 0;
	
	int port;
	String taskName;
	
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
		latencyResultString = new StringBuilder();
		counterResultString = new StringBuilder();
		
		taskName = context.getThisComponentId();
		port = context.getThisWorkerPort();
    }
	
	@Override
	public void boltExecute(BoltExecuteInfo info) {
		// TODO Auto-generated method stub
		
		
        if (info.tuple != null) {
        	timeStamp = System.currentTimeMillis();
        	
        	latencyCompleteTimeList.add(timeStamp - info.tuple.getLong(2));
        	Ackcounter++;
        	
        	if(timeStamp-now > cycle)
        	{
        		counter++;
        		long timeMod = timeStamp - (timeStamp % cycle);
        		double ltAverage = average(latencyCompleteTimeList);
        		        	
        		//LOG.info(timeMod + ",Average," + ltAverage);
        		latencyResultString.append(taskName + ","+ timeMod + ",Average," + ltAverage + "\n");
        		counterResultString.append(taskName + ","+ timeMod + ",AckCounter," + Ackcounter + "\n");
        		
        		now = timeStamp;
        		latencyCompleteTimeList.clear();
        		Ackcounter = 0;
        		
        		if(counter >= printCycle)
        		{
        			try {
        				FileWriter writer = new FileWriter("/home/kend/Bolt-FinalLatencyHook.csv", true);
        				writer.write(latencyResultString.toString());
        				writer.close();
        				
        				writer = new FileWriter("/home/kend/Bolt-CounterHook.csv", true);
        				writer.write(counterResultString.toString());
        				writer.close();
        			}catch(Exception e){ }
        			        			
        			latencyResultString.setLength(0);
        			counterResultString.setLength(0);
        			counter = 0;
        		}
        	}
        }
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

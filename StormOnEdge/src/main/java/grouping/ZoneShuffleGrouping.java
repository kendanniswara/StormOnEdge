package grouping;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.task.WorkerTopologyContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.thrift7.TException;

public class ZoneShuffleGrouping implements CustomStreamGrouping {

	private static final long serialVersionUID = -4062741858518237161L;
	List<Integer> choosenTasks = new ArrayList<Integer>();
	Nimbus.Client client;
	Map<String,Bolt> bolts;
	Map<Integer,String> workersByNameMap;
	
	public ZoneShuffleGrouping(Map<Integer,String> supervisors)
	{		
		workersByNameMap = supervisors;
	}
	
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		/*
		List<String> componentIDs = new ArrayList<String>();
		for(Integer taskID : targetTasks)
		{
			componentIDs.add(context.getComponentId(taskID));
		}
		
		//now we have the list of componentID, what to do?
		
		ClusterSummary summary = client.getClusterInfo();
		for (TopologySummary ts: summary.get_topologies()) {
		      String id = ts.get_id();
		      TopologyInfo info = client.getTopologyInfo(id);
		      info.get_executors().get(1).
		}
		
		String sourceExecutorID = stream.get_componentId();
		List<Integer> sourceTasks = new ArrayList<Integer>(context.getComponentTasks(stream.get_componentId()));
		
		StormTopology topology = context.getRawTopology();
		Iterator<Integer> targetTasksIterator = targetTasks.iterator();
		
		while(targetTasksIterator.hasNext())
		{
			context. .getComponentCommon(context.getComponentId(targetTasksIterator.next()))
			
		}
		*/
	}

	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		// TODO Auto-generated method stub
		return null;
	}
	

}

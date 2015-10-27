package zoneGrouping;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.thrift7.TException;
import org.mortbay.util.MultiMap;

public class ZoneShuffleGrouping implements CustomStreamGrouping {

	private static final long serialVersionUID = -4062741858518237161L;
	MultiMap supervisorTaskMap = new MultiMap();
	HashMap<Integer, String> taskSupNameMap = new HashMap<Integer,String>();
	List<Integer> targetList;
	Random rand = new Random();
	
	HashMap<Integer, List<Integer>> taskResultList = new HashMap<Integer, List<Integer>>();
	
	@SuppressWarnings("unchecked")
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		
		targetList = targetTasks;
		
		getListFromFile();
		for(Integer source : context.getComponentTasks(stream.get_componentId()))
		{
			taskResultList.put(source, findIntersections(targetTasks, (List<Integer>) supervisorTaskMap.get(taskSupNameMap.get(source))));
		}
		
		/*
		//Extra for debugging
		StringBuilder sb = new StringBuilder();
		
		sb.append("-------------------------\n");
		sb.append(taskSupNameMap.toString() + "\n");
		sb.append("-------------------------\n\n");
		
		sb.append(context.getThisWorkerPort() + ":\n");
		sb.append("Tasks in this worker:" + context.getThisWorkerTasks() + "\n");
		sb.append("Source tasks:" + context.getComponentTasks(stream.get_componentId()) + "\n");
		sb.append("Supervisor Name:" + taskSupNameMap.get(context.getThisWorkerTasks().get(1)) + "\n");
		sb.append("Output location: " + "\n");
		for(Integer i : targetTasks)
		{
			sb.append(i + " = " + taskSupNameMap.get(i) + "\n");
		}
		List<Integer> targets = (List<Integer>) supervisorTaskMap.get(taskSupNameMap.get(context.getThisWorkerTasks().get(1)));
		sb.append("Choosen target : " + targets + "\n");
		
		try {	
			FileWriter writer = new FileWriter("/home/kend/groupingTask.csv", true);
			writer.write(sb.toString());
			writer.close();
		}catch(Exception e){ }
		*/
	}

	public List<Integer> chooseTasks(int taskId, List<Object> values) {		
		//return setIntersections((List<Integer>) supervisorTaskMap.get(taskSupNameMap.get(new Integer(taskId))));
		List<Integer> result = taskResultList.get(new Integer(taskId));
		List<Integer> singleResult = new ArrayList<Integer>(1);
		
		if (result == null || result.isEmpty())
			result = targetList;
		
		int resultIdx = rand.nextInt(result.size());
		singleResult.add(result.get(resultIdx));
		
		return singleResult;
	}
	
	private List<Integer> findIntersections(List<Integer> choosenTasks, List<Integer> fromSupervisor)
	{
		List<Integer> temp = new ArrayList<Integer>();
		
		for(Integer i : choosenTasks)
		{
			if(fromSupervisor.contains(i))
				temp.add(i);
		}
		return temp;
	}
	
	private void getListFromFile()
	{
		try {
		    FileReader supervisorTaskFile = new FileReader("/home/kend/fromSICSCloud/PairSupervisorTasks.txt");
		    BufferedReader textReader = new BufferedReader(supervisorTaskFile);
		    
		    String line = textReader.readLine();
		    while(line != null && !line.equals(""))
		    {
		    	//edge-supervisor;1,2,3,4,5
		    	String[] pairString = line.split(";");
		    	String key = pairString[0];
		    	String[] taskString = pairString[1].split(",");
		    	
		    	for(int ii = 0; ii < taskString.length; ii++)
		    	{
		    		supervisorTaskMap.add(key, new Integer(taskString[ii]));
		    		taskSupNameMap.put(new Integer(taskString[ii]), key);
		    	}
		    	
		    	line = textReader.readLine();
		    }
		    
		    textReader.close();
		    }catch(IOException e){e.printStackTrace();}
	}
	
}

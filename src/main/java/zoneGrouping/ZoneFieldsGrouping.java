package zoneGrouping;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.storm.guava.hash.HashFunction;
import org.apache.storm.guava.hash.Hashing;
import org.apache.thrift7.TException;
import org.mortbay.util.MultiMap;

public class ZoneFieldsGrouping implements CustomStreamGrouping {

	private static final long serialVersionUID = -4062741858518237161L;
	MultiMap supervisorTaskMap = new MultiMap();
	HashMap<Integer, String> taskSupNameMap = new HashMap<Integer,String>();
	List<Integer> targetList;
	
	HashMap<Integer, List<Integer>> taskResultList = new HashMap<Integer, List<Integer>>();
	
	Fields inputFields;
	Fields outputFields;
	HashFunction h1 = Hashing.murmur3_128();
	
	public ZoneFieldsGrouping(Fields f) {
		inputFields = f;
	}
	
	@SuppressWarnings("unchecked")
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		
		targetList = targetTasks;
		
		getListFromFile();
		for(Integer source : context.getComponentTasks(stream.get_componentId()))
		{
			taskResultList.put(source, findIntersections(targetTasks, (List<Integer>) supervisorTaskMap.get(taskSupNameMap.get(source))));
		}
		
		if (inputFields != null) {
            outputFields = context.getComponentOutputFields(stream);
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
		
		List<Integer> taskDefault = taskResultList.get(new Integer(taskId));
		List<Integer> hashResultTask = new ArrayList<Integer>(1);
		
		if (taskDefault == null || taskDefault.isEmpty())
			taskDefault = targetList;
		
		//Taken from PartialKeyGrouping.java
		if (values.size() > 0) {
            byte[] raw = null;
            if (inputFields != null) {
                List<Object> selectedFields = outputFields.select(inputFields, values);
                ByteBuffer out = ByteBuffer.allocate(selectedFields.size() * 4);
                for (Object o: selectedFields) {
                    out.putInt(o.hashCode());
                }
                raw = out.array();
            } else {
                raw = values.get(0).toString().getBytes(); // assume key is the first field
            }
            
            int Choice = (int) (Math.abs(h1.hashBytes(raw).asLong()) % taskDefault.size());
            hashResultTask.add(taskDefault.get(Choice));
        }
		
		return hashResultTask;
	}
	
	private List<Integer> findIntersections(List<Integer> targetTasks, List<Integer> fromSupervisor)
	{
		List<Integer> temp = new ArrayList<Integer>();
		
		for(Integer i : targetTasks)
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

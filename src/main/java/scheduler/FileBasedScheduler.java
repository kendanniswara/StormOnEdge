package scheduler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.mortbay.util.MultiMap;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

public class FileBasedScheduler implements IScheduler {
	
	Random rand = new Random(System.currentTimeMillis());
    public void prepare(Map conf) {}

    @SuppressWarnings("unchecked")
	public void schedule(Topologies topologies, Cluster cluster) {
        // Gets the topology which we want to schedule
    //TopologyDetails topology = topologies.getByName(topologyName);
    
    System.out.println("FileBasedScheduler: begin scheduling");

    //HARDCODE
    HashMap<String,String> componentSupervisorPair = new HashMap<String, String>();
    try {
    FileReader pairDataFile = new FileReader("/home/kend/fromSICSCloud/pairDataFile.txt");
    BufferedReader textReader = new BufferedReader(pairDataFile);
    
    String line = textReader.readLine();
    while(line != null && !line.equals(""))
    {
    	//messageSpout;edge-supervisor
    	System.out.println("Read from file: " + line);
    	String[] pairString = line.split(";");
    	componentSupervisorPair.put(pairString[0],pairString[1]);
    	
    	line = textReader.readLine();
    }
    //taskSupervisorPair.put("messageSpout", "edge-supervisor");  
    
    textReader.close();
    }catch(IOException e){e.printStackTrace();}
    
	for (TopologyDetails topology : topologies.getTopologies()) {
		boolean needsScheduling = cluster.needsScheduling(topology);
		
		if (!needsScheduling) {
            		System.out.println("Our special topology DOES NOT NEED scheduling.");
		}
		else {
			Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
                
            System.out.println("needs scheduling(component->executor): " + componentToExecutors);
            SchedulerAssignment currentAssignment = cluster.getAssignmentById(topology.getId());
            
            if (currentAssignment != null) {
            	System.out.println("current assignments: " + currentAssignment.getExecutorToSlot());
            } else {
            	System.out.println("current assignments: {}");
            }
                
            ////////////////
            ///NEW SCHEDULER
            ////////////////
            System.out.println("Start categorizing the supervisor");
            
            MultiMap workerSlotClusterBySupervisorMap = new MultiMap();
            MultiMap taskClusterBySupervisorMap = new MultiMap();
            MultiMap workerExecutors = new MultiMap();
            
            Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
            
            //put workerSLots based on supervisor type 
            for (SupervisorDetails supervisor : supervisors) {
                Map meta = (Map) supervisor.getSchedulerMeta();
                
                workerSlotClusterBySupervisorMap.addValues(meta.get("name"), cluster.getAvailableSlots(supervisor));
            }
            
            //print supervisor name and the worker list
            for(Object workerClusterKey : workerSlotClusterBySupervisorMap.keySet())
            {
            	String key = (String)workerClusterKey;
            	System.out.println(key + ": " + workerSlotClusterBySupervisorMap.getValues(workerClusterKey));
            }
            
            
            for(String executorKey : componentSupervisorPair.keySet())
            {
            	//for example: messageSpout
            	System.out.println("Our " + executorKey  + " needs scheduling.");
            	List<ExecutorDetails> executors = componentToExecutors.get(executorKey);
            	List<WorkerSlot> workers = new ArrayList<WorkerSlot>();
            	
            	if(componentSupervisorPair.get(executorKey).split(",")[0].equals("ALLWORKERS"))
            	{
            		System.out.println("ALLWORKERS found, get all workers");
            		workers = cluster.getAssignableSlots();
            	}
            	else
            	{
            		System.out.println("Get workers with value : " + componentSupervisorPair.get(executorKey));
            		String[] workersName = componentSupervisorPair.get(executorKey).split(",");
            		for(int ii = 0; ii < workersName.length; ii++)
            		{
            			List<WorkerSlot>  wk = (List<WorkerSlot>) workerSlotClusterBySupervisorMap.getValues(workersName[ii]);
            			if(wk != null)
            				workers.addAll(wk);
            		}
            	}
            	
            	if(executors == null)
            	{
            		System.out.println("No executors");
            	}
            	else if(workers.isEmpty())
            	{
            		System.out.println("No workers");
            	}
            	else
            	{
	            	System.out.println("Number of executors on" + executorKey  + ": " + executors.size() );
	            	System.out.println("Number of workers on" + executorKey  + ": " + workers.size() );
	            	
	            	Iterator<WorkerSlot> workerIterator = workers.iterator();
	            	Iterator<ExecutorDetails> executorIterator = executors.iterator();

	            	//if more executors than workers, do simple round robin
	            	if(executors.size() >= workers.size())
	            	{
		            	//round-robin for all executors A to all supervisors B
		            	while(executorIterator.hasNext() && workerIterator.hasNext())
		            	{
		            		WorkerSlot w = workerIterator.next();
		            		ExecutorDetails ed = executorIterator.next();
		            		workerExecutors.add(w, ed);
		            		
		            		String SupName = findSupervisorNameinWorkerSlot(workerSlotClusterBySupervisorMap, w);
		            		for(int ii = ed.getStartTask(); ii <= ed.getEndTask(); ii++)
		            			taskClusterBySupervisorMap.add(SupName, new Integer(ii));
		            		
		            		//reset to 0 again
		            		if(!workerIterator.hasNext())
		            			workerIterator = workers.iterator();
		            	}
	            	}
	            	//if more workers than executors, choose randomly
	            	else
	            	{
		            	//random for all executors A to all supervisors B
		            	while(executorIterator.hasNext() && !workers.isEmpty())
		            	{
		            		WorkerSlot w = workers.get(rand.nextInt(workers.size()));
		            		ExecutorDetails ed = executorIterator.next();
		            		workerExecutors.add(w, ed);
		            		
		            		String SupName = findSupervisorNameinWorkerSlot(workerSlotClusterBySupervisorMap, w);
		            		for(int ii = ed.getStartTask(); ii <= ed.getEndTask(); ii++)
		            			taskClusterBySupervisorMap.add(SupName, new Integer(ii));
		            	}
	            	}
            	}
            }
            
            //Assign the tasks into cluster
            StringBuilder workerStringBuilder = new StringBuilder();
            for(Object ws : workerExecutors.keySet())
        	{
            	List<ExecutorDetails> edetails = (List<ExecutorDetails>) workerExecutors.getValues(ws);
            	WorkerSlot wslot = (WorkerSlot) ws;
            	
        		cluster.assign(wslot, topology.getId(), edetails);
        		System.out.println("We assigned executors:" + workerExecutors.getValues(ws) + " to slot: [" + wslot.getNodeId() + ", " + wslot.getPort() + "]");
        		workerStringBuilder.append(workerExecutors.getValues(ws) + " to slot: [" + wslot.getNodeId() + ", " + wslot.getPort() + "]\n");
        	}
            
            try {
				FileWriter writer = new FileWriter("/home/kend/SchedulerResult.csv", false);
				writer.write(workerStringBuilder.toString());
				writer.close();
			}catch(Exception e){ }
            
            
            StringBuilder taskStringBuilder = new StringBuilder();            
            for(Object sup : taskClusterBySupervisorMap.keySet())
            {
            	String taskString = (String) sup + ";";
            	
            	for(Integer t : (List<Integer>) taskClusterBySupervisorMap.getValues(sup))
            	{
            		taskString = taskString + t.toString() + ",";
            	}
            	
            	taskStringBuilder.append(taskString.substring(0, taskString.length()-1));
            	taskStringBuilder.append("\n");
            }
            
            try {
				FileWriter writer = new FileWriter("/home/kend/fromSICSCloud/PairSupervisorTasks.txt", false);
				writer.write(taskStringBuilder.toString());
				writer.close();
			}catch(Exception e){ }
            
            System.out.println(taskStringBuilder.toString());

            }
        }
        
        // let system's even scheduler handle the rest scheduling work
        // you can also use your own other scheduler here, this is what
        // makes storm's scheduler composable.
        new EvenScheduler().schedule(topologies, cluster);
    }

	@SuppressWarnings("unchecked")
	private String findSupervisorNameinWorkerSlot(MultiMap workerSlotClusterBySupervisorMap, WorkerSlot w) {
		
		String workerName = w.getNodeId();
		String supervisorName = "";
		
		for(Object key : workerSlotClusterBySupervisorMap.keySet())
		{
			for(WorkerSlot ws : (List<WorkerSlot>) workerSlotClusterBySupervisorMap.get(key))
			{
				if(ws.getNodeId().equals(workerName))
						supervisorName = (String)key;
			}
		}
		
		return supervisorName;
	}

}

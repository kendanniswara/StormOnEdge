package scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

public class SimpleScheduler implements IScheduler {
	
	String topologyName = "test_0";
    public void prepare(Map conf) {}

    public void schedule(Topologies topologies, Cluster cluster) {
	System.out.println("DemoScheduler: begin scheduling");
        // Gets the topology which we want to schedule
    TopologyDetails topology = topologies.getByName(topologyName);
    
    System.out.println("DemoScheduler: begin scheduling");

	if (topology != null) {
	
	 boolean needsScheduling = cluster.needsScheduling(topology);
		if (!needsScheduling) {
            		System.out.println("Our special topology DOES NOT NEED scheduling.");
		}
		else {
		Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
                
                System.out.println("needs scheduling(component->executor): " + componentToExecutors);
                System.out.println("needs scheduling(executor->compoenents): " + cluster.getNeedsSchedulingExecutorToComponents(topology));
                SchedulerAssignment currentAssignment = cluster.getAssignmentById(topologies.getByName(topologyName).getId());
                if (currentAssignment != null) {
                	System.out.println("current assignments: " + currentAssignment.getExecutorToSlot());
                } else {
                	System.out.println("current assignments: {}");
                }
                
		///////////////////////////
		///schedule InputSpout
		///////////////////////////
                if (!componentToExecutors.containsKey("messageSpout")) {
                	System.out.println("Our InputSpout DOES NOT NEED scheduling.");
                } else {
                    System.out.println("Our InputSpout needs scheduling.");
                    List<ExecutorDetails> executors = componentToExecutors.get("messageSpout");
		    System.out.println("Number of InputSpout : " + executors.size());

                    // find out the our "spout-supervisor" from the supervisor metadata
                    Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
                    Collection<SupervisorDetails> specialSupervisors = new ArrayList<SupervisorDetails>();
                    for (SupervisorDetails supervisor : supervisors) {
                        Map meta = (Map) supervisor.getSchedulerMeta();

                        if (meta.get("name").equals("spout-supervisor")) {
                        	specialSupervisors.add(supervisor);
                        }
                    }

                    // found the special supervisor
                    if (!specialSupervisors.isEmpty()) {
                    	System.out.println("Found the spout-supervisor: " + specialSupervisors.size());
			for(SupervisorDetails supervisor : specialSupervisors)
			{
				List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);
				// if there is no available slots on this supervisor, free some.
		                // TODO for simplicity, we free all the used slots on the supervisor.
		                if (availableSlots.isEmpty() && !executors.isEmpty()) {
		                    for (Integer port : cluster.getUsedPorts(supervisor)) {
		                        cluster.freeSlot(new WorkerSlot(supervisor.getId(), port));
	                            }
	                        }

				// re-get the aviableSlots
        			availableSlots = cluster.getAvailableSlots(supervisor);

				// since it is just a demo, to keep things simple, we assign all the
	               		// executors into one slot.
        	        	cluster.assign(availableSlots.get(0), topology.getId(), executors);
        	                System.out.println("We assigned executors:" + executors + " to slot: [" + availableSlots.get(0).getNodeId() + ", " + availableSlots.get(0).getPort() + "]");
			}
                       
                    } else {
                    	System.out.println("There is no supervisor named spout-supervisor!!!");
                    }
                }

		///////////////////////////
		///schedule Level1Bolt
		///////////////////////////
                if (!componentToExecutors.containsKey("messageBolt1")) {
                	System.out.println("Our Level1Bolt DOES NOT NEED scheduling.");
                } else {
                    System.out.println("Our Level1Bolt needs scheduling.");
                    List<ExecutorDetails> executors = componentToExecutors.get("messageBolt1");
		    System.out.println("Number of Level1Bolt : " + executors.size());

                    // find out the our "spout-supervisor" from the supervisor metadata
                    Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
                    Collection<SupervisorDetails> specialSupervisors = new ArrayList<SupervisorDetails>();
                    for (SupervisorDetails supervisor : supervisors) {
                        Map meta = (Map) supervisor.getSchedulerMeta();

                        if (meta.get("name").equals("Level1Bolt-supervisor")) {
                        	specialSupervisors.add(supervisor);
                        }
                    }

                    // found the special supervisor
                    if (!specialSupervisors.isEmpty()) {
                    	System.out.println("Found the Level1Bolt-supervisor: " + specialSupervisors.size());
			for(SupervisorDetails supervisor : specialSupervisors)
			{
				List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);
				// if there is no available slots on this supervisor, free some.
		                // TODO for simplicity, we free all the used slots on the supervisor.
		                if (availableSlots.isEmpty() && !executors.isEmpty()) {
		                    for (Integer port : cluster.getUsedPorts(supervisor)) {
		                        cluster.freeSlot(new WorkerSlot(supervisor.getId(), port));
	                            }
	                        }

				// re-get the aviableSlots
        			availableSlots = cluster.getAvailableSlots(supervisor);

				// since it is just a demo, to keep things simple, we assign all the
	               		// executors into one slot.
        	        	cluster.assign(availableSlots.get(0), topology.getId(), executors);
        	                System.out.println("We assigned executors:" + executors + " to slot: [" + availableSlots.get(0).getNodeId() + ", " + availableSlots.get(0).getPort() + "]");
			}
                       
                    } else {
                    	System.out.println("There is no supervisor named Level1Bolt-supervisor!!!");
                    }
                }

		///////////////////////////
		///schedule Level2Bolt
		///////////////////////////
                if (!componentToExecutors.containsKey("messageBolt2")) {
                	System.out.println("Our Level2Bolt DOES NOT NEED scheduling.");
                } else {
                    System.out.println("Our Level2Bolt needs scheduling.");
                    List<ExecutorDetails> executors = componentToExecutors.get("messageBolt2");
		    System.out.println("Number of Level2Bolt : " + executors.size());

                    // find out the our "spout-supervisor" from the supervisor metadata
                    Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
                    Collection<SupervisorDetails> specialSupervisors = new ArrayList<SupervisorDetails>();
                    for (SupervisorDetails supervisor : supervisors) {
                        Map meta = (Map) supervisor.getSchedulerMeta();

                        if (meta.get("name").equals("Level2Bolt-supervisor")) {
                        	specialSupervisors.add(supervisor);
                        }
                    }

                    // found the special supervisor
                    if (!specialSupervisors.isEmpty()) {
                    	System.out.println("Found the Level2Bolt-supervisor: " + specialSupervisors.size());
			for(SupervisorDetails supervisor : specialSupervisors)
			{
				List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);
				// if there is no available slots on this supervisor, free some.
		                // TODO for simplicity, we free all the used slots on the supervisor.
		                if (availableSlots.isEmpty() && !executors.isEmpty()) {
		                    for (Integer port : cluster.getUsedPorts(supervisor)) {
		                        cluster.freeSlot(new WorkerSlot(supervisor.getId(), port));
	                            }
	                        }

				// re-get the aviableSlots
        			availableSlots = cluster.getAvailableSlots(supervisor);

				// since it is just a demo, to keep things simple, we assign all the
	               		// executors into one slot.
        	        	cluster.assign(availableSlots.get(0), topology.getId(), executors);
        	                System.out.println("We assigned executors:" + executors + " to slot: [" + availableSlots.get(0).getNodeId() + ", " + availableSlots.get(0).getPort() + "]");
			}
                       
                    } else {
                    	System.out.println("There is no supervisor named Level2Bolt-supervisor!!!");
                    }
                }
            }
        }
        
        // let system's even scheduler handle the rest scheduling work
        // you can also use your own other scheduler here, this is what
        // makes storm's scheduler composable.
        new EvenScheduler().schedule(topologies, cluster);
    }

}

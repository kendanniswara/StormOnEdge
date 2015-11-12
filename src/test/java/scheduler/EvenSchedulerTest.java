package scheduler;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.INimbus;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SchedulerAssignmentImpl;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.testing.TestWordCounter;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mockito;

/**
 *
 * @author Hooman
 */
public class EvenSchedulerTest {

  public EvenSchedulerTest() {
  }

  /**
   * Test of schedule method, of class LocalGlobalGroupScheduler.
   */
  @org.junit.Test
  public void testSchedule() {
    Cluster cluster = createCluster();//        LocalGlobalGroupScheduler scheduler = new LocalGlobalGroupScheduler();
    String tName = "t1";
    Topologies topologies = createTopologies(tName);
    EvenScheduler es = new EvenScheduler();
    es.schedule(topologies, cluster);
    Map<String, SchedulerAssignment> assignments = cluster.getAssignments();
    assert assignments.size() == 1;
    Map<ExecutorDetails, WorkerSlot> assignment = assignments.get(tName).getExecutorToSlot();
    assert assignment.size() == 8;
    // Here, asserts can come.
  }

  private Cluster createCluster() {
    // The following code is to demonstrate how to create an instance of Cluster for unit testing.
    Collection<Number> allPorts = new ArrayList<Number>(4);// Four slots per supervisor.
    allPorts.add(5000);
    allPorts.add(5001);
    allPorts.add(5002);
    allPorts.add(5003);

    SupervisorDetails s1 = new SupervisorDetails("1", "1", null, allPorts);
    SupervisorDetails s2 = new SupervisorDetails("2", "2", null, allPorts);
    SupervisorDetails s3 = new SupervisorDetails("3", "3", null, allPorts);
    SupervisorDetails s4 = new SupervisorDetails("4", "4", null, allPorts);

    Map<String, SupervisorDetails> supervisors = new HashMap<String, SupervisorDetails>();
    supervisors.put(s1.getId(), s1);
    supervisors.put(s2.getId(), s2);
    supervisors.put(s3.getId(), s3);
    supervisors.put(s4.getId(), s4);
    INimbus nimbus = Mockito.mock(INimbus.class);
    Mockito.when(nimbus.getHostName(Mockito.any(Map.class), Mockito.eq(s1.getId()))).thenReturn(s1.getHost());
    Mockito.when(nimbus.getHostName(Mockito.any(Map.class), Mockito.eq(s2.getId()))).thenReturn(s2.getHost());
    Mockito.when(nimbus.getHostName(Mockito.any(Map.class), Mockito.eq(s3.getId()))).thenReturn(s3.getHost());
    Mockito.when(nimbus.getHostName(Mockito.any(Map.class), Mockito.eq(s4.getId()))).thenReturn(s4.getHost());
    Map<String, SchedulerAssignmentImpl> assignments = new HashMap<String, SchedulerAssignmentImpl>();

    return new Cluster(nimbus, supervisors, assignments);

  }

  private Topologies createTopologies(String tName) {
    TopologyBuilder tb = new TopologyBuilder();
    Map<ExecutorDetails, String> executors = new HashMap<ExecutorDetails, String>();
    for (int i = 0; i < 8; i++) {
      ExecutorDetails e1 = new ExecutorDetails(i, i);
      executors.put(e1, "");
    }
    TopologyDetails topology = new TopologyDetails(tName, new HashMap(), tb.createTopology(), 8, executors);
    Map<String, TopologyDetails> ts = new HashMap<String, TopologyDetails>();
    ts.put(tName, topology);
    Topologies topologies = new Topologies(ts);
    return topologies;
  }
}

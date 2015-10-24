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
public class LocalGlobalGroupSchedulerTest {

    public LocalGlobalGroupSchedulerTest() {
    }

    /**
     * Test of schedule method, of class LocalGlobalGroupScheduler.
     */
    @org.junit.Test
    public void testSchedule() {
        Cluster cluster = createCluster();
//        LocalGlobalGroupScheduler scheduler = new LocalGlobalGroupScheduler();
        Topologies topologies = createTopologies();
        EvenScheduler es = new EvenScheduler();
        es.schedule(topologies, cluster);
        Map<String, SchedulerAssignment> assignments = cluster.getAssignments();
        System.out.println(assignments.size());
        System.out.println(cluster.getAvailableSlots().toString());
        // Here, asserts can come.
//        scheduler.schedule(topologies, cluster);
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

        Map<String, SupervisorDetails> supervisors = new HashMap();
        supervisors.put(s1.getId(), s1);
        supervisors.put(s2.getId(), s2);
        supervisors.put(s3.getId(), s3);
        supervisors.put(s4.getId(), s4);
        INimbus nimbus = Mockito.mock(INimbus.class);
        Mockito.when(nimbus.getHostName(Mockito.any(Map.class), Mockito.eq(s1.getId()))).thenReturn(s1.getHost());
        Mockito.when(nimbus.getHostName(Mockito.any(Map.class), Mockito.eq(s2.getId()))).thenReturn(s2.getHost());
        Mockito.when(nimbus.getHostName(Mockito.any(Map.class), Mockito.eq(s3.getId()))).thenReturn(s3.getHost());
        Mockito.when(nimbus.getHostName(Mockito.any(Map.class), Mockito.eq(s4.getId()))).thenReturn(s4.getHost());
        Map<String, SchedulerAssignmentImpl> assignments = new HashMap();

        return new Cluster(nimbus, supervisors, assignments);

    }

    private Topologies createTopologies() {
        TopologyBuilder tb = new TopologyBuilder();
        IBasicBolt b1 = new TestWordCounter();
        tb.setBolt("b1", b1, 4);
        IRichSpout s1 = new TestWordSpout();
        tb.setSpout("s1", s1, 2);
        Map<ExecutorDetails, String> executors = new HashMap<ExecutorDetails, String>();
        ExecutorDetails e1 = new ExecutorDetails(0, 1);
        executors.put(e1, "");
        TopologyDetails topology = new TopologyDetails("t1", new HashMap(), tb.createTopology(), 4, executors);
        Map<String, TopologyDetails> ts = new HashMap<String, TopologyDetails>();
        ts.put("t1", topology);
        Topologies topologies = new Topologies(ts);
        return topologies;
    }
}

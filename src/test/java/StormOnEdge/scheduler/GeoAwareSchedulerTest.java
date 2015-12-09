package StormOnEdge.scheduler;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.INimbus;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SchedulerAssignmentImpl;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import StormOnEdge.grouping.stream.ZoneShuffleGrouping;
import org.mockito.Mockito;

/**
 *
 * @author Ken
 */
public class GeoAwareSchedulerTest {

  private final int PORT = 6701;

    public GeoAwareSchedulerTest() {
  }

  /**
   * Test of schedule method, of class GeoAwareScheduler.
   */
  @org.junit.Test
  public void testSchedule() {
    Cluster cluster = createCluster(); // GeoAwareScheduler StormOnEdge.scheduler = new GeoAwareScheduler();

    String tName = "t1";
    Topologies topologies = createTopologies(tName);
    GeoAwareScheduler lg = new GeoAwareScheduler();

    Map<String, Object> conf = dummyStormCONF();

//        es.prepare(conf);
//        es.schedule(topologies, cluster);
    lg.prepare(conf);
    lg.schedule(topologies, cluster);

    Map<String, SchedulerAssignment> assignments = cluster.getAssignments();
    //assert assignments.size() == 1;
    Map<ExecutorDetails, WorkerSlot> assignment = assignments.get(tName).getExecutorToSlot();
    System.out.println(assignment.toString());
        //assert assignment.size() == 8;
    // TODO: Here, asserts come.
  }

  private Map<String, Object> dummyStormCONF() {
    Map<String, Object> config = new HashMap<String, Object>();

    //config.put("geoAwareScheduler.in-SourceInfo", "data/Test-SpoutCloudsPair.txt");
    config.put("geoAwareScheduler.in-SourceInfo", "data/Scheduler-SpoutCloudsPair.txt");
    config.put("geoAwareScheduler.in-CloudInfo", "data/Scheduler-LatencyMatrix.txt");
    config.put("geoAwareScheduler.out-SchedulerResult", "data/Result-Scheduler.txt");
    config.put("geoAwareScheduler.out-ZGConnector", "data/Result-ZoneGrouping.txt");

    return config;
  }

  @SuppressWarnings("unchecked")
  private Cluster createCluster() {
    // The following code is to demonstrate how to create an instance of Cluster for unit testing.
    Collection<Number> allPorts = new ArrayList<Number>(2);// two slots per supervisor.
    allPorts.add(PORT);
    allPorts.add(PORT + 1);

    Map<String, SupervisorDetails> supervisors = new HashMap<String, SupervisorDetails>();//Create clouds with 2 supervisors each
    supervisors.putAll(createSupervisors("CloudEdgeA", 2, allPorts));
    supervisors.putAll(createSupervisors("CloudEdgeB", 2, allPorts));
    supervisors.putAll(createSupervisors("CloudEdgeC", 2, allPorts));
    supervisors.putAll(createSupervisors("CloudEdgeD", 2, allPorts));
    supervisors.putAll(createSupervisors("CloudEdgeE", 2, allPorts));
    supervisors.putAll(createSupervisors("CloudEdgeF", 2, allPorts));
    supervisors.putAll(createSupervisors("CloudEdgeG", 2, allPorts));
    supervisors.putAll(createSupervisors("CloudEdgeH", 2, allPorts));
    supervisors.putAll(createSupervisors("CloudEdgeI", 2, allPorts));
    supervisors.putAll(createSupervisors("CloudGlobalA", 4, allPorts));

    INimbus nimbus = Mockito.mock(INimbus.class);
    for (SupervisorDetails sp : supervisors.values()) {
      Mockito.when(nimbus.getHostName(Mockito.any(Map.class), Mockito.eq(sp.getId()))).thenReturn(sp.getHost());
    }
//      Mockito.when(nimbus.getHostName(Mockito.any(Map.class), Mockito.eq(s1.getId()))).thenReturn(s1.getHost());

    Map<String, SchedulerAssignmentImpl> assignments = new HashMap<String, SchedulerAssignmentImpl>();

    return new Cluster(nimbus, supervisors, assignments);
  }

  private Map<String, SupervisorDetails> createSupervisors(String cloudName, int numSupervisor, Collection<Number> allPorts) {
    Map<String, SupervisorDetails> supervisors = new HashMap<String, SupervisorDetails>();

    Map<String, Object> schedulerMeta = new HashMap<String, Object>();
    schedulerMeta.put("cloud-name", cloudName);

    for (int i = 1; i <= numSupervisor; i++) {
      String id = cloudName + "-" + i;
      String host = cloudName + "-hostname";

      supervisors.put(id, new SupervisorDetails(id, host, schedulerMeta, allPorts));
    }

    return supervisors;
  }

  private Topologies createTopologies(String tName) {

    TopologyBuilder tb = getSampleTopologyBuilder();
    Map<ExecutorDetails, String> executors = getExecutorToComponents(tb);

    TopologyDetails topology = new TopologyDetails(tName, new HashMap(), tb.createTopology(), 8, executors);

    Map<String, TopologyDetails> ts = new HashMap<String, TopologyDetails>();
    ts.put(tName, topology);
    Topologies topologies = new Topologies(ts);
    return topologies;
  }

  private TopologyBuilder getSampleTopologyBuilder() {
    TopologyBuilder builder = new TopologyBuilder();

//    builder.setSpout("Spout1", new ASpout(), 8).addConfiguration("group-name", "Local1");
//    builder.setBolt("Bolt1", new ABolt(), 4).shuffleGrouping("spout1").addConfiguration("group-name", "Local1");
//    builder.setBolt("Bolt2", new ABolt(), 4).shuffleGrouping("Bolt1").addConfiguration("group-name", "Global1");

    int totalSpout = 2 * 5;
    int totalLocalBolt = 2 * 5;
    int totalLocalResultBolt = 5;
    int totalGlobalBolt = 4;
    int totalGlobalResultBolt = 1;

    builder.setSpout("messageSpoutLocal1", new ASpout(), totalSpout).addConfiguration("group-name", "Local1");
    builder.setBolt("messageBoltLocal1_1", new ABolt(), totalLocalBolt).customGrouping("messageSpoutLocal1", new ZoneShuffleGrouping()).addConfiguration("group-name", "Local1");
    builder.setBolt("messageBoltLocal1_LocalResult", new ABolt(), totalLocalResultBolt).customGrouping("messageBoltLocal1_1", new ZoneShuffleGrouping()).addConfiguration("group-name", "Local1");

    builder.setSpout("messageSpoutLocal2", new ASpout(), totalSpout).addConfiguration("group-name", "Local2");
    builder.setBolt("messageBoltLocal2_1", new ABolt(), totalLocalBolt).customGrouping("messageSpoutLocal2", new ZoneShuffleGrouping()).addConfiguration("group-name", "Local2");
    builder.setBolt("messageBoltLocal2_LocalResult", new ABolt(), totalLocalResultBolt).customGrouping("messageBoltLocal2_1", new ZoneShuffleGrouping()).addConfiguration("group-name", "Local2");

    builder.setBolt("messageBoltGlobal1_1A", new ABolt(), totalGlobalBolt).shuffleGrouping("messageBoltLocal1_1").addConfiguration("group-name", "Global1");
    builder.setBolt("messageBoltGlobal1_1B", new ABolt(), totalGlobalBolt).shuffleGrouping("messageBoltLocal2_1").addConfiguration("group-name", "Global1");
    builder.setBolt("messageBoltGlobal1_FG", new ABolt(), 2)
            .fieldsGrouping("messageBoltGlobal1_1A", new Fields("fieldValue"))
            .fieldsGrouping("messageBoltGlobal1_1B", new Fields("fieldValue"))
            .addConfiguration("group-name", "Global1");
    builder.setBolt("messageBoltGlobal1_GlobalResult", new ABolt(), totalGlobalResultBolt)
            .shuffleGrouping("messageBoltGlobal1_FG")
            .addConfiguration("group-name", "Global1");

    return builder;
  }

  private Map<ExecutorDetails, String> getExecutorToComponents(TopologyBuilder builder) {
    Map<ExecutorDetails, String> executors = new HashMap<ExecutorDetails, String>();
    int TaskIndex = 0;

    StormTopology st = builder.createTopology();

    for (String name : st.get_spouts().keySet()) {
      SpoutSpec s = st.get_spouts().get(name);
      for (int i = 0; i < s.get_common().get_parallelism_hint(); i++) {
        ExecutorDetails ed = new ExecutorDetails(TaskIndex, TaskIndex);
        executors.put(ed, name);
        TaskIndex++;
      }
    }

    for (String name : st.get_bolts().keySet()) {
      Bolt b = st.get_bolts().get(name);
      for (int i = 0; i < b.get_common().get_parallelism_hint(); i++) {
        ExecutorDetails ed = new ExecutorDetails(TaskIndex, TaskIndex);
        executors.put(ed, name);
        TaskIndex++;
      }
    }

    return executors;
  }
}

class ABolt extends BaseRichBolt {

  private static final long serialVersionUID = -6049014472332715388L;

  public void prepare(Map stormConf, TopologyContext context,
    OutputCollector collector) {
		// TODO Auto-generated method stub

  }

  public void execute(Tuple input) {
		// TODO Auto-generated method stub

  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

  }

}

class ASpout extends BaseRichSpout {

  private static final long serialVersionUID = -2528554652404782661L;

  public void open(Map conf, TopologyContext context,
    SpoutOutputCollector collector) {
		// TODO Auto-generated method stub

  }

  public void nextTuple() {
		// TODO Auto-generated method stub

  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

  }

}

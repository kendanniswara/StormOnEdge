package performance_test;

/*
 * Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import zoneGrouping.ZoneFieldsGrouping;
import zoneGrouping.ZoneShuffleGrouping;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import backtype.storm.utils.NimbusClient;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologySummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.ExecutorStats;

import java.util.HashMap;

public class StaticMultiZone {
  private static final Log LOG = LogFactory.getLog(Main.class);

  @Option(name="--help", aliases={"-h"}, usage="print help message")
  private boolean _help = false;
  
  @Option(name="--debug", aliases={"-d"}, usage="enable debug")
  private boolean _debug = false;
  
  @Option(name="--local", usage="run in local mode")
  private boolean _local = false;
  
  @Option(name="--messageSizeByte", aliases={"--messageSize"}, metaVar="SIZE",
      usage="size of the messages generated in bytes")
  private int _messageSize = 100;

  @Option(name="--numTopologies", aliases={"-n"}, metaVar="TOPOLOGIES",
      usage="number of topologies to run in parallel")
  private int _numTopologies = 1;
 
   @Option(name="--numLevels", aliases={"-l"}, metaVar="LEVELS",
      usage="number of levels of bolts per topolgy")
  private int _numLevels = 1;

  @Option(name="--spoutParallel", aliases={"--spout"}, metaVar="SPOUT",
      usage="number of spouts to run in parallel")
  private int _spoutParallel = 3;
  
  @Option(name="--boltParallel", aliases={"--bolt"}, metaVar="BOLT",
      usage="number of bolts to run in parallel")
  private int _boltParallel = 3;
  
  @Option(name="--numWorkers", aliases={"--workers"}, metaVar="WORKERS",
      usage="number of workers to use per topology")
  private int _numWorkers = 3;
  
  @Option(name="--ackers", metaVar="ACKERS", 
      usage="number of acker bolts to launch per topology")
  private int _ackers = 1;
  
  @Option(name="--maxSpoutPending", aliases={"--maxPending"}, metaVar="PENDING",
      usage="maximum number of pending messages per spout (only valid if acking is enabled)")
  private int _maxSpoutPending = -1;
  
  @Option(name="--name", aliases={"--topologyName"}, metaVar="NAME",
      usage="base name of the topology (numbers may be appended to the end)")
  private String _name = "test";
  
  @Option(name="--ackEnabled", aliases={"--ack"}, usage="enable acking")
  private boolean _ackEnabled = false;
  
  @Option(name="--pollFreqSec", aliases={"--pollFreq"}, metaVar="POLL",
      usage="How often should metrics be collected")
  private int _pollFreqSec = 30;
  
  @Option(name="--testTimeSec", aliases={"--testTime"}, metaVar="TIME",
      usage="How long should the benchmark run for.")
  private int _testRunTimeSec = 5 * 60;
  
  @Option(name="--sampleRateSec", aliases={"--sampleRate"}, metaVar="SAMPLE",
	      usage="Sample rate for metrics (0-1).")
	  private double _sampleRate = 0.1;

  private static class MetricsState {
    long transferred = 0;
    int slotsUsed = 0;
    long lastTime = 0;
  }


  public void metrics(Nimbus.Client client, int size, int poll, int total) throws Exception {
    System.out.println("status\ttopologies\ttotalSlots\tslotsUsed\ttotalExecutors\texecutorsWithMetrics\ttime\ttime-diff ms\ttransferred\tthroughput (MB/s)");
    MetricsState state = new MetricsState();
    long pollMs = poll * 1000;
    long now = System.currentTimeMillis();
    state.lastTime = now;
    long startTime = now;
    long cycle = 0;
    long sleepTime;
    long wakeupTime;
    while (metrics(client, size, now, state, "WAITING")) {
      now = System.currentTimeMillis();
      cycle = (now - startTime)/pollMs;
      wakeupTime = startTime + (pollMs * (cycle + 1));
      sleepTime = wakeupTime - now;
      if (sleepTime > 0) {
        Thread.sleep(sleepTime);
      }
      now = System.currentTimeMillis();
    }

    now = System.currentTimeMillis();
    cycle = (now - startTime)/pollMs;
    wakeupTime = startTime + (pollMs * (cycle + 1));
    sleepTime = wakeupTime - now;
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }
    now = System.currentTimeMillis();
    long end = now + (total * 1000);
    do {
      metrics(client, size, now, state, "RUNNING");
      now = System.currentTimeMillis();
      cycle = (now - startTime)/pollMs;
      wakeupTime = startTime + (pollMs * (cycle + 1));
      sleepTime = wakeupTime - now;
      if (sleepTime > 0) {
        Thread.sleep(sleepTime);
      }
      now = System.currentTimeMillis();
    } while (now < end);
  }

  public boolean metrics(Nimbus.Client client, int size, long now, MetricsState state, String message) throws Exception {
    ClusterSummary summary = client.getClusterInfo();
    long time = now - state.lastTime;
    state.lastTime = now;
    int numSupervisors = summary.get_supervisors_size();
    int totalSlots = 0;
    int totalUsedSlots = 0;
    
    //////////
    //String namaSupervisor = "";
    for (SupervisorSummary sup: summary.get_supervisors()) {
      totalSlots += sup.get_num_workers();
      totalUsedSlots += sup.get_num_used_workers();
      //namaSupervisor = namaSupervisor + sup.get_host() + ",";
    }
    //System.out.println(namaSupervisor);
    
    int slotsUsedDiff = totalUsedSlots - state.slotsUsed;
    state.slotsUsed = totalUsedSlots;

    int numTopologies = summary.get_topologies_size();
    long totalTransferred = 0;
    int totalExecutors = 0;
    int executorsWithMetrics = 0;
    for (TopologySummary ts: summary.get_topologies()) {
      String id = ts.get_id();
      TopologyInfo info = client.getTopologyInfo(id);

      ////SOE Addition
      PerftestWriter.print(summary, info, new HashMap<String, Long>());

      ////
      for (ExecutorSummary es: info.get_executors()) {
        ExecutorStats stats = es.get_stats();
        totalExecutors++;
        if (stats != null) {
          Map<String,Map<String,Long>> transferred = stats.get_transferred();
          if ( transferred != null) {
            Map<String, Long> e2 = transferred.get(":all-time");
            if (e2 != null) {
              executorsWithMetrics++;
              //The SOL messages are always on the default stream, so just count those
              Long dflt = e2.get("default");
              if (dflt != null) {
                totalTransferred += dflt;
              }
            }
          }
        }
      }
    }
    //long transferredDiff = totalTransferred - state.transferred;
    state.transferred = totalTransferred;
    //double throughput = (transferredDiff == 0 || time == 0) ? 0.0 : (transferredDiff * size)/(1024.0 * 1024.0)/(time/1000.0);
    //System.out.println(message+"\t"+numTopologies+"\t"+totalSlots+"\t"+totalUsedSlots+"\t"+totalExecutors+"\t"+executorsWithMetrics+"\t"+now+"\t"+time+"\t"+transferredDiff+"\t"+throughput);
	System.out.println(message+","+totalSlots+","+totalUsedSlots+","+totalExecutors+","+executorsWithMetrics+","+time+",NOLIMIT");
    if ("WAITING".equals(message)) {
      //System.err.println(" !("+totalUsedSlots+" > 0 && "+slotsUsedDiff+" == 0 && "+totalExecutors+" > 0 && "+executorsWithMetrics+" >= "+totalExecutors+")");
    }
    return !(totalUsedSlots > 0 && slotsUsedDiff == 0 && totalExecutors > 0 && executorsWithMetrics >= totalExecutors);
  } 

 
  public void realMain(String[] args) throws Exception {
    Map clusterConf = Utils.readStormConfig();
    clusterConf.putAll(Utils.readCommandLineOpts());
    Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();
    
    CmdLineParser parser = new CmdLineParser(this);
    parser.setUsageWidth(80);
    try {
      // parse the arguments.
      parser.parseArgument(args);
    } catch( CmdLineException e ) {
      // if there's a problem in the command line,
      // you'll get this exception. this will report
      // an error message.
      System.err.println(e.getMessage());
      _help = true;
    }
    if(_help) {
      parser.printUsage(System.err);
      System.err.println();
      return;
    }
    if (_numWorkers <= 0) {
      throw new IllegalArgumentException("Need at least one worker");
    }
    if (_name == null || _name.isEmpty()) {
      throw new IllegalArgumentException("name must be something");
    }
    if (!_ackEnabled) {
      _ackers = 0;
    }

    try {
    	/*
    	for (int topoNum = 0; topoNum < _numTopologies; topoNum++) {
        
        TopologyBuilder builder = new TopologyBuilder();
        LOG.info("Adding in "+_spoutParallel+" spouts");
        builder.setSpout("messageSpout", 
            new SOLSpout(_messageSize, _ackEnabled), 6);
        
        LOG.info("Adding in "+_boltParallel+" bolts");
        builder.setBolt("messageBoltSG1", new SOLBolt(), 6)
            .shuffleGrouping("messageSpout");
        builder.setBolt("messageBoltFG1", new SOLBolt(), 6)
            .fieldsGrouping("messageBoltSG1", new Fields("fieldValue"));
        
        builder.setBolt("messageBoltLocalResult", new SOLBolt(), 2)
        	.shuffleGrouping("messageBoltFG1");
        
        builder.setBolt("messageBoltSG2", new SOLBolt(), 4)
            .shuffleGrouping("messageBoltFG1");
        builder.setBolt("messageBoltFG2", new SOLBolt(), 4)
            .fieldsGrouping("messageBoltSG2", new Fields("fieldValue"));
        
        builder.setBolt("messageBoltGlobalResult", new SOLBolt(), 2)
        .shuffleGrouping("messageBoltFG2");
        */
    	
    	for (int topoNum = 0; topoNum < _numTopologies; topoNum++) {
            
            TopologyBuilder builder = new TopologyBuilder();
            
            builder.setSpout("messageSpoutA", new SOLSpout(_messageSize, _ackEnabled), 2);
            builder.setSpout("messageSpoutB", new SOLSpout(_messageSize, _ackEnabled), 2);
            builder.setSpout("messageSpoutC", new SOLSpout(_messageSize, _ackEnabled), 2);
                       
            builder.setBolt("messageBoltSG_A", new SOLBolt(), 2).shuffleGrouping("messageSpoutA");
            builder.setBolt("messageBoltSG_B", new SOLBolt(), 2).shuffleGrouping("messageSpoutB");
            builder.setBolt("messageBoltSG_C", new SOLBolt(), 2).shuffleGrouping("messageSpoutC");
            
            builder.setBolt("messageBoltFG_A", new SOLBolt(), 2).fieldsGrouping("messageBoltSG_A", new Fields("fieldValue"));
            builder.setBolt("messageBoltFG_B", new SOLBolt(), 2).fieldsGrouping("messageBoltSG_B", new Fields("fieldValue"));
            builder.setBolt("messageBoltFG_C", new SOLBolt(), 2).fieldsGrouping("messageBoltSG_C", new Fields("fieldValue"));
            
            builder.setBolt("messageBoltLocalResultA", new SOLFinalBolt(), 2).shuffleGrouping("messageBoltFG_A");
            builder.setBolt("messageBoltLocalResultB", new SOLFinalBolt(), 2).shuffleGrouping("messageBoltFG_B");
            builder.setBolt("messageBoltLocalResultC", new SOLFinalBolt(), 2).shuffleGrouping("messageBoltFG_C");
            
            builder.setBolt("messageBoltLocalAggregatorResult", new SOLFinalBolt(), 2)
        		.shuffleGrouping("messageBoltFG_A")
        		.shuffleGrouping("messageBoltFG_B")
        		.shuffleGrouping("messageBoltFG_C");
            
            builder.setBolt("messageBoltSG2", new SOLBolt(), 4)
            	.shuffleGrouping("messageBoltFG_A")
        		.shuffleGrouping("messageBoltFG_B")
        		.shuffleGrouping("messageBoltFG_C");
            
            builder.setBolt("messageBoltFG2", new SOLBolt(), 4)
            	.fieldsGrouping("messageBoltSG2", new Fields("fieldValue"));
            
            builder.setBolt("messageBoltGlobalResult", new SOLFinalBolt(), 2)
            	.shuffleGrouping("messageBoltFG2");
            
            /*****************************************************
             
            builder.setSpout("messageSpoutA", new SOLSpout(_messageSize, _ackEnabled), 6);
                       
            builder.setBolt("messageBoltSG_A", new SOLBolt(), 6)
            	//.shuffleGrouping("messageSpoutA");
            	.customGrouping("messageSpoutA", new ZoneShuffleGrouping());
            
            builder.setBolt("messageBoltFG_A", new SOLBolt(), 6)
            	//.fieldsGrouping("messageBoltSG_A", new Fields("fieldValue"));
            	.customGrouping("messageBoltSG_A", new ZoneFieldsGrouping(new Fields("fieldValue")));
            
            //builder.setBolt("messageBoltFG_B", new SOLBolt(), 2).fieldsGrouping("messageBoltSG_A", new Fields("fieldValue"));
            //builder.setBolt("messageBoltFG_C", new SOLBolt(), 2).fieldsGrouping("messageBoltSG_A", new Fields("fieldValue"));
            
            builder.setBolt("messageBoltLocalResult", new SOLFinalBolt(), 6)
    		.customGrouping("messageBoltFG_A", new ZoneShuffleGrouping());
            
            builder.setBolt("messageBoltLocalAggregatorResult", new SOLFinalBolt(), 2)
        		.shuffleGrouping("messageBoltFG_A");
            
            builder.setBolt("messageBoltSG2", new SOLBolt(), 4)
            	.shuffleGrouping("messageBoltFG_A");
            
            builder.setBolt("messageBoltFG2", new SOLBolt(), 4)
            	.fieldsGrouping("messageBoltSG2", new Fields("fieldValue"));
            	//.customGrouping("messageBoltSG2", new ZoneFieldsGrouping(new Fields("fieldValue")));
            
            builder.setBolt("messageBoltGlobalResult", new SOLFinalBolt(), 2)
            	.globalGrouping("messageBoltFG2");
            	
             *****************************************************/

	        Config conf = new Config();
	        conf.setDebug(_debug);
	        conf.setNumWorkers(_numWorkers);
	        conf.setNumAckers(_ackers);
	        conf.setStatsSampleRate(_sampleRate);        
	        if (_maxSpoutPending > 0) {
	          conf.setMaxSpoutPending(_maxSpoutPending);
        }

        StormSubmitter.submitTopology(_name+"_"+topoNum, conf, builder.createTopology());
      }
      metrics(client, _messageSize, _pollFreqSec, _testRunTimeSec);
    } finally {
      //Kill it right now!!!
      KillOptions killOpts = new KillOptions();
      killOpts.set_wait_secs(0);

      for (int topoNum = 0; topoNum < _numTopologies; topoNum++) {
        LOG.info("KILLING "+_name+"_"+topoNum);
        try {
          client.killTopologyWithOpts(_name+"_"+topoNum, killOpts);
        } catch (Exception e) {
          LOG.error("Error tying to kill "+_name+"_"+topoNum,e);
        }
      }
    }
  }
  
  public static void main(String[] args) throws Exception {
    new StaticMultiZone().realMain(args);
  }
}


package StormOnEdge.state.ZGState;

import backtype.storm.Config;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.curator.framework.CuratorFramework;
import org.apache.storm.curator.framework.imps.CuratorFrameworkState;
import org.mortbay.util.MultiMap;
import backtype.storm.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by ken on 12/7/2015.
 *
 * This ZConnector use the zookeeper used by storm to store the immediate information for
 * Workers to be able to do a correct ZoneGrouping
 *
 * Zookeeper location is read from storm.yaml, with semi-hardcoded location.
 * The Supervisor are expected to start from outside apache-storm folder. In this
 * case this connector will be able to read storm.yaml correctly.
 */
@SuppressWarnings("Duplicates")
public class ZookeeperZGConnector extends ZGConnector {

  private static final Log LOG = LogFactory.getLog(ZookeeperZGConnector.class);

  private String stormID;

  private List<String> zooHosts;
  private Object zkPort;
  private String zkRoot = "/storm/ZKConnector";

  private CuratorFramework client;
  private Map storm_conf;

  @SuppressWarnings("unchecked")
  public ZookeeperZGConnector(String iD) {

    LOG.info("ZGConnector Created");

    stormID = iD;
    storm_conf = Utils.readStormConfig();

    zooHosts = (List<String>) storm_conf.get(Config.STORM_ZOOKEEPER_SERVERS);
    zkPort = storm_conf.get(Config.STORM_ZOOKEEPER_PORT);
    zkRoot = zkRoot + "-" + stormID;
  }

  public void writeInfo() {

    LOG.info("~~~~~~~~~~~~~~~~~~~~~~~~WRITE INFO CALLED~~~~~~~~~~~~");

    StringBuilder taskStringBuilder = new StringBuilder();

    for (Object cloudName : tasksByCloudName.keySet()) {

      @SuppressWarnings("unchecked")
      ArrayList<Integer> taskIDs = (ArrayList<Integer>) tasksByCloudName.getValues(cloudName);
      String cloudString = cloudName.toString();

      for (Integer t : taskIDs) {
        cloudString = cloudString + t.toString() + ",";
      }

      taskStringBuilder.append(cloudString.substring(0, cloudString.length() - 1));
      taskStringBuilder.append("\n");
    }

    System.out.println(taskStringBuilder.toString());
    LOG.info(taskStringBuilder.toString());

    try {
      resetZKConnection();

      //block until ready
      while(client.getState() != CuratorFrameworkState.STARTED) {
        Thread.sleep(500);
        LOG.info("Waiting for ZK");
      }

      if (client.checkExists().forPath(zkRoot) == null)
        client.create().forPath(zkRoot);

      client.setData().forPath(zkRoot, taskStringBuilder.toString().getBytes());
//      if(result2 != null) {
//        System.out.println("Write to " + zkRoot + " complete");
//        System.out.println("length: " + result2.getDataLength());
//      }

      client.close();
    }catch(Exception e){
      e.printStackTrace();
      LOG.info(e.getMessage());
    }

    LOG.info("WRITE FINISHED");
  }

  public MultiMap readInfo() {

    LOG.info("READ INFO CALLED");

    try {
      resetZKConnection();
      LOG.info("zkROOT : " + zkRoot);

      //block until ready
      LOG.info("client.getState() : "  + client.getState());
      LOG.info("client.checkExists() : "  + client.checkExists().forPath(zkRoot));
      while(client.getState() != CuratorFrameworkState.STARTED
              || client.checkExists().forPath(zkRoot) == null
              || client.getData().forPath(zkRoot).length <= 0) {
        LOG.info("client.getState() : "  + client.getState());
        LOG.info("client.checkExists() : "  + client.checkExists().forPath(zkRoot));
        LOG.info("client.getData().forPath(zkRoot) : "  + client.getData().forPath(zkRoot).length);
        Thread.sleep(500);
        LOG.info("Waiting for ZK");
      }

      byte[] data = client.getData().forPath(zkRoot);

      String dataString = new String(data);
      LOG.info("zkConnector : " + dataString);

      if(!dataString.isEmpty()) {

        String[] arrayData = dataString.split("\\r?\\n");

        //Format:
        //cloudA;1,2,3,4,5
        for (int idx = 0; idx < arrayData.length; idx++) {
          LOG.debug("arrayData : " + arrayData[idx]);
          String[] pairString = arrayData[idx].split(";");
          if (pairString.length == 2) {
            String key = pairString[0];
            String[] taskString = pairString[1].split(",");

            for (int ii = 0; ii < taskString.length; ii++)
              tasksByCloudName.add(key, new Integer(taskString[ii]));
          }
        }
      }

      client.close();
    }catch(Exception e){
      e.printStackTrace();
    LOG.info(e.getMessage());}

    return tasksByCloudName;
  }

  private void resetZKConnection() {

    //RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    client = Utils.newCurator(storm_conf,zooHosts,zkPort);
    //client = CuratorFrameworkFactory.newClient(connString,retryPolicy);
    client.start();
  }

}

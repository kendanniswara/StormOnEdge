package StormOnEdge.state.ZGState;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.mortbay.util.MultiMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

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

  private final String zkIPloc = "apache-storm-0.9.3/conf/storm.yaml";

  private String stormID;
  private String connString = "localhost:2181"; //default ZK location
  private String zkPath = "/storm/ZKConnector";

  private CuratorFramework client;

  public ZookeeperZGConnector(String id) {

    stormID = id;
    zkPath = zkPath + "-" + stormID;

    BufferedReader textReader;
    String line;

    try {
      File file = new File(zkIPloc);
      System.out.println("File absolute Path: " + file.getAbsolutePath());

      textReader = new BufferedReader(new FileReader(file));
      line = textReader.readLine();

      while(line != null) {
        if(line.contains("storm.zookeeper.servers"))
        {
          String zooLine = textReader.readLine();
          String[] split = zooLine.split("\"");
          connString = split[1] + ":2181";
          break;
        }

        line = textReader.readLine();
      }

    }catch(Exception e) {e.printStackTrace();}

  }

  public void writeInfo() {

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

    try {
      resetZKConnection();

      if (client.checkExists().forPath(zkPath) == null)
        client.create().forPath(zkPath);

      client.setData().forPath(zkPath, taskStringBuilder.toString().getBytes());
//      if(result2 != null) {
//        System.out.println("Write to " + zkPath + " complete");
//        System.out.println("length: " + result2.getDataLength());
//      }

      client.close();
    }catch(Exception e){e.printStackTrace();}


  }

  public MultiMap readInfo() {

    try {
      resetZKConnection();

      //block until ready
      while(client.checkExists().forPath(zkPath) == null) {
        Thread.sleep(500);
        System.out.println("Waiting for ZK");
      }

      if(client.checkExists().forPath(zkPath) != null) {
        byte[] data = client.getData().forPath(zkPath);

        String dataString = new String(data);
        System.out.print("zkConnector : " + dataString);

        if(!dataString.isEmpty()) {

          String[] arrayData = dataString.split("\\r?\\n");

          //Format:
          //cloudA;1,2,3,4,5
          for (int idx = 0; idx < arrayData.length; idx++) {
            String[] pairString = arrayData[idx].split(";");
            if (pairString.length == 2) {
              String key = pairString[0];
              String[] taskString = pairString[1].split(",");

              for (int ii = 0; ii < taskString.length; ii++)
                tasksByCloudName.add(key, new Integer(taskString[ii]));
            }
          }
        }
      }

      client.close();
    }catch(Exception e){e.printStackTrace();}

    return tasksByCloudName;
  }

  private void resetZKConnection() {

    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    client = CuratorFrameworkFactory.newClient(connString,retryPolicy);

    client.start();
  }

}

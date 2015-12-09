package StormOnEdge.state.ZGState;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.mortbay.util.MultiMap;

import java.util.ArrayList;

/**
 * Created by ken on 12/7/2015.
 */
@SuppressWarnings("Duplicates")
public class ZookeeperZGConnector extends ZGConnector {

  private final String connString = "192.168.60.105:2181";
  private final String zkPath = "/storm/ZKConnector";
  private CuratorFramework client;

  //127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183,127.0.0.1:2184

  public ZookeeperZGConnector() {

  }

  public void writeInfo() {

    StringBuilder taskStringBuilder = new StringBuilder();

    for (Object cloudName : tasksByCloudName.keySet()) {

      @SuppressWarnings("unchecked")
      ArrayList<Integer> taskIDs = (ArrayList<Integer>) tasksByCloudName.get(cloudName);
      String cloudString = cloudName.toString();

      for (Integer t : taskIDs) {
        cloudString = cloudString + t.toString() + ",";
      }

      taskStringBuilder.append(cloudString.substring(0, cloudString.length() - 1));
      taskStringBuilder.append("\n");
    }

    System.out.println(taskStringBuilder.toString());

    try {
      resetZookeeperConn();

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
      resetZookeeperConn();

      if(client.checkExists().forPath(zkPath) != null) {
        byte[] data = client.getData().forPath(zkPath);

        System.out.print("zkConnector : " + new String(data));

        String[] arrayData = new String(data).split("\\r?\\n");

        for (int idx = 0; idx < arrayData.length; idx++) {

          //Format:
          //cloudA;1,2,3,4,5
          String[] pairString = arrayData[idx].split(";");
          String key = pairString[0];
          String[] taskString = pairString[1].split(",");

          for (int ii = 0; ii < taskString.length; ii++) {
            tasksByCloudName.add(key, new Integer(taskString[ii]));
          }
        }
      }

      client.close();
    }catch(Exception e){e.printStackTrace();}

    return tasksByCloudName;
  }

  private void resetZookeeperConn() {

    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    client = CuratorFrameworkFactory.newClient(connString,retryPolicy);

    client.start();
  }

}

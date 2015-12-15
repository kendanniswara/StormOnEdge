package StormOnEdge.state.CloudState;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class FileBasedCloudsInfo implements CloudsInfo {

  private Map storm_config;
  private String fileCloudKey;
  private float[][] twoDimLatency;

  private LinkedList<String> cloudNames;

  public enum Type {
    MinMax,
    Average
  }

  public FileBasedCloudsInfo(String key, Map conf) {
    fileCloudKey = key;
    storm_config = conf;
    init();
  }

  public void update() {
    init();
  }

  private void init() {
    cloudNames = new LinkedList<String>();
    String inputPath = storm_config.get(fileCloudKey).toString();

    //Reading the information from file
    FileReader dataFile;
    BufferedReader textReader;
    String line;
    try {

      dataFile = new FileReader(inputPath);
      textReader = new BufferedReader(dataFile);

      //read first line
      //Format:
      //cloudA,cloudB,cloudC
      line = textReader.readLine();
      String[] cloudList = line.split(",");
      cloudNames.addAll(Arrays.asList(cloudList));
      twoDimLatency = new float[cloudNames.size()][cloudNames.size()];

      //read rest of the lines
      //Format:
      //  0,10,20
      // 10, 0, 5
      // 20, 5, 0
      line = textReader.readLine();
      int lineIdx = 0;
      while (line != null && !line.equals("")) {
        String[] latencies = line.split(",");
        for (int i = 0; i < latencies.length; i++) {
          twoDimLatency[lineIdx][i] = Float.parseFloat(latencies[i]);
        }
        lineIdx++;
        line = textReader.readLine();
      }

      textReader.close();

    } catch (IOException e) {
      System.out.println(e.getMessage());
    }
  }

  public List<String> getCloudNames() {
    if (cloudNames == null) {
      init();
    }

    return (List<String>) this.cloudNames.clone();
  }

  public float getLatency(String cloudName1, String cloudName2) {
    if (cloudNames.contains(cloudName1) && cloudNames.contains(cloudName2)) {
      int idxC1 = cloudNames.indexOf(cloudName1);
      int idxC2 = cloudNames.indexOf(cloudName2);
      return twoDimLatency[idxC1][idxC2];
    } else {
      return -1; //error when retrieving information
    }
  }

  public String bestCloud() {
    return bestCloud(Type.Average, new HashSet<String>(cloudNames), new HashSet<String>(cloudNames));
  }

  public String bestCloud(FileBasedCloudsInfo.Type type, Set<String> participatedClouds, Set<String> dependencies) {

    if (type == Type.MinMax) {
      return minMaxLatency(participatedClouds, dependencies);
    } else if (type == Type.Average) {
      return avgLatency(participatedClouds, dependencies);
    } else {
      return null;
    }
  }

  private String minMaxLatency(Set<String> cloudNameList, Set<String> cloudDependencies) {
    String bestCloud = null;
    float lowestMaxLatency = Float.MAX_VALUE;

    System.out.println("Start MinMaxLatency: ");

    for (String cloud : cloudNameList) {
      float currentMaxLatency = 0;
      //Skip clouds that are not registered in the latency table
      if (cloudNames.indexOf(cloud) == -1) {
        continue;
      }

      //Make sure the chosen cloud is different from their dependencies
      //if(cloudDependencies.contains(cloud))
      //	continue;
      for (String dependency : cloudDependencies) {
        int idxDep = cloudNames.indexOf(dependency);
        int idxCl = cloudNames.indexOf(cloud);
        float lat = twoDimLatency[idxDep][idxCl];
        if (lat > currentMaxLatency) {
          currentMaxLatency = lat;
        }
      }

      if (currentMaxLatency < lowestMaxLatency) {
        lowestMaxLatency = currentMaxLatency;
        bestCloud = cloud;
        System.out.println("bestCloud changed into: " + bestCloud + "=" + lowestMaxLatency);
      }

    }

    return bestCloud;
  }

  private String avgLatency(Set<String> cloudNameList, Set<String> cloudDependencies) {

    String bestCloud = null;
    float lowestAvgLatency = Float.MAX_VALUE;

    System.out.println("Start avgLatency: ");

    for (String cloud : cloudNameList) {
      float avgLatency = 0;

      //Make sure the chosen cloud is different from their dependencies
      //if(cloudDependencies.contains(cloud))
      //	continue;
      for (String dependency : cloudDependencies) {
        int idxDep = cloudNames.indexOf(dependency);
        int idxCl = cloudNames.indexOf(cloud);
        avgLatency += twoDimLatency[idxDep][idxCl];
      }

      avgLatency = avgLatency / cloudDependencies.size();

      if (avgLatency < lowestAvgLatency) {
        lowestAvgLatency = avgLatency;
        bestCloud = cloud;
        System.out.println("bestCloud changed into: " + bestCloud + "=" + lowestAvgLatency);
      }
    }

    return bestCloud;
  }

}

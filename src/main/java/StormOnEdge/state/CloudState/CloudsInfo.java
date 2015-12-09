package StormOnEdge.state.CloudState;

import java.util.List;
import java.util.Set;

public interface CloudsInfo {

  public float getLatency(String cloudName1, String cloudName2);

  public String bestCloud();
  public String bestCloud(FileBasedCloudsInfo.Type type, Set<String> participatedClouds, Set<String> dependencies);
  
  public List<String> getCloudNames();

}

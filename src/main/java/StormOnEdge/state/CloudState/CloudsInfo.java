package StormOnEdge.state.CloudState;

import java.util.List;
import java.util.Set;

public interface CloudsInfo {

  float getLatency(String cloudName1, String cloudName2);

  String bestCloud();
  String bestCloud(FileBasedCloudsInfo.Type type, Set<String> participatedClouds, Set<String> dependencies);
  
  List<String> getCloudNames();

}

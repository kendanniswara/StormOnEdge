package StormOnEdge.grouping.topology;

import java.util.LinkedHashMap;

public class LocalTaskGroup extends TaskGroup {

  public LocalTaskGroup(String Groupname) {
    super(Groupname);
  }

  public LinkedHashMap<String, Integer> spoutsWithCloudParallelizationInfo = new LinkedHashMap<String, Integer>();
}

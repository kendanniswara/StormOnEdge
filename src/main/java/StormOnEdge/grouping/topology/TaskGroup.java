package StormOnEdge.grouping.topology;

import StormOnEdge.scheduler.CloudAssignment;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

public class TaskGroup {

  public TaskGroup(String Groupname) {
    name = Groupname;
  }

  public String name;
  //public List<String> clouds = new ArrayList<String>();
  public List<CloudAssignment> taskGroupClouds = new ArrayList<CloudAssignment>();

  //Gives information about each task and how many executor should be placed on each cloud
  public LinkedHashMap<String, Integer> boltsWithCloudParallelizationInfo = new LinkedHashMap<String, Integer>();

  public Set<String> boltDependencies = new HashSet<String>();
}

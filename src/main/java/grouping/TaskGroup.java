package grouping;

import scheduler.CloudAssignment;
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
  public LinkedHashMap<String, Integer> boltsWithParInfo = new LinkedHashMap<String, Integer>();
  public Set<String> boltDependencies = new HashSet<String>();
}

package grouping;

import java.util.LinkedHashMap;

public class LocalTaskGroup extends TaskGroup {

  public LocalTaskGroup(String Groupname) {
    super(Groupname);
  }

  public LinkedHashMap<String, Integer> spoutsWithParInfo = new LinkedHashMap<String, Integer>();
}

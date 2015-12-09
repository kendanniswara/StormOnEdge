package StormOnEdge.state.ZGState;

import java.util.HashMap;

import org.mortbay.util.MultiMap;
import StormOnEdge.scheduler.CloudAssignment;

public abstract class ZGConnector {

  protected final MultiMap tasksByCloudName = new MultiMap();

  public abstract void writeInfo();

  public abstract MultiMap readInfo();

  public void addInfo(HashMap<String, CloudAssignment> clouds) {

    for (CloudAssignment c : clouds.values()) {
      String cloudName = (String) c.getName() + ";";

      if (!c.getTasks().isEmpty()) {
        for (Integer t : c.getTasks()) {
          tasksByCloudName.add(cloudName, t);
        }
      }
    }
  }
}

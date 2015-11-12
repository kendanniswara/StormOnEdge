package external;

import java.util.HashMap;

import org.mortbay.util.MultiMap;
import core.Cloud;

public abstract class ZGConnector {

  protected MultiMap tasksByCloudName = new MultiMap();

  public abstract void writeInfo();

  public abstract MultiMap readInfo();

  public void addInfo(HashMap<String, Cloud> clouds) {

    for (Cloud c : clouds.values()) {
      String cloudName = (String) c.getName() + ";";

      if (!c.getTasks().isEmpty()) {
        for (Integer t : c.getTasks()) {
          tasksByCloudName.add(cloudName, t);
        }
      }
    }
  }
}

package state;

import java.util.HashMap;

import org.mortbay.util.MultiMap;
import scheduler.CloudAssignment;

public interface ZGConnector {

  public void writeInfo();

  public MultiMap readInfo();

  public void addInfo(HashMap<String, CloudAssignment> clouds);
}

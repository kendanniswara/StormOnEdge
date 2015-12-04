package state.ZGState;

import java.util.HashMap;

import org.mortbay.util.MultiMap;
import scheduler.CloudAssignment;

public interface ZGConnector {

  void writeInfo();

  MultiMap readInfo();

  void addInfo(HashMap<String, CloudAssignment> clouds);
}

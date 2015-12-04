package state.SourceState;

import java.util.ArrayList;
import java.util.Set;

public interface SourceInfo {

  public ArrayList<String> getCloudLocations(String spoutName);

  public Set<String> getSpoutNames();
}

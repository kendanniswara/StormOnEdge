package state.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import state.SourceInfo;

public class FileSourceInfo implements SourceInfo {

  private HashMap<String, ArrayList<String>> spoutCloudsPair = new HashMap<String, ArrayList<String>>();
  private final Map storm_config;
  final String CONF_sourceCloudKey = "geoScheduler.sourceCloudList";

  public FileSourceInfo(Map conf) throws FileNotFoundException {
    storm_config = conf;
    String inputPath = storm_config.get(CONF_sourceCloudKey).toString();

    if (inputPath != null || checkFileAvailablity(inputPath)) {
      try {
        String line;
        FileReader pairDataFile = new FileReader(inputPath);
        BufferedReader textReader = new BufferedReader(pairDataFile);

        line = textReader.readLine();
        while (line != null && !line.equals("")) {
          //Format
          //SpoutID;cloudA,cloudB,cloudC
          System.out.println("~~Read from file: " + line);
          String[] pairString = line.split(";");
          String spoutName = pairString[0];
          String[] cloudList = pairString[1].split(",");
          addCloudLocations(spoutName, cloudList);

          line = textReader.readLine();
        }
        textReader.close();
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    } else {
      throw new FileNotFoundException();
    }

  }

  @Override
  public ArrayList<String> getCloudLocations(String spoutName) {
    if (spoutCloudsPair.containsKey(spoutName)) {
      return spoutCloudsPair.get(spoutName);
    } else {
      return new ArrayList<String>(); //return empty list
    }
  }

  private boolean checkFileAvailablity(String path) {
    File file = new File(path);
    if (file.exists()) {
      return true;
    } else {
      return false;
    }
  }

  public Set<String> getSpoutNames() {
    return spoutCloudsPair.keySet();
  }

  private void addCloudLocation(String spoutName, String cloudName) {
    if (spoutCloudsPair.containsKey(spoutName)) {
      spoutCloudsPair.get(spoutName).add(cloudName);
    } else {
      ArrayList<String> clouds = new ArrayList<String>();
      clouds.add(cloudName);
      spoutCloudsPair.put(spoutName, clouds);
    }
  }

  private void addCloudLocations(String spoutName, String[] cloudNames) {
    for (String cloudName : cloudNames) {
      addCloudLocation(spoutName, cloudName);
    }
  }

  protected void addCloudLocations(String spoutName, ArrayList<String> cloudNames) {
    for (String cloudName : cloudNames) {
      addCloudLocation(spoutName, cloudName);
    }
  }

}

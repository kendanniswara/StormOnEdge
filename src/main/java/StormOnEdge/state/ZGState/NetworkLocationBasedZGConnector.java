package StormOnEdge.state.ZGState;

import org.mortbay.util.MultiMap;
import StormOnEdge.scheduler.CloudAssignment;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;

@SuppressWarnings("Duplicates")
public class NetworkLocationBasedZGConnector extends ZGConnector {

  final String fileName = "Result-ZoneGrouping.txt";
  String fileLocation = "";

  private final MultiMap tasksByCloudName = new MultiMap();

  public NetworkLocationBasedZGConnector() {

  }

  public void writeInfo() {

    String resultPath = fileLocation + fileName;

    writeInfoFromNetworkFile(resultPath);
  }

  public MultiMap readInfo() {

    String resultPath = fileLocation + fileName;

    if(checkFileAvailability(resultPath))
      return readInfoFromNetworkFile(resultPath);
    else
      return new MultiMap();
  }

  private MultiMap readInfoFromNetworkFile(String resultPath) {
    try {

      FileReader supervisorTaskFile = new FileReader(resultPath);
      BufferedReader textReader = new BufferedReader(supervisorTaskFile);

      String line = textReader.readLine();
      while (line != null && !line.equals("")) {
        //Format:
        //cloudA;1,2,3,4,5
        String[] pairString = line.split(";");
        String key = pairString[0];
        String[] taskString = pairString[1].split(",");

        for (int ii = 0; ii < taskString.length; ii++) {
          tasksByCloudName.add(key, new Integer(taskString[ii]));
        }

        line = textReader.readLine();
      }

      textReader.close();

    } catch (Exception e) {
      System.out.println(e.getMessage());
      return null;
    }

    return tasksByCloudName;
  }

  private void writeInfoFromNetworkFile(String resultPath) {

    StringBuilder taskStringBuilder = new StringBuilder();

    for (Object cloudName : tasksByCloudName.keySet()) {

      @SuppressWarnings("unchecked")
      ArrayList<Integer> taskIDs = (ArrayList<Integer>) tasksByCloudName.get(cloudName);
      String cloudString = cloudName.toString();

      for (Integer t : taskIDs) {
        cloudString = cloudString + t.toString() + ",";
      }

      taskStringBuilder.append(cloudString.substring(0, cloudString.length() - 1));
      taskStringBuilder.append("\n");

    }
    System.out.println(taskStringBuilder.toString());

    try {

      URL url = new URL(resultPath);
      URLConnection connection = url.openConnection();
      connection.setDoOutput(true);

      OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream());
      out.write(taskStringBuilder.toString());
      out.close();

    } catch (Exception e) {
      System.out.println(e.getMessage());
      System.out.println("WARNING: No file provided for the ZoneGrouping!");
    }
  }

  private boolean checkFileAvailability(String path) {
    File file = new File(path);

    return file.exists();
  }

  private boolean checkFolderAvailability(String path) {
    File file = new File(path);

    return file.getParentFile().exists();
  }

}

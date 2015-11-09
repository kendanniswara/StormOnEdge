package external;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

public class FileSourceInfo extends SourceInfo {

	Map storm_config;
	final String CONF_sourceCloudKey = "geoScheduler.sourceCloudList";
	
	public FileSourceInfo(Map conf) throws FileNotFoundException {
		storm_config = conf;
		String inputPath = storm_config.get(CONF_sourceCloudKey).toString();
		
		if(inputPath != null || checkFileAvailablity(inputPath)) {
			try {
				String line;
				FileReader pairDataFile = new FileReader(inputPath);
				BufferedReader textReader  = new BufferedReader(pairDataFile);
				
				line = textReader.readLine();
				while(line != null && !line.equals(""))
				{
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
			}
			catch(Exception e) {
				System.out.println(e.getMessage());
			}
		}
		else{
			throw new FileNotFoundException();
		}
		
	}
	
	@Override
	protected void init() {
		
		
	}
	
	private boolean checkFileAvailablity(String path)
    {
    	File file = new File(path);
    	if (file.exists())
    	    return true;
    	else
    		return false;
    }

}

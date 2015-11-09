package external;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class LatencyCloudsState extends CloudsState {
	
	final String CONF_cloudLocatorKey = "geoScheduler.cloudInformation";
	private float[][] twoDimLatency;
	
	public enum Type {
		MinMax,
		Average
	}
	
	@Override
	public void update() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void init() {
		// TODO Auto-generated method stub

	}
	
	public LatencyCloudsState(String fileName){
		init(fileName);
	}
	
	public void update(String fileName){
		init(fileName);
	}
	
	private void init(String fileName)
	{
		cloudNames = new ArrayList<String>();
		
		//Reading the information from file
	    FileReader dataFile; 
	    BufferedReader textReader;
	    String line;
	    try {
		    
	    	//---------------------------
		    //get spout & cloud_list pair
		    //---------------------------
		    dataFile = new FileReader(fileName);
		    textReader  = new BufferedReader(dataFile);
		    		    
		    //read first line
		    //Format:
	    	//cloudA,cloudB,cloudC
		    line = textReader.readLine();
		    String[] cloudList = line.split(",");
		    for(String cloud : cloudList)
		    	cloudNames.add(cloud);
		    twoDimLatency = new float[cloudNames.size()][cloudNames.size()];
		    
		    //read rest of the lines
		    //Format:
	    	//  0,10,20
	    	// 10, 0, 5
	    	// 20, 5, 0
		    line = textReader.readLine();
		    int lineIdx = 0;
		    while(line != null && !line.equals(""))
		    {
		    	String[] latencies = line.split(",");
		    	for(int i = 0; i < latencies.length; i++)
		    	{
		    		twoDimLatency[lineIdx][i] = Float.parseFloat(latencies[i]);
		    	}
		    	lineIdx++;
		    	line = textReader.readLine();
		    }
		    
		    textReader.close();
		    
	    }catch(IOException e){}
	}
	
	public String getCloudBasedOnLatency(LatencyCloudsState.Type type, Set<String> cloudNameList, Set<String> cloudDependencies)
	{
		
		String cloud = null;
		
		if(type == Type.MinMax)
			cloud = minMaxLatency(cloudNameList, cloudDependencies);
		else if(type == Type.Average)
			cloud = avgLatency(cloudNameList, cloudDependencies);
			
		return cloud;
	}

	private String minMaxLatency(Set<String> cloudNameList, Set<String> cloudDependencies) {
		String bestCloud = null;
		float lowestMaxLatency = Float.MAX_VALUE;
		
		System.out.println("Start MinMaxLatency: ");
		
		for(String cloud : cloudNameList)
		{
			float currentMaxLatency = 0;
			//Skip clouds that are not registered in the latency table
			if(cloudNames.indexOf(cloud) == -1)
				continue;
			
			//Make sure the chosen cloud is different from their dependencies
			//if(cloudDependencies.contains(cloud))
			//	continue;
			
			for(String dependency : cloudDependencies)
			{
				int idxDep = cloudNames.indexOf(dependency);
				int idxCl = cloudNames.indexOf(cloud);
				float lat = twoDimLatency[idxDep][idxCl];
				if(lat > currentMaxLatency)
					currentMaxLatency = lat;
			}
			
			if(currentMaxLatency < lowestMaxLatency)
			{
				lowestMaxLatency = currentMaxLatency;
				bestCloud = cloud;
				System.out.println("bestCloud changed into: " + bestCloud + "=" + lowestMaxLatency);
			}
			
		}
    	
    	return bestCloud;
	}
	
	private String avgLatency(Set<String> cloudNameList, Set<String> cloudDependencies) {
		
		String bestCloud = null;
		float lowestAvgLatency = Float.MAX_VALUE;
		
		for(String cloud : cloudNameList)
		{
			float avgLatency = 0;
			
			//Make sure the chosen cloud is different from their dependencies
			//if(cloudDependencies.contains(cloud))
			//	continue;

			for(String dependency : cloudDependencies)
			{
				int idxDep = cloudNames.indexOf(dependency);
				int idxCl = cloudNames.indexOf(cloud);
				avgLatency += twoDimLatency[idxDep][idxCl];
			}
			
			avgLatency = avgLatency / cloudDependencies.size();
			
			if(avgLatency < lowestAvgLatency)
			{
				lowestAvgLatency = avgLatency;
				bestCloud = cloud;
				System.out.println("bestCloud changed into: " + bestCloud + "=" + lowestAvgLatency);
			}
			
		}
    	
    	return bestCloud;
	}


}

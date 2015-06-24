package scheduler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class CloudLocator {
	
	ArrayList<String> cloudNames;
	int[][] twoDimLatency;
	
	public CloudLocator(String fileName){
		read2DArrayFromFile(fileName);
		
	}
	
	public void update(String fileName){
		read2DArrayFromFile(fileName);
	}
	
	private void read2DArrayFromFile(String fileName)
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
		    twoDimLatency = new int[cloudNames.size()][cloudNames.size()];
		    
		    //read rest of the lines
		    //Format:
	    	//0,10,20
	    	//10,0,5
	    	//20,5,0
		    line = textReader.readLine();
		    int lineIdx = 0;
		    while(line != null && !line.equals(""))
		    {
		    	String[] latencies = line.split(",");
		    	for(int i = 0; i < latencies.length; i++)
		    	{
		    		twoDimLatency[lineIdx][i] = Integer.parseInt(latencies[i]);
		    	}
		    	lineIdx++;
		    	line = textReader.readLine();
		    }
		    
		    textReader.close();
		    
	    }catch(IOException e){}
	}

	public String MinMaxLatency(List<String> cloudNameList, Set<String> cloudDependencies) {
		// TODO Auto-generated method stub
    	
		//return null;
    	return "CloudMidA";
	}
}

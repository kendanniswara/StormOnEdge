package external;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public abstract class SourceInfo {
	private HashMap<String,ArrayList<String>> spoutCloudsPair = new HashMap<String, ArrayList<String>>();
	
	protected abstract void init();
	
	public ArrayList<String> getCloudLocations(String spoutName) {
		if(spoutCloudsPair.containsKey(spoutName))
			return spoutCloudsPair.get(spoutName);
		else
			return new ArrayList<String>(); //return empty list
	}
	
	public Set<String> getSpoutNames() {
		return spoutCloudsPair.keySet();
	}
	
	private void addCloudLocation(String spoutName, String cloudName) {
		if(spoutCloudsPair.containsKey(spoutName)) {
			spoutCloudsPair.get(spoutName).add(cloudName);
		}
		else {
			ArrayList<String> clouds = new ArrayList<String>();
			clouds.add(cloudName);
			spoutCloudsPair.put(spoutName, clouds);
		}
	}
	
	protected void addCloudLocations(String spoutName, String[] cloudNames) {
		for(String cloudName : cloudNames) {
			addCloudLocation(spoutName, cloudName);
		}
	}
	
	protected void addCloudLocations(String spoutName, ArrayList<String> cloudNames)
	{
		for(String cloudName : cloudNames) {
			addCloudLocation(spoutName, cloudName);
		}
	}	
}

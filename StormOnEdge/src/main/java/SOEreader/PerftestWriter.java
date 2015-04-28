package SOEreader;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologyInfo;

public class PerftestWriter {
	
	public void print(ClusterSummary summary, TopologyInfo info, HashMap<String, Long> previousMap)
	{
		HashMap<String, Long> hostTupleMap = new HashMap<String, Long>();
		
		for(SupervisorSummary sup : summary.get_supervisors())
		{
			hostTupleMap.put(sup.get_host(), (long) 0);
			System.out.print(sup.get_host()+",");
		}
		System.out.println();
		
		for (ExecutorSummary es: info.get_executors()) {
			ExecutorStats stats = es.get_stats();
			if (stats != null) {
				Long tuples = getStatValueFromMap(stats.get_transferred(), ":all-time");
				if ( tuples == null)
					tuples = (long) 0;
				
				Long newl = (long) 0;
				if(hostTupleMap.containsKey(es.get_host()))
				{
					newl = hostTupleMap.get(es.get_host());
				}
				
				newl = newl + tuples;
				hostTupleMap.put(es.get_host(), newl);
			}
		}
		
		Iterator<String> mapIterator = hostTupleMap.keySet().iterator();
		while(mapIterator.hasNext())
		{
			String key = mapIterator.next();
			
			Long tuples = hostTupleMap.get(key);
			Long oldtuples;
			
			if(previousMap.containsKey(key))
				oldtuples = previousMap.get(mapIterator.next());
			else
				oldtuples = (long) 0;
			
			System.out.print(tuples-oldtuples + ",");
			
			previousMap.put(key, tuples);
		}
		System.out.println("");
	}
	
	public Long getStatValueFromMap(Map<String, Map<String, Long>> map, String statName) {
		  Long statValue = null;
		  Map<String, Long> intermediateMap = map.get(statName);
		  statValue = intermediateMap.get("default");
		  return statValue;
		 }
	
	public Long getBoltStatLongValueFromMap(Map<String, Map<GlobalStreamId, Long>> map, String statName) {
		  Long statValue = null;
		  Map<GlobalStreamId, Long> intermediateMap = map.get(statName);
		  Set<GlobalStreamId> key = intermediateMap.keySet();
		  if(key.size() > 0) {
		   Iterator<GlobalStreamId> itr = key.iterator();
		   statValue = intermediateMap.get(itr.next());
		  }
		  return statValue;
		 }
}

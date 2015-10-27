package performance_test;

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
	
	public static void print(ClusterSummary summary, TopologyInfo info, HashMap<String, Long> previousMap)
	{
		StringBuilder keyString = new StringBuilder();
		StringBuilder tupleString = new StringBuilder();
		HashMap<String, Long> hostTupleMap = new HashMap<String, Long>();
		for(SupervisorSummary sup : summary.get_supervisors()) {
			hostTupleMap.put(sup.get_host(), (long) 0);
		}

		for (ExecutorSummary es: info.get_executors()) {
			ExecutorStats stats = es.get_stats();
			if (stats != null) {
				Long tuples = getStatValueFromMap(stats.get_transferred(), ":all-time");
				if ( tuples == null)
					tuples = (long) 0;
					Long newl = (long) 0;
				if(hostTupleMap.containsKey(es.get_host())) {
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
			
			keyString.append(key + ",");
			tupleString.append((tuples-oldtuples) + ",");
			previousMap.put(key, tuples);
		}
		
		System.out.println(keyString.toString());
		System.out.print(tupleString.toString());
	//System.out.println("");
	}
	
	public static void print2(ClusterSummary summary, TopologyInfo info, HashMap<String, Long> previousMap)
	{
		StringBuilder keyString = new StringBuilder();
		StringBuilder tupleString = new StringBuilder();
		HashMap<String, Long> hostTupleMap = new HashMap<String, Long>();
		for(SupervisorSummary sup : summary.get_supervisors()) {
			hostTupleMap.put(sup.get_host(), (long) 0);
		}

		for (ExecutorSummary es: info.get_executors()) {
			ExecutorStats stats = es.get_stats();
			if (stats != null) {
				Long tuples = getStatValueFromMap(stats.get_transferred(), ":all-time");
				if ( tuples == null)
					tuples = (long) 0;
					Long newl = (long) 0;
				if(hostTupleMap.containsKey(es.get_host())) {
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
			
			keyString.append(key + ",");
			tupleString.append((tuples-oldtuples) + ",");
			previousMap.put(key, tuples);
		}
		
		//System.out.println(keyString.toString());
		//System.out.print(tupleString.toString());
	//System.out.println("");
	}
	
	public static Long getStatValueFromMap(Map<String, Map<String, Long>> map, String statName) {
		  Long statValue = null;
		  Map<String, Long> intermediateMap = map.get(statName);
		  statValue = intermediateMap.get("default");
		  
		  if(statValue == null)
			  return (long) 0;
		  else
			  return statValue;
		 }
	
	public static Long getBoltStatLongValueFromMap(Map<String, Map<GlobalStreamId, Long>> map, String statName) {
		  Long statValue = null;
		  Map<GlobalStreamId, Long> intermediateMap = map.get(statName);
		  Set<GlobalStreamId> key = intermediateMap.keySet();
		  if(key.size() > 0) {
		   Iterator<GlobalStreamId> itr = key.iterator();
		   statValue = intermediateMap.get(itr.next());
		  }
		  if(statValue == null)
			  return (long) 0;
		  else
			  return statValue;
		 }
}

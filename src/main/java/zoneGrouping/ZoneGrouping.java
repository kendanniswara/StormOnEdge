package zoneGrouping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mortbay.util.MultiMap;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import external.FileBasedZGConnector;
import external.ZGConnector;

public abstract class ZoneGrouping implements CustomStreamGrouping {

  private static final long serialVersionUID = 1L;
  protected Map<String, Object> config = new HashMap<String, Object>();

  protected MultiMap supervisorTaskMap;
  protected HashMap<Integer, String> taskSupNameMap;

  protected HashMap<Integer, List<Integer>> taskResultList = new HashMap<Integer, List<Integer>>();
  protected List<Integer> targetList;

  @SuppressWarnings("unchecked")
  public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {

    config.put("geoScheduler.out-ZoneGrouping", "data/Result-ZoneGrouping.txt"); //hardcoded
    ZGConnector zgConnector = new FileBasedZGConnector(config);
    supervisorTaskMap = zgConnector.readInfo();
    taskSupNameMap = convertKeyValue(supervisorTaskMap);

    targetList = targetTasks;

    for (Integer source : context.getComponentTasks(stream.get_componentId())) {
      taskResultList.put(source, findIntersections(targetTasks, (List<Integer>) supervisorTaskMap.get(taskSupNameMap.get(source))));
    }

    /*
     //Extra for debugging
     StringBuilder sb = new StringBuilder();
		
     sb.append("-------------------------\n");
     sb.append(taskSupNameMap.toString() + "\n");
     sb.append("-------------------------\n\n");
		
     sb.append(context.getThisWorkerPort() + ":\n");
     sb.append("Tasks in this worker:" + context.getThisWorkerTasks() + "\n");
     sb.append("Source tasks:" + context.getComponentTasks(stream.get_componentId()) + "\n");
     sb.append("Supervisor Name:" + taskSupNameMap.get(context.getThisWorkerTasks().get(1)) + "\n");
     sb.append("Output location: " + "\n");
     for(Integer i : targetTasks)
     {
     sb.append(i + " = " + taskSupNameMap.get(i) + "\n");
     }
     List<Integer> targets = (List<Integer>) supervisorTaskMap.get(taskSupNameMap.get(context.getThisWorkerTasks().get(1)));
     sb.append("Choosen target : " + targets + "\n");
		
     try {	
     FileWriter writer = new FileWriter("/home/kend/groupingTask.csv", true);
     writer.write(sb.toString());
     writer.close();
     }catch(Exception e){ }
     */
  }

  private List<Integer> findIntersections(List<Integer> choosenTasks, List<Integer> fromSupervisor) {
    List<Integer> temp = new ArrayList<Integer>();

    for (Integer i : choosenTasks) {
      if (fromSupervisor.contains(i)) {
        temp.add(i);
      }
    }
    return temp;
  }

  private HashMap<Integer, String> convertKeyValue(MultiMap supervisorTaskMap2) {
    HashMap<Integer, String> taskMap = new HashMap<Integer, String>();

    for (Object cloudName : supervisorTaskMap2.keySet()) {
      @SuppressWarnings("unchecked")
      ArrayList<Integer> taskIDs = (ArrayList<Integer>) supervisorTaskMap2.get(cloudName);
      String cloudString = cloudName.toString();

      for (Integer t : taskIDs) {
        taskMap.put(t, cloudString);
      }
    }

    return taskMap;
  }

}

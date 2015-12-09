package StormOnEdge.grouping.stream;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.WorkerTopologyContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ZoneShuffleGrouping extends ZoneGrouping {

  private static final long serialVersionUID = -4062741858518237161L;

  Random rand = new Random();

  public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
    super.prepare(context, stream, targetTasks);
  }

  public List<Integer> chooseTasks(int taskId, List<Object> values) {

    List<Integer> result = taskResultList.get(new Integer(taskId));
    List<Integer> singleResult = new ArrayList<Integer>(1);

    if (result == null || result.isEmpty()) {
      result = targetList;
    }

    int resultIdx = rand.nextInt(result.size());
    singleResult.add(result.get(resultIdx));

    return singleResult;
    //return setIntersections((List<Integer>) supervisorTaskMap.get(taskSupNameMap.get(new Integer(taskId))));
  }

}

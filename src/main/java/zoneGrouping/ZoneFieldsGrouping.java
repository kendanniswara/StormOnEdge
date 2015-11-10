package zoneGrouping;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.storm.guava.hash.HashFunction;
import org.apache.storm.guava.hash.Hashing;

public class ZoneFieldsGrouping extends ZoneGrouping {

	private static final long serialVersionUID = -4062741858518237161L;
	
	Fields inputFields;
	Fields outputFields;
	HashFunction h1 = Hashing.murmur3_128();
	
	public ZoneFieldsGrouping(Fields f) {
		inputFields = f;
	}
	
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		
		if (inputFields != null) {
            outputFields = context.getComponentOutputFields(stream);
        }
		
		super.prepare(context, stream, targetTasks);
	}

	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		
		List<Integer> taskDefault = taskResultList.get(new Integer(taskId));
		List<Integer> hashResultTask = new ArrayList<Integer>(1);
		
		if (taskDefault == null || taskDefault.isEmpty())
			taskDefault = targetList;
		
		//Taken from PartialKeyGrouping.java
		if (values.size() > 0) {
            byte[] raw = null;
            if (inputFields != null) {
                List<Object> selectedFields = outputFields.select(inputFields, values);
                ByteBuffer out = ByteBuffer.allocate(selectedFields.size() * 4);
                for (Object o: selectedFields) {
                    out.putInt(o.hashCode());
                }
                raw = out.array();
            } else {
                raw = values.get(0).toString().getBytes(); // assume key is the first field
            }
            
            int Choice = (int) (Math.abs(h1.hashBytes(raw).asLong()) % taskDefault.size());
            hashResultTask.add(taskDefault.get(Choice));
        }
		
		return hashResultTask;
	}
	
}

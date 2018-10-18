package trident01.hello;

import java.util.List;
import java.util.Map;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class CountryRepartition implements CustomStreamGrouping {
	private static final long serialVersionUID = 1L;
	private static final Map<String, Integer> countries = ImmutableMap.of("Brazil", 1, "Russia", 2, "India", 3, "China", 4, "SouthAfrica", 5);
	private int tasks = 0;
	
	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		this.tasks = targetTasks.size();
	}

	/**
	 * 自定义再分区定义方法
	 * 
	 */
	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		String country = (String) values.get(0);
		return ImmutableList.of(countries.get(country) % tasks); //[int]
	}

}

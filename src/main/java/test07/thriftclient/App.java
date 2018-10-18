package test07.thriftclient;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.Nimbus.Client;
import org.apache.storm.generated.NimbusSummary;
import org.apache.storm.generated.SupervisorSummary;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.thrift.TException;

/**
 * App
 * @author kinglyjn
 * @date 2018年8月13日
 *
 */
public class App {
	
	public static void main(String[] args) throws AuthorizationException, TException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Client client = ThriftClient.getNimbusClient();
		
		// 获取 ClusterSummary NimbusSummary SupervisorSummary 及其详细信息
		ClusterSummary clusterInfo = client.getClusterInfo();
		//
		List<NimbusSummary> nimbuses = clusterInfo.get_nimbuses();
		nimbuses.forEach(System.out::println);
		//
		List<SupervisorSummary> supervisors = clusterInfo.get_supervisors();
		supervisors.forEach(System.out::println);
		// 
		for (SupervisorSummary supervisorSummary : supervisors) {
			System.out.println("===============================");
			Method[] declaredMethods = SupervisorSummary.class.getDeclaredMethods();
			for (Method method : declaredMethods) {
				if (method.getName().startsWith("get") && (method.getParameters()==null || method.getParameters().length==0)) {
					Object ret = method.invoke(supervisorSummary);
					System.out.println(method.getName() + ": " + ret);
				}
			}
			
		}
		
		// 获取所有的topology及其详细信息
		List<TopologySummary> topologySummarys = clusterInfo.get_topologies();
		System.out.println(topologySummarys);
		for (TopologySummary topologySummary : topologySummarys) {
			System.out.println("===============================");
			Method[] declaredMethods = TopologySummary.class.getDeclaredMethods();
			for (Method method : declaredMethods) {
				if (method.getName().startsWith("get") && (method.getParameters()==null || method.getParameters().length==0)) {
					Object ret = method.invoke(topologySummary);
					System.out.println(method.getName() + ": " + ret);
				}
			}
		}
		
		// 获取 nimbusConf
		String nimbusConf = client.getNimbusConf();
		System.out.println(nimbusConf);
	}
}

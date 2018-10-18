package test07.thriftclient;

import org.apache.storm.generated.Nimbus.Client;
import org.apache.storm.thrift.protocol.TBinaryProtocol;
import org.apache.storm.thrift.transport.TFramedTransport;
import org.apache.storm.thrift.transport.TSocket;


/**
 * ThriftClient
 * @author kinglyjn
 * @date 2018年8月13日
 *
 */
public class ThriftClient {
	private static final String STORM_UI_NODE = "storm01";
	private static final int STORM_UI_PORT = 6627;
	private static Client client;
	
	private static Client createNimbusClient(String stormUiNode, int port) {
		TSocket socket = new TSocket(STORM_UI_NODE, 6627); //storm ui端口的套接字，nimbus.thrift.port=6627
		TFramedTransport transport = new TFramedTransport(socket);
		TBinaryProtocol protocol = new TBinaryProtocol(transport);
		Client client = new Client(protocol);
		try {
			transport.open();
		} catch (Exception e) {
			throw new RuntimeException("连接thrift server时发生异常！");
		}
		return client;
	}
	
	/**
	 * 获取单例的 Nimbus.Client
	 * 
	 */
	public static Client getNimbusClient() {
		if (client==null) {
			synchronized (ThriftClient.class) {
				if (client==null) {
					client = createNimbusClient(STORM_UI_NODE, STORM_UI_PORT);
				}
			}
		}
		return client;
	}
}	

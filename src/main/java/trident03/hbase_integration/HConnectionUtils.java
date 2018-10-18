package trident03.hbase_integration;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

/**
 * HConnectionUtils Hbase连接工具类
 * @author kinglyjn
 * @date 2018年8月6日
 *
 */
public class HConnectionUtils {
	private static Connection connection = null;
	private static final String HBASE_ZOOKEEPER_QUORUM = "bd117,bd118,bd119";
	private static final int HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = 2181;

	
	/**
	 * 获取全局Connection
	 * @throws IOException 
	 */
	public static Connection getConnection() throws IOException {
		if (connection != null) {
			return connection;
		}
		synchronized (HConnectionUtils.class) {
			if (connection==null) {
				synchronized (HConnectionUtils.class) {
					try {
						// 设置zk连接
						Configuration conf = HBaseConfiguration.create();
						conf.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM);
						conf.setInt("hbase.zookeeper.property.clientPort", HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT);
						// 创建hbase链接
						connection = ConnectionFactory.createConnection(conf);
					} catch (IOException e) {
						System.err.println("创建Connection实例时发生异常！");
					}
				}
			}
		}
		return connection;
	}
	
	/**
	 * 获取Admin
	 * @throws IOException 
	 */
	public static Admin getAdmin() throws IOException {
		return getConnection().getAdmin();
	}
	
	/**
	 * 获取Table
	 * @throws IOException 
	 */
	public static Table getTable(String tableName) throws IOException {
		return getConnection().getTable(TableName.valueOf(tableName));
	}
	
	/**
	 * 关闭Admin
	 * @throws IOException 
	 */
	public static void closeAdmin(Admin admin) throws IOException {
		if (admin != null) {
			admin.close();
		}
	}
	
	/**
	 * 关闭Table
	 * @throws IOException 
	 */
	public static void closeTable(Table table) throws IOException {
		if (table != null) {
			table.close();
		}
	}
}

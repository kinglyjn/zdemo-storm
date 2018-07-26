package utils;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * NC工具类
 * @author zhangqingli
 *
 */
public class NCUtil {
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
	private static final String LISTEN_HOST = "supervisor01z";
	private static final int LISTEN_PORT = 8888;
	private static final boolean OPEN = true;
	
	/**
	 * 将消息写入nc
	 * @param o
	 * @param msg
	 */
	public static void write2NC(Object o, String msg) {
		if (!OPEN) {
			return;
		}
		
		try {
			//系统时间
			Date now = new Date();
			String currrentTime = sdf.format(now);
			//进程号 
			RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
			String pidname = runtimeMXBean.getName(); //1234@xxx
			//线程名称-线程号
			String thread = "Thread-" + Thread.currentThread().getId();
			//类名称-hashcode
			String oname = o.getClass().getSimpleName() + ":" + o.hashCode();
			String prefix = "["+ currrentTime + "," + pidname + "," + thread + "," + oname +"] ";
			
			Process process = Runtime.getRuntime().exec("nc " + LISTEN_HOST + " " + LISTEN_PORT);
			OutputStream os = process.getOutputStream();
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os));
			br.write(prefix + msg + System.getProperty("line.separator"));
			
			br.flush();
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}

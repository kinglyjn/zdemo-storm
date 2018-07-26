package test06.transactional_topo01;

import java.io.Serializable;

/**
 * MyMetaData 事务元数据（必须实现序列化接口）
 * @author kinglyjn
 * @date 2018年7月26日
 *
 */
public class MyMetaData implements Serializable {
	private static final long serialVersionUID = 1L;
	/** 事务开始读取数据的位置 */
	private int beginPonit;	
	/** 每个事务处理的数据条数 */
	private int batchSize;	
	
	public MyMetaData(int beginPonit, int batchSize) {
		super();
		this.beginPonit = beginPonit;
		this.batchSize = batchSize;
	}
	public MyMetaData() {
		super();
	}
	public int getBeginPonit() {
		return beginPonit;
	}
	public void setBeginPonit(int beginPonit) {
		this.beginPonit = beginPonit;
	}
	public int getBatchSize() {
		return batchSize;
	}
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}
	
	@Override
	public String toString() {
		return "MyMetaData [beginPonit=" + beginPonit + ", batchSize=" + batchSize + "]";
	}
}

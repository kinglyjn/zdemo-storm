package test06.transactional_topo02_count_by_date;

import java.io.Serializable;

/**
 * OrderMetaData 订单批次的元数据
 * @author kinglyjn
 * @date 2018年7月27日
 *
 */
public class OrderMetaData implements Serializable {
	private static final long serialVersionUID = 1L;
	private int beginPonit;
	private int batchSize;

	public OrderMetaData(int beginPonit, int batchSize) {
		this.beginPonit = beginPonit;
		this.batchSize = batchSize;
	}
	public OrderMetaData() {
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
		return "OrderMetaData [beginPonit=" + beginPonit + ", batchSize=" + batchSize + "]";
	}
}

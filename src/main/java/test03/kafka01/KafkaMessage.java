package test03.kafka01;

import java.io.Serializable;
import java.util.Date;


/**
 * 实时计算-申请量&放款量数据
 * 		T：KafkaRequestLoanRecord
 * 		topicName：BDPRequestMonitor
 * 		subType：request_loan
 * ...
 * 
 */
public class KafkaMessage<T> implements Serializable {
	private static final long serialVersionUID = 1L;
	private String timestamp; 	 //发送时间，使用时间戳作为消息的key
	private String topic;         //该消息的topic
	private String subType;       //topic下的子分类，如果没有子分类，该值为空
	private Integer partition;	 //消息所在分区
	private T t;           		 //泛型，发送的具体消息内容，可能为json等
	
	public KafkaMessage() {}
	public KafkaMessage(String topic, String subType, T t) {
		this.topic = topic;
		this.subType = subType;
		this.t = t;
		this.timestamp = Long.toString(new Date().getTime());
	}
	
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	public String getSubType() {
		return subType;
	}
	public void setSubType(String subType) {
		this.subType = subType;
	}
	public Integer getPartition() {
		return partition;
	}
	public void setPartition(Integer partition) {
		this.partition = partition;
	}
	public T getT() {
		return t;
	}
	public void setT(T t) {
		this.t = t;
	}
}




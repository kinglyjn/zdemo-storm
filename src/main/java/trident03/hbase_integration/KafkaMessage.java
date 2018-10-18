package trident03.hbase_integration;

import java.io.Serializable;

/**
 * KafkaMessage<T>
 * 
 */
public class KafkaMessage<M> implements Serializable {
	private static final long serialVersionUID = 1L;
	private String topic;         	//该消息的topic
	private String subType;       	//topic下的子分类，如果没有子分类，该值为空
	private String key; 	 		//消息的key
	private M message;           	//消息内容(可能为json等)
	
	public KafkaMessage() {}
	public KafkaMessage(String topic, String subType, String key, M message) {
		this.topic = topic;
		this.subType = subType;
		this.key = key;
		this.message = message;
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
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public M getMessage() {
		return message;
	}
	public void setMessage(M message) {
		this.message = message;
	}
	
	@Override
	public String toString() {
		return "["+topic+":"+subType+"] key=" + key + ", message=" + message;
	}
}




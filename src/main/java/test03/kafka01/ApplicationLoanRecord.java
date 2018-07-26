package test03.kafka01;

import java.io.Serializable;

/**
 * 申请量或放款量记录
 *
 */
public class ApplicationLoanRecord implements Serializable {
	private static final long serialVersionUID = 1L;
	
	/*
	    1.1 申请推送
	  	{
		"type":"apply",——固定的，如果两个topic的话省略
		"applicationId":1,——申请表id
		"orderNo":1, ——申请单流水号
		"pipelineName":"IOS A",——风控pipeline的名称
		"levelId":"A"——风控等级
		"createTime":1504494784000,——申请时间
		"appId":1—app编号
		"businessId":1—业务线编号
		}

		
		1.2	放款推送
		{
		"type":"withdraw",——固定的，如果两个topic的话省略
		"loanId":1,——订单表id
		"applicationId":1,——申请表id
		"orderNo":1, ——申请单流水号
		"pipelineName":"IOS A",——风控pipeline的名称（订单里没有，需要推）
		"levelId":"A"——风控等级（订单里没有，需要推）
		"effectiveTime":1504494784000,——放款时间（订单里没有，需要推）
		"appId":1—app编号
		"businessId":1—业务线编号
		}

	 */
	private String type; //
	private Long applicationId;
	private Long orderNo;
	private Long loanId;
	private Long productId;
	private String pipelineName;
	private String levelId;  //风控等级
	private Long appId;
	private Long businessId;
	private Long createTime; //申请时间
	private Long effectiveTime; //放款时间
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public Long getApplicationId() {
		return applicationId;
	}
	public void setApplicationId(Long applicationId) {
		this.applicationId = applicationId;
	}
	public Long getOrderNo() {
		return orderNo;
	}
	public void setOrderNo(Long orderNo) {
		this.orderNo = orderNo;
	}
	public Long getLoanId() {
		return loanId;
	}
	public void setLoanId(Long loanId) {
		this.loanId = loanId;
	}
	public Long getProductId() {
		return productId;
	}
	public void setProductId(Long productId) {
		this.productId = productId;
	}
	public String getPipelineName() {
		return pipelineName;
	}
	public void setPipelineName(String pipelineName) {
		this.pipelineName = pipelineName;
	}
	public String getLevelId() {
		return levelId;
	}
	public void setLevelId(String levelId) {
		this.levelId = levelId;
	}
	public Long getAppId() {
		return appId;
	}
	public void setAppId(Long appId) {
		this.appId = appId;
	}
	public Long getBusinessId() {
		return businessId;
	}
	public void setBusinessId(Long businessId) {
		this.businessId = businessId;
	}
	public Long getCreateTime() {
		return createTime;
	}
	public void setCreateTime(Long createTime) {
		this.createTime = createTime;
	}
	public Long getEffectiveTime() {
		return effectiveTime;
	}
	public void setEffectiveTime(Long effectiveTime) {
		this.effectiveTime = effectiveTime;
	}
	
	@Override
	public String toString() {
		return "ApplicationLoanRecord [type=" + type + ", applicationId=" + applicationId + ", orderNo=" + orderNo
				+ ", loanId=" + loanId + ", productId=" + productId + ", pipelineName=" + pipelineName + ", levelId="
				+ levelId + ", appId=" + appId + ", businessId=" + businessId + ", createTime=" + createTime
				+ ", effectiveTime=" + effectiveTime + "]";
	}
}

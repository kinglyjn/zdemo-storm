package test03.kafka01;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import test00.config.ApplicationLoanConfig;

/**
 * ApplicationLoanRedisService
 * @author zhangqingli
 *
 */
public class ApplicationLoanRedisService {
	private static JedisPool jedisPool = BasicRedisUtils.getJedisPoolInstance();


	/**
	 * 获取今天计数的计数map集合
	 * 
	 * apply:20180109
	 *     pipeline:AndroidA:12  xxx
	 * loan:20180109
	 *     pipeline:AndroidA:12  xxx
	 *     riskrank:A:12         xxx
	 *     
	 *     |
	 *     |/
	 *     
	 * apply:20180109:pipeline:AndroidA:12  xxx
	 * loan:20180109:pipeline:AndroidA:12   xxx
	 * loan:20180109:riskrank:A:12          xxx
	 *  
	 */
	public Map<String, String> getTodayBaseCountMap() {
		Map<String, String> todayBaseCountMap = new HashMap<>();
		Jedis jedis = jedisPool.getResource();
		jedis.auth(BasicRedisUtils.getPasswd());
		
		Date currentDatetime = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String currentDay = sdf.format(currentDatetime);
		
		String applyHsetKey = ApplicationLoanConfig.TYPE_APPLY + ApplicationLoanConfig.DEFAULT_SEPARATOR + currentDay;
		Map<String, String> applyHsetFieldMap = jedis.hgetAll(applyHsetKey);
		String loanHsetKey = ApplicationLoanConfig.TYPE_LOAN + ApplicationLoanConfig.DEFAULT_SEPARATOR + currentDay;
		Map<String, String> loanHsetFieldMap = jedis.hgetAll(loanHsetKey);
		
		for (Entry<String, String> entry : applyHsetFieldMap.entrySet()) {
			todayBaseCountMap.put(applyHsetKey + ApplicationLoanConfig.DEFAULT_SEPARATOR + entry.getKey(), entry.getValue());
		}
		for (Entry<String, String> entry : loanHsetFieldMap.entrySet()) {
			todayBaseCountMap.put(loanHsetKey + ApplicationLoanConfig.DEFAULT_SEPARATOR + entry.getKey(), entry.getValue());
		}
		
		jedis.close();
		return todayBaseCountMap;
	}
	
	
	/**
	 * 存储 hset field
	 * 
	 */
	public Long hset(String key, String field, String value) {
		Jedis jedis = jedisPool.getResource();
		jedis.auth(BasicRedisUtils.getPasswd());
		Long hset = jedis.hset(key, field, value);
		jedis.close();
		return hset;
	}
	
	/**
	 * 设置过期时间
	 * 
	 */
	public Long expire(String key, Integer seconds) {
		Jedis jedis = jedisPool.getResource();
		jedis.auth(BasicRedisUtils.getPasswd());
		Long expire = jedis.expire(key, seconds);
		jedis.close();
		return expire;
	}
	
	
}

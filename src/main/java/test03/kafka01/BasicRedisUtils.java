package test03.kafka01;

import java.util.Map;
import java.util.Set;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Tuple;
import test00.config.ApplicationLoanConfig;


public class BasicRedisUtils {
	private static JedisPool jedisPool;
	private static String password;

	static {
		jedisPool = new JedisPool(ApplicationLoanConfig.REDIS_HOST, ApplicationLoanConfig.REDIS_PORT);
		password = ApplicationLoanConfig.REDIS_PASSWD;
	}
	
	public static JedisPool getJedisPoolInstance() {
		return jedisPool;
	}
	public static String getPasswd() {
		return password;
	}

	public static String set(String key, String value) {
		Jedis jedis = jedisPool.getResource();
		jedis.auth(password);
		String set = jedis.set(key, value);
		jedis.close();
		return set;
	}

	public static String get(String key) {
		Jedis jedis = jedisPool.getResource();
		jedis.auth(password);
		String get = jedis.get(key);
		jedis.close();
		return get;
	}

	public static Long hset(String key, String field, String value) {
		Jedis jedis = jedisPool.getResource();
		jedis.auth(password);
		Long hset = jedis.hset(key, field, value);
		jedis.close();
		return hset;
	}

	public static String hget(String key, String field) {
		Jedis jedis = jedisPool.getResource();
		jedis.auth(password);
		String hget = jedis.hget(key, field);
		jedis.close();
		return hget;
	}

	public static String hmset(String key, Map<String, String> fv) {
		Jedis jedis = jedisPool.getResource();
		jedis.auth(password);
		String hmset = jedis.hmset(key, fv);
		jedis.close();
		return hmset;
	}

	public static Map<String, String> hgetAll(String key) {
		Jedis jedis = jedisPool.getResource();
		jedis.auth(password);
		Map<String, String> hgetAll = jedis.hgetAll(key);
		jedis.close();
		return hgetAll;
	}

	public static Long expire(String key, Integer seconds) {
		Jedis jedis = jedisPool.getResource();
		jedis.auth(password);
		Long expire = jedis.expire(key, seconds);
		jedis.close();
		return expire;
	}

	public static Long del(String key) {
		Jedis jedis = jedisPool.getResource();
		jedis.auth(password);
		Long del = jedis.del(key);
		jedis.close();
		return del;
	}

	public static Long hdel(String key, String field) {
		Jedis jedis = jedisPool.getResource();
		jedis.auth(password);
		Long hdel = jedis.hdel(key, field);
		jedis.close();
		return hdel;
	}

	public static Long zadd(String key, double score, String member) {
		Jedis jedis = jedisPool.getResource();
		jedis.auth(password);
		Long zadd = jedis.zadd(key, score, member);
		jedis.close();
		return zadd;
	}

	public static Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
		Jedis jedis = jedisPool.getResource();
		jedis.auth(password);
		Set<Tuple> zre = jedis.zrevrangeByScoreWithScores(key, max, min, offset, count);
		jedis.close();
		return zre;
	}

	public static Set<Tuple> zrangeWithScores(String key, Long start, Long end) {
		Jedis jedis = jedisPool.getResource();
		jedis.auth(password);
		Set<Tuple> zran = jedis.zrangeWithScores(key, start, end);
		jedis.close();
		return zran;
	}

}

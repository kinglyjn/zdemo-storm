package test.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * 自定义序列化的 Tuple Field
 * @author zhangqingli
 *
 */
public class UserSerializer extends Serializer<User> {

	@Override
	public void write(Kryo kryo, Output output, User user) {
		
	}

	@Override
	public User read(Kryo kryo, Input input, Class<User> type) {
		return null;
	}

}

package test.kryo;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Date;

import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoTest {
	private static final String SER_DIR = System.getProperty("user.dir")+"/src/test/java/test/kryo/user.ser";
	
	@Test
	public void test01() throws Exception {
		Kryo kryo = new Kryo();
		Output output = new Output(new FileOutputStream(SER_DIR));
		
		User user = new User("zhangsan", "123456", new Date());
		kryo.writeObject(output, user);
		output.close();
	}
	
	
	@Test
	public void test02() throws FileNotFoundException {
		Kryo kryo = new Kryo();
		Input input = new Input(new FileInputStream(SER_DIR));
		
		User user = kryo.readObject(input, User.class);
		System.out.println(user);
		input.close();
	}
}

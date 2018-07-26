package test.kryo;

import java.io.Serializable;
import java.util.Date;

/**
 * 使用 kryo 实现序列化和反序列化
 * 需要 实现 Serializable 接口，并保证无参构造方法可用
 * @author zhangqingli
 *
 */
public class User implements Serializable {
	private static final long serialVersionUID = 1L;
	private String username;
	private String password;
	private Date birthday;
	
	public User() {
		super();
	}
	public User(String username, String password, Date birthday) {
		super();
		this.username = username;
		this.password = password;
		this.birthday = birthday;
	}
	
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public Date getBirthday() {
		return birthday;
	}
	public void setBirthday(Date birthday) {
		this.birthday = birthday;
	}
	
	@Override
	public String toString() {
		return "User [username=" + username + ", password=" + password + ", birthday=" + birthday + "]";
	}
}

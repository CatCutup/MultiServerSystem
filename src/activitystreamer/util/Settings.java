package activitystreamer.util;

import java.math.BigInteger;
import java.net.Socket;
import java.security.SecureRandom;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Settings {
	private static final Logger log = LogManager.getLogger();
	private static SecureRandom random = new SecureRandom();
	private static int localPort = 3780;
	private static String localHostname = "localhost";
	private static String remoteHostname = null;
	private static int remotePort = 3780;
	private static int activityInterval = 5000; // milliseconds
	private static String secret = "fmnmpp3ai91qb3gc2bvs14g3ue";
	private static String username = "anonymous";

	
	public static int getLocalPort() {
		return localPort;
	}

	public static void setLocalPort(int localPort) {
		if(localPort<0 || localPort>65535){
			log.error("supplied port "+localPort+" is out of range, using "+getLocalPort());
		} else {
			Settings.localPort = localPort;
		}
	}
	
	public static int getRemotePort() {
		return remotePort;
	}

	public static void setRemotePort(int remotePort) {
		if(remotePort<0 || remotePort>65535){
			log.error("supplied port "+remotePort+" is out of range, using "+getRemotePort());
		} else {
			Settings.remotePort = remotePort;
		}
	}
	
	public static String getRemoteHostname() {
		return remoteHostname;
	}

	public static void setRemoteHostname(String remoteHostname) {
		Settings.remoteHostname = remoteHostname;
	}
	
	public static int getActivityInterval() {
		return activityInterval;
	}

	public static void setActivityInterval(int activityInterval) {
		Settings.activityInterval = activityInterval;
	}
	
	public static String getSecret() {
		return secret;
	}

	public static void setSecret(String s) {
		secret = s;
	}
	
	public static String getUsername() {
		return username;
	}

	public static void setUsername(String username) {
		Settings.username = username;
	}
	
	public static String getLocalHostname() {
		return localHostname;
	}

	public static void setLocalHostname(String localHostname) {
		Settings.localHostname = localHostname;
	}

	
	/*
	 * some general helper functions
	 */
	
	public static String socketAddress(Socket socket){
		return socket.getInetAddress()+":"+socket.getPort();
	}

	public static String nextSecret() {
	    return new BigInteger(130, random).toString(32);
	 }



	public static String createID()
	{
		StringBuilder sb=new StringBuilder();
		Random rand=new Random();//随机用以下三个随机生成器
		Random randdata=new Random();
		int data=0;
		for(int i=0;i<15;i++)
		{
			int index=rand.nextInt(3);
			//目的是随机选择生成数字，大小写字母
			switch(index)
			{
				case 0:
					data=randdata.nextInt(10);//仅仅会生成0~9
					sb.append(data);
					break;
				case 1:
					data=randdata.nextInt(26)+65;//保证只会产生65~90之间的整数
					sb.append((char)data);
					break;
				case 2:
					data=randdata.nextInt(26)+97;//保证只会产生97~122之间的整数
					sb.append((char)data);
					break;
			}
		}
		String result=sb.toString();
		return result;


	}

	
}

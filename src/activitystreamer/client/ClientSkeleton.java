package activitystreamer.client;

import java.io.*;
import java.net.Socket;
import java.util.Scanner;

//server 之间组起来
//实现client registry

//client 多线程
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import activitystreamer.server.Control;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import activitystreamer.util.Settings;

import activitystreamer.server.Connection;
import activitystreamer.util.Settings;
import javax.sound.sampled.Port;

public class ClientSkeleton extends Thread {
	private static final Logger log = LogManager.getLogger();
	private static ClientSkeleton clientSolution;
	private TextFrame textFrame;

	private DataInputStream in;
	private DataOutputStream out;
	private BufferedReader inreader;
	private PrintWriter outwriter;

	private boolean term = false;


	Socket ClientSocket = null;
	private JSONObject Message;


	//这个方法是干嘛的
	public static ClientSkeleton getInstance() throws IOException {
		if (clientSolution == null) {
			clientSolution = new ClientSkeleton();
		}
		return clientSolution;
	}


	//client的主要步骤都在这里运行
	public ClientSkeleton() {


		try {
			//ClientSocket = new Socket(Settings.getRemoteHostname(), Settings.getRemotePort());
			ClientSocket = new Socket(Settings.getLocalHostname(), Settings.getLocalPort());
			//ClientSocket = new Socket(Settings.getLocalHostname(), 3781);
			in = new DataInputStream(ClientSocket.getInputStream());
			out = new DataOutputStream(ClientSocket.getOutputStream());
			inreader = new BufferedReader(new InputStreamReader(in));
			outwriter = new PrintWriter(out, true);
		} catch (IOException e) {
			e.printStackTrace();
		}

		//打开界面
		textFrame = new TextFrame();
		//对应哪一个线程
		start();

	}


	@SuppressWarnings("unchecked")
	//IO

	//发送JSON格式对象
	public void sendActivityObject(JSONObject activityObj) {
		// Marshelling 发送Json 对象给sever

		log.info("Activity message sent: " + activityObj);

		Message = activityObj;

		outwriter.println(Message.toJSONString());
		outwriter.flush();


	}


	public void disconnect() {

		try {

			term = true;
			out.close();
			inreader.close();
			in.close();
			ClientSocket.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	//写Process方法处理传回来的数据的问题
	public void ProcessReceivedMessage(JSONObject obj){





	}



	//
	@Override
	public void run() {

		log.debug("Client started");

			String data;

			//这里写的明显有问题


			while (!term) {

				System.out.println("This is message writing test1");


				try {
					System.out.println("This is message writing test 2");
					while ((data = inreader.readLine()) != null) {
						JSONObject obj;


						System.out.println("This is message writing test 4");
						obj = (JSONObject) (new JSONParser().parse(data));
						textFrame.setOutputText(obj);


						String state = (String) obj.get("command");

						if(state.equals("REGISTER_FAILED")) {
							log.info("Register failed");
							//System.exit(0);
						}
						else if(state.equals("LOGIN_FAILED")) {
							log.info("Login failed");
							login();
							break;
						}
						else if(state.equals("REDIRECT")) {
							log.info("Redirect");
							redirect(obj);
							break;
						}
							System.out.println("This is message writing test 5");
							//term = true;
							//disconnect();


					}
				} catch (IOException e) {
					e.printStackTrace();
				} catch (ParseException e) {
					e.printStackTrace();
				}


				System.out.println("This is message writing test 5");
			}



	}


	@SuppressWarnings("unchecked")
	public void redirect(JSONObject Obj){

		String NewHost = (String) Obj.get("hostname");
		int NewPort = (int) Obj.get("port");

		Settings.setRemoteHostname(NewHost);
		Settings.setRemotePort(NewPort);

		JSONObject redirect = new JSONObject();
		redirect.put("username", Settings.getUsername());
		redirect.put("secret", Settings.getSecret());

		outwriter.println(redirect.toJSONString());
		outwriter.flush();

	}

	@SuppressWarnings("unchecked")
	public void login() {
		JSONObject Login = new JSONObject();
		Login.put("username", Settings.getUsername());
		Login.put("secret", Settings.getSecret());

		outwriter.println(Login.toJSONString());
		outwriter.flush();

	}

}



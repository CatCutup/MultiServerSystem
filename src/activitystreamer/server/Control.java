package activitystreamer.server;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.LinkedList;
import java.util.Queue;

import activitystreamer.Server;
import java.nio.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import activitystreamer.util.Settings;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Control extends Thread {

	private static final Logger log = LogManager.getLogger();
	private static ArrayList<Connection> connections;
	private static boolean term=false;
	private static boolean selfconnection=false;
	//alone是用来判断server是不是做过认证或者是否接受过认证
	private static boolean alone =true;
	private static boolean status = false;
	private static int backupnum =0;
	private static final String ServerID=Settings.createID();
	private static Listener listener;
	private static Map<String, String> UserMap = new ConcurrentHashMap<>();
	private static Map<String, JSONObject> ServerMap = new ConcurrentHashMap<>();
	private static Map<String, JSONObject> BackupMap = new ConcurrentHashMap<>();
	private static Queue<JSONObject> MessageQueue = new LinkedList<JSONObject>();

	/*

	1.给Server添加buffer确保短线之后的小子传输
	2.广播Server的信息设置断线备份  //这两个方法先不测试
	3.Server进行重连

-lp 3782 -lh localhost -rp 3780 -rh localhost -s fmnmpp3ai91qb3gc2bvs14g3ue
-lp 3784 -lh localhost -rp 3782 -rh localhost -s fmnmpp3ai91qb3gc2bvs14g3ue


	Server重连广播的第一条信息应该是认证
    */

	protected static Control control = null;
	
	public static Control getInstance() {
		if(control==null){
			control=new Control();
		} 
		return control;
	}
	
	public Control() {
		// initialize the connections array
		connections = new ArrayList<Connection>();
		// start a listener
		try {
			listener = new Listener();


			//调用
			this.initiateConnection();

			start();

		} catch (IOException e1) {
			log.fatal("failed to startup a listening thread: "+e1);
			System.exit(-1);
		}	
	}

	
	public void initiateConnection(){
		// make a connection to another server if remote hostname is supplied
		if(Settings.getRemoteHostname()!=null){

			try {
				outgoingConnection(new Socket(Settings.getRemoteHostname(),Settings.getRemotePort()));
			} catch (IOException e) {
				log.error("failed to make connection to "+Settings.getRemoteHostname()+":"+Settings.getRemotePort()+" :"+e);
				System.exit(-1);
			}
		}
	}

	
	/*
	 * The connection has been closed by the other party.
	 */
	public synchronized void connectionClosed(Connection con){
		if(!term) connections.remove(con);
	}
	
	/*
	 * A new incoming connection has been established, and a reference is returned to it
	 */
	public synchronized Connection incomingConnection(Socket s) throws IOException{


		log.debug("incomming connection: "+Settings.socketAddress(s));
		Connection c = new Connection(s);
		connections.add(c);
		return c;
		
	}
	
	/*
	 * A new outgoing connection has been established, and a reference is returned to it
	 */
	public synchronized Connection outgoingConnection(Socket s) throws IOException{
		log.debug("outgoing connection: "+Settings.socketAddress(s));

		JSONObject AuthenticateMsg = new JSONObject();
		AuthenticateMsg.put("command", "AUTHENTICATE");
		AuthenticateMsg.put("ServerID", ServerID);
		AuthenticateMsg.put("secret", Settings.getSecret());
		alone = false;

		Connection c = new Connection(s);

		c.writeMsg(AuthenticateMsg.toJSONString());

		c.setAuthenticate(true);

		System.out.println("Outgoing connection has been established.");

		connections.add(c);

		return c;
		
	}



	public synchronized boolean process(Connection con,String msg){


		log.debug("Server Receieved: " + msg);

		JSONObject ReceivedMessage = null;
		//System.out.println("Message Test 1");


		try {
			ReceivedMessage = (JSONObject) (new JSONParser().parse(msg));
			//System.out.println("Message Test 2");
		} catch (ParseException e) {
			e.printStackTrace();
		}
		//ReceivedMessage = new JsonObject(msg);



		//这里写的有问题
		//System.out.println("Message Test 3");


		String MessageType = (String) ReceivedMessage.get("command");

		//System.out.println("Message Test 4");

		System.out.println(MessageType);


		//这里这个方法用来处理command的各种异常

		//处理No_Command_Error


		//怎么样判断MessageType是否为空
		//写明各种函数来对传入的json格式数据进行处理
		if(MessageType.equals(null)){

			return InvalidMessage(con,"No Command Error");
			//con.writeMsg(ReceivedMessage.toJSONString());
		}
		//处理其他类型的异常
		else if(MessageType.equals("LOGOUT")){

			con.closeCon();
			return true;
		}

		//用户认证
		else if(MessageType.equals("AUTHENTICATE")){


			return Authenticate(con, ReceivedMessage);

		}

		else if(MessageType.equals("LOGIN")){

			//ClientConnectionList.add(con);
			//con.setConnectionStatus("Client");
			//login这里的secret有问题
			return Login(con, ReceivedMessage);

		}

		else if(MessageType.equals("REGISTER")){



			return Register(con, ReceivedMessage);

		}

		else if(MessageType.equals("ACTIVITY_MESSAGE")){

			//Store Message in the queue
			MessageQueue.offer(ReceivedMessage);

				return ActivityMessage(con,ReceivedMessage);





		}
		else if(MessageType.equals("ACTIVITY_BROADCAST")){

			return ActivityBroadcast(con,ReceivedMessage);
		}
		else if(MessageType.equals("SERVER_ANNOUNCE")){

			return HandleServerAnnounce(con,ReceivedMessage);

		}
		else if(MessageType.equals("UserInfo_ANNOUNCE")){

			return HandleUserAnnounce(con,ReceivedMessage);
		}
		else if(MessageType.equals("Server_Backup")){

			return HandleBackupAnnounce(con,ReceivedMessage);
		}
		//lock部分的异常处理没有写
		//先去掉 lockrequest 的部分
		/*
		else if(MessageType.equals("LOCK_REQUEST")){

			return LockRequest(con, ReceivedMessage);
		}
		else if(MessageType.equals("LOCK_DENIED")){

			return LockDenied(con, ReceivedMessage);
		}
		else if(MessageType.equals("LOCK_ALLOWED")){

			return LockAllowed(con, ReceivedMessage);
		}
		*/

		return true;
	}

	private boolean HandleBackupAnnounce(Connection con, JSONObject obj) {


		String id =(String) obj.get("id");

		if(id!=ServerID){


			BackupMap.put(id,obj);

			System.out.println("****** Server information has already been backup. ******");

			return false;
		}



		return false;
	}

	private boolean HandleUserAnnounce(Connection con, JSONObject obj) {


		String username = (String) obj.get("username");

		int count = UserMap.size();
		int i=0;


		//这里逻辑有问题
		if(count !=0){
			for (String name : UserMap.keySet()) {

				if(name.equals("username"))
				{
					System.out.println("User name already exist");
					return false;
				}
				else{
					i++;
				}
			}

			if(i == count){
				String secret = (String) obj.get("secret");

				UserMap.put(username, secret);

				System.out.println("Already get new user name.");

				return false;
			}


		}

		else{
			String secret = (String) obj.get("secret");

			UserMap.put(username, secret);

			System.out.println("Already get new user name.");

			return false;
		}




		return false;

	}


	private boolean HandleServerAnnounce(Connection con, JSONObject obj) {

		//这里没有判断

		JSONObject ServerAnnounceMsg = new JSONObject();
		con.setConnectionStatus("Server");

		String id = (String) obj.get("id");
		int serversize=0;
		int flag =0;

			if(con.isAuthenticate()){
				if(obj.get("id").equals(null) || obj.get("load").equals(null)|| obj.get("hostname").equals(null)|| obj.get("port").equals(null))
				{
					InvalidMessage(con,"Message Style Wrong");
					return true;
				}
				else{


					serversize= ServerMap.size();
					for (String serverid : ServerMap.keySet()) {

						if(serverid == id){

							System.out.println("server information has been stored.");
						}
						else{
							flag=flag+1;
						}

					}

					if(flag==serversize){
						long load =(long) obj.get("load");
						String hostname = (String) obj.get("hostname");
						String port = (String) obj.get("port");

						ServerAnnounceMsg.put("load",load);
						ServerAnnounceMsg.put("hostname",hostname);
						ServerAnnounceMsg.put("port",port);
						ServerMap.put(id,ServerAnnounceMsg);

						//alone = false;


						System.out.println("Already get server announce");

						return false;

					}

				}

			}
			else{
				//Announce from unAnnounce Server
				InvalidMessage(con,"None Authenticate Server");
				return true;
			}

		return false;
	}

	private boolean ActivityBroadcast(Connection con, JSONObject object) {
		//Between Server and each server to its client



		if(object.get("activity") == null){

			InvalidMessage(con,"None Correct ActivityBroadcast");
			con.closeCon();
			return true;
		}
		else{

			JSONObject ReceivedMsg;

			ReceivedMsg = (JSONObject) object.get("activity");
			JSONObject finalMsgObj = new JSONObject();

			finalMsgObj.put("activity", ReceivedMsg);
			finalMsgObj.put("command", "ACTIVITY_BROADCAST");

			if(con.getConnectionStatus().equals("Server")){

				if (!con.isAuthenticate()){
					InvalidMessage(con,"None Authenticate Server");
					con.closeCon();
					return true;
				}

				else{

					BroadCastToOtherServer(finalMsgObj);
					BroadCastToAllClient(finalMsgObj);
					return false;
				}

			}
			else{

				InvalidMessage(con,"Activity Broadcast can only from Server");

			}


		}

		return true;
	}

	//在广播的时候 被广播的json没有被处理 sercret 等信息应该被隐藏
	//就是这里的判断绝壁有问题
	private boolean ActivityMessage(Connection con, JSONObject object) {


		JSONObject obj = new JSONObject();

		obj=MessageQueue.poll();

		JSONObject ActivityMsg = new JSONObject();
		JSONObject ActivityStatusMsg = new JSONObject();

		String username = (String) object.get("username");
		String secret = (String) object.get("secret");

		//这里有空指针异常
		//为什么这里有问题呢
		if(username.equals("anonymous")){

			ActivityMsg = ProcessMsg(object);
			BroadCastToAllClient(ActivityMsg);
			BroadCastToOtherServer(ActivityMsg);

			return false;

		}
		else {

			for(String name : UserMap.keySet()){

				while(username.equals(name)){
					if(UserMap.get(name).equals(secret)){

						ActivityMsg = ProcessMsg(object);

						BroadCastToAllClient(ActivityMsg);
						BroadCastToOtherServer(ActivityMsg);
						return false;

					}
					else{
						ActivityStatusMsg.put("command","AUTHENTICATION_FAIL");
						ActivityStatusMsg.put("info","None correct secret");
						con.writeMsg(ActivityStatusMsg.toJSONString());
						return true;
					}
				}



			}
			ActivityStatusMsg.put("command","AUTHENTICATION_FAIL");
			ActivityStatusMsg.put("info","We cannot find your username, please login first");
			con.writeMsg(ActivityStatusMsg.toJSONString());
			return true;

		}

	}


    //这是处理一下发送过来的 Activity Message
	private JSONObject ProcessMsg(JSONObject object) {

		JSONObject processedMsg ;
		JSONObject Message = new JSONObject();

		String username = (String) object.get("username");

		//这里写的还是有问题
		processedMsg = (JSONObject) object.get("activity");

		Message.put("authenticated_user", username);
		Message.put("command", "ACTIVITY_MESSAGE");
		Message.put("activity", processedMsg);

		return Message;
	}




	// These three methods are using for broadcasting message

	//this method is using for broadcast to client
	private void BroadCastToAllClient(JSONObject object) {



		for(Connection connection: connections){

			if(connection.getConnectionStatus().equals("Client")){

				connection.writeMsg(object.toJSONString());
			}
		}


	}

	//this method is using to broadcast to all servers
	private void BroadCastToAllServer(JSONObject object) {
		for(Connection connection: connections){

			if(connection.getConnectionStatus().equals("Server")){

				connection.writeMsg(object.toJSONString());
			}
		}


	}

	//this method is using to broadcast to servers except the request server
	private void BroadCastToOtherServer(JSONObject obj) {
		//怎么识别这个唯一的server
		for(Connection connection: connections){
			while(connection.getConnectionStatus().equals("Server")){
				if(connection.getServerID().equals(ServerID)){
					System.out.println("Broadcast to all Server except this one");
				}
				else{
					connection.writeMsg(obj.toJSONString());
				}
			}
		}
	}
	//用户进行注册
	private boolean Register(Connection con, JSONObject obj) {


			JSONObject RegisterMsg = new JSONObject();

			String secret = (String) obj.get("secret");
			String username = (String) obj.get("username");

			if (secret == null) {

				RegisterMsg.put("command", "REGISTER_FAILED");
				RegisterMsg.put("info", "None secret");
				con.writeMsg(RegisterMsg.toJSONString());
				con.closeCon();
				return true;
			}
			for (String name : UserMap.keySet()) {
				if (name.equals(username)) {


					RegisterMsg.put("command", "REGISTER_FAILED");
					RegisterMsg.put("info", username + " is already registered with the system");


					con.writeMsg(RegisterMsg.toJSONString());
					//发送完成Register信息 close连接

					con.closeCon();
					return true;
				}
			}


		RegisterMsg.put("command", "REGISTER_SUCCESS");
		RegisterMsg.put("info", "register success for " + username);

		UserMap.put(username, secret);

		//注册成功之后标记为Client
		con.setConnectionStatus("Client");
		con.writeMsg(RegisterMsg.toJSONString());
		return false;





	}



	private boolean Login(Connection con, JSONObject obj) {


			String username = (String) obj.get("username");

			String secret = (String) obj.get("secret");

			JSONObject LoginMsg = new JSONObject();

			if(username.equals("anonymous")){

				LoginMsg.put("command", "LOGIN_SUCCESS");
				LoginMsg.put("info", "Logged in as user " + username);

				return false;

			}
			else if(username == null){

				LoginMsg.put("command", "LOGIN_FAILED");
				LoginMsg.put("info", "attempt to login with null username");
				con.writeMsg(LoginMsg.toJSONString());
				return true;
			}
			else{

				for(String name : UserMap.keySet()){
					while(UserMap.get(name)!=null && name.equals(username) && UserMap.get(name).equals(secret)){

						LoginMsg.put("command","LOGIN_SUCCESS");
						LoginMsg.put("info","Logged in as user "+ username);
						con.writeMsg(LoginMsg.toJSONString());
						return false;
					}
				}
			}


			LoginMsg.put("command","LOGIN_FAILED");
			LoginMsg.put("info","Loggin failed with unknown error");


		return true;
	}



	//这里是处理认证的方法 但是有个问题我们现在没法判断一个Server是不是做过了认证
	private boolean Authenticate(Connection con, JSONObject obj) {

		JSONObject ErrorMsg = new JSONObject();

		String Secret = (String) obj.get("secret");

		// 这里写的就有问题

		if(Secret.equals(Settings.getSecret()))
		{
			System.out.println("Authenticate Successfully");

			//给每一个连接设置独立的标识符

			//con.setServerID(Settings.createID());
			con.setAuthenticate(true);

			con.setConnectionStatus("Server");

			alone = false;

			return false;

		}
		else{
			ErrorMsg.put("command","AUTHENTICATION_FAIL");
			ErrorMsg.put("info", "the supplied secret is incorrect: " +Secret);
			con.writeMsg(ErrorMsg.toJSONString());
			con.setAuthenticate(false);
			//connectionClosed(con);
			return true;
		}
	}

	//I don't know why I write this method. Maybe because my girlfriend don't want to watch Avengers with me.
	//Oh programmer don't need a girlfriend because we already have "object".
	//By the way I love Ironman and he is not died in Avengers : Infinity War

	private boolean AuthenticateFail(Connection con,JSONObject obj){

		JSONObject invalidMsg = new JSONObject();
		invalidMsg.put("command","AUTHENTICATION_FAIL");
		invalidMsg.put("info", "the supplied secret is incorrect: ");
		return true;
	}

	//处理异常情况
	//这个函数其实还可以改一下 让它直接输出mistake就行
	private boolean InvalidMessage(Connection con, String Mistake)
	{

		JSONObject invalidMsg = new JSONObject();

        invalidMsg.put("command","INVALID_MESSAGE");

		if (Mistake.equals("No Command Error")) {
			invalidMsg.put("info", "the received message did not contain a command");
		} else if (Mistake.equals("wrongCmdError")) {
			invalidMsg.put("info", "cannot find a matching command");
		}
		else if (Mistake.equals("None Authenticate Message")){
			invalidMsg.put("info","The message is received from an unauthenticated server");
		}
		else if(Mistake.equals("None Correct ActivityBroadcast")){
			invalidMsg.put("info",Mistake);
		}
		else if(Mistake.equals("Activity Broadcast can only from Server")){
			invalidMsg.put("info",Mistake);
		}
		else if(Mistake.equals("Server Announce can only send by Server")){
			invalidMsg.put("info",Mistake);
		}
		else if(Mistake.equals("None Authenticate Server")){
			invalidMsg.put("info",Mistake);
		}
		else if(Mistake.equals("Message Style Wrong")){
			invalidMsg.put("info",Mistake);
		}




		con.writeMsg(invalidMsg.toJSONString());

		System.out.println(Mistake);


		return true;
	}

	public void ServerAnnounce(int loadnumber){


		JSONObject Announce = new JSONObject();

		String hostname = Settings.getLocalHostname();
		String port = String.valueOf(Settings.getLocalPort());
		Announce.put("command", "SERVER_ANNOUNCE");
		Announce.put("id", ServerID);
		Announce.put("load",loadnumber);
		Announce.put("hostname", hostname);
		Announce.put("port", port);

		BroadCastToAllServer(Announce);
		System.out.println("Server Announce has been already broadcast");

	}

	public void UserAnnounce(){

		for (Map.Entry<String, String> entry : UserMap.entrySet()) {
			//System.out.println("key= " + entry.getKey() + " and value= " + entry.getValue());
			String username = entry.getKey();
			String secret = entry.getValue();
			JSONObject UserInfo = new JSONObject();
			UserInfo.put("command", "UserInfo_ANNOUNCE");

			UserInfo.put("username",username);
			UserInfo.put("secret",secret);
			BroadCastToAllServer(UserInfo);
			System.out.println("User information broadcasting");
		}

	}



	public void BackupServer(){
		//广播给相领的Server 备份Server的信息

		for (Map.Entry<String, JSONObject> entry : ServerMap.entrySet()) {
			//System.out.println("key= " + entry.getKey() + " and value= " + entry.getValue());
			String serverid = entry.getKey();
			JSONObject msg = entry.getValue();
			JSONObject ServerMsg = new JSONObject();

			String hostname = (String) msg.get("hostname");
			String port = (String) msg.get("port");

			ServerMsg.put("hostname",hostname);
			ServerMsg.put("port",port);
			ServerMsg.put("id",serverid);
			ServerMsg.put("command", "Server_Backup");


			BroadCastToAllServer(ServerMsg);
			System.out.println("备份服务器信息广播测试");
		}

	}

	// number of client
	private int ClientNumber() {
		int ClientNumber = 0;

		//Useless math index, just for test
		int OtherNumber= 0;

		for (Connection c: this.getConnections()) {
			if (!c.getConnectionStatus().equals("Client")) {
				OtherNumber++;
			}
			else {
				ClientNumber++;
			}
		}
		return ClientNumber;
	}
	private int ServerNumber() {
		int ServerNumber = 0;

		//Useless math index, just for test
		int OtherNumber= 0;

		for (Connection c: this.getConnections()) {
			if (!c.getConnectionStatus().equals("Server")) {
				OtherNumber++;
			}
			else {
				ServerNumber++;
			}
		}
		return ServerNumber;
	}

	//这个是来进行redirect的方法

	private boolean Redirect(Connection con, JSONObject obj) {
		int clientNum = ClientNumber();

		//Check load of all connected servers

		//System.out.println("通过Map.entrySet遍历key和value");
		for (Map.Entry<String, JSONObject> entry : ServerMap.entrySet()) {

				JSONObject Value = entry.getValue();

				long load = (long) Value.get("load");
				String hostname =(String) Value.get("hostname");
			    String port =(String) Value.get("port");

				if (clientNum - 2 > load){
					JSONObject redirectMsg = new JSONObject();
					redirectMsg.put("command", "REDIRECT");
					redirectMsg.put("hostname", hostname);
					redirectMsg.put("port", port);

					if (con.writeMsg(obj.toJSONString())){

						con.writeMsg(redirectMsg.toJSONString());
						return false;
					}

				}

		}

		return true;
	}

	private boolean testconnection(int servernumber) {

		int serverstamp =0;

		if(servernumber>0){

			for(Connection con: connections){

				if(con.getConnectionStatus().equals("Server")){

					serverstamp = serverstamp+1;


				}
			}

			if(servernumber!= serverstamp){

				return true;
			}
			else {

				return false;
			}

		}


			return false;




	}


	private void handleselfclose(boolean selfconnection, Connection con, String msg) {

		CharBuffer buff = CharBuffer.allocate(10);
		buff.put(msg);

		JSONObject warning = new JSONObject();


		System.out.println("Message: "+ msg+"already stored in the buffer.");
	}



	@Override
	public void run(){

		log.info("using activity interval of "+Settings.getActivityInterval()+" milliseconds");

		while(!term){
			// do something with 5 second intervals in between
			int loadnumber = ClientNumber();
			int servernumber = ServerMap.size();

			if(servernumber>0){

				selfconnection = testconnection(servernumber);

			}



			if(selfconnection){

				System.out.println("****** Congratulations, Server is already disconnected ******.");
				selfconnection = false;

				handleservercrash();
				ServerMap.clear();
				BackupMap.clear();
			}





			this.ServerAnnounce(loadnumber);
			this.UserAnnounce();

			if(servernumber>1){
				this.BackupServer();
			}

			try {
				Thread.sleep(Settings.getActivityInterval());
			} catch (InterruptedException e) {
				log.info("received an interrupt, system is shutting down");
				break;
			}
			if(!term){
				log.debug("doing activity");
				term=doActivity();
			}
			
		}
		log.info("closing "+connections.size()+" connections");
		// clean up
		for(Connection connection : connections){
			connection.closeCon();
		}
		listener.setTerm(true);
	}

	private void handleservercrash() {

		for (Map.Entry<String, JSONObject> entry : BackupMap.entrySet()) {

			String serverid = entry.getKey();
			JSONObject msg = entry.getValue();



			while(serverid!=ServerID){

				int i = 0;
				i = serverid.compareTo(ServerID);
				if(i>0){

					String hostname = (String) msg.get("hostname");
					int port = Integer.parseInt((String) msg.get("port"));
					Settings.setRemoteHostname(hostname);
					Settings.setRemotePort(port);
					try {
						System.out.println("###### I'm watching you ######.");
						sleep(1000);

					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						outgoingConnection(new Socket(Settings.getRemoteHostname(), Settings.getRemotePort()));
					} catch (IOException e) {
						e.printStackTrace();
					}


					selfconnection = false;
					break;
				}
				else if(i<=0){
					System.out.println("###### Waiting for other server connection ######");
					selfconnection = false;
					break;

				}

			}



		}
	}


	public boolean doActivity(){
		return false;
	}
	
	public final void setTerm(boolean t){
		term=t;
	}
	
	public final ArrayList<Connection> getConnections() {
		return connections;
	}



}

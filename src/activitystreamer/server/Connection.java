package activitystreamer.server;


import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import activitystreamer.util.Settings;
import org.json.simple.JSONObject;


public class Connection extends Thread {
	private static final Logger log = LogManager.getLogger();
	private static String ConnectionStatus = "None";
	private static  String ID ;
	private DataInputStream in;
	private DataOutputStream out;
	private BufferedReader inreader;
	private PrintWriter outwriter;
	private boolean open = false;
	private Socket socket;
	private boolean term=false;
	private boolean AuthenticateStatus=false;


	//连接客户端socket
	Connection(Socket socket) throws IOException{


		in = new DataInputStream(socket.getInputStream());
	    out = new DataOutputStream(socket.getOutputStream());
	    inreader = new BufferedReader( new InputStreamReader(in));
	    outwriter = new PrintWriter(out, true);
	    this.socket = socket;
	    open = true;
	    start();
	}
	
	/*
	 * returns true if the message was written, otherwise false
	 */
	//写入客户端信息
	public boolean writeMsg(String msg) {

		System.out.println("This is message writing test");
		if(open){
			outwriter.println(msg);
			outwriter.flush();
			return true;	
		}
		return false;
	}


	public void closeCon(){
		if(open){
			log.info("closing connection "+Settings.socketAddress(socket));
			try {
				term=true;
				inreader.close();
				out.close();
			} catch (IOException e) {
				// already closed?
				log.error("received exception closing the connection "+Settings.socketAddress(socket)+": "+e);
			}
		}
	}
	protected void closeStream()
	{
		// Close streams and readers
		try
		{
			inreader.close();
			in.close();

			outwriter.close();
			out.close();
		}
		catch (IOException e)
		{
			// already closed?
			log.error("received exception closing the connection " + Settings.socketAddress(socket) + ": " + e);
		}

		open = false;
	}

	public void run(){
		try {
			String data;
			//这个term就是负责管理close的

			/*
			while(!term){
				System.out.println("I'm still running");
				while((data = inreader.readLine())!=null){
					term=Control.getInstance().process(this,data);
					System.out.println(data);
			}
			}
			*/
			while (!term&&(data = inreader.readLine())!=null){
				term=Control.getInstance().process(this,data);
				System.out.println(data);
			}
			log.debug("connection closed to "+Settings.socketAddress(socket));
			Control.getInstance().connectionClosed(this);
			in.close();
		} catch (IOException e) {
			log.error("connection "+Settings.socketAddress(socket)+" closed with exception: "+e);
			Control.getInstance().connectionClosed(this);
			closeStream();
		}
		open=false;
	}
	
	public Socket getSocket() {
		return socket;
	}
	
	public boolean isOpen() {
		return open;
	}

	public String getConnectionStatus(){
           return ConnectionStatus;
	}

	public void setConnectionStatus(String Status){
		Connection.ConnectionStatus = Status;
	}

	public boolean setAuthenticate(boolean status){

		AuthenticateStatus = status;
		return AuthenticateStatus;
	}
	public boolean isAuthenticate(){

		return AuthenticateStatus;
	}

	public void setServerID(String ID){

		Connection.ID = ID;

	}

	public String getServerID(){

		return ID;
	}


}



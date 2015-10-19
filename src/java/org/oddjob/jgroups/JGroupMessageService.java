package org.oddjob.jgroups;

import java.io.InputStream;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.oddjob.FailedToStopException;
import org.oddjob.framework.Service;

public class JGroupMessageService implements Service {

	private static final Logger logger = Logger.getLogger(JGroupMessageService.class);
	
	private volatile String name;
	
	private volatile String clusterName;
	
	private volatile boolean discardOwnMessages;
	
	private volatile JChannel channel;

	private volatile InputStream config;

	private volatile Collection<Object> receive;
	
	@Override
	public void start() throws Exception {
		String clusterName = this.clusterName;
		if (clusterName == null) {
			clusterName = "OddjobCluster";
		}
		
		if (config == null) {
		    channel = new JChannel();
		}
		else {
			new JChannel();
		    channel = new JChannel(config);
		}
		
	    channel.setDiscardOwnMessages(discardOwnMessages);
	    channel.connect(clusterName);
	    
	    logger.info("Connected to cluster: " + clusterName);
	    
	    channel.setReceiver(new ReceiverAdapter() {
	    	
	    	public void viewAccepted(View newView) {
	    		
	    	    logger.info("** view: " + newView);
	    	}

	    	public void receive(Message msg) {
	    		Object payload = msg.getObject();
	    		
	    	    logger.info("Received from " + msg.getSrc() + ": " + 
	    	    		payload);
	    	    
	    	    if (receive != null) {
	    	    	receive.add(payload);
	    	    }
	    	}
	    });
	}
	
	@Override
	public void stop() throws FailedToStopException {
	    channel.close();
	    channel = null;
	}
	
	public void setSend(Object message) throws Exception {
		logger.info("Sending to cluster: " + message);
		channel.send(null, message);
	}

	public List<Address> getMembers() {
		JChannel channel = this.channel;
		if (channel == null) {
			return null;
		}
		
		View view = channel.getView();
		if (view == null) {
			return null;
		}
		
		return view.getMembers();
	}
	
	public Address getAddress() {
		JChannel channel = this.channel;
		if (channel == null) {
			return null;
		}
		
		return channel.getAddress();
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}
	
	public String getClusterName() {
		return clusterName;
	}
	
	public void setReceive(Collection<Object> receive) {
		this.receive = receive;
	}
	
	public Collection<Object> getReceive() {
		return receive;
	}
	
	public void setConfig(InputStream config) {
		this.config = config;
	}
	
	public InputStream getConfigPath() {
		return config;
	}
	
	public void setDiscardOwnMessages(boolean discardOwnMessages) {
		this.discardOwnMessages = discardOwnMessages;
	}
	
	public boolean isDiscardOwnMessages() {
		return discardOwnMessages;
	}

	@Override
	public String toString() {
		if (name == null) {
			return getClass().getSimpleName();
		}
		else {
			return name;
		}		
	}
}

package org.oddjob.jgroups;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.Test;

public class JGroupMessageServiceTest {

	private static final Logger logger = Logger.getLogger(JGroupMessageServiceTest.class);
	static final long TIMEOUT = 5000;
	
	@Test
	public void testSendingMessageInSimpleCluster() throws Exception {
		
		BlockingQueue<Object> messages1 = new LinkedBlockingQueue<>();
		BlockingQueue<Object> messages2 = new LinkedBlockingQueue<>();
		
		final JGroupMessageService test1 = new JGroupMessageService();
		test1.setClusterName("OurTest");
		test1.setReceive(messages1);
		test1.setConfig(getClass().getResourceAsStream("tcp.xml"));
		
		final JGroupMessageService test2 = new JGroupMessageService();
		test2.setClusterName("OurTest");
		test2.setReceive(messages2);
		test2.setConfig(getClass().getResourceAsStream("tcp.xml"));
		
		test1.start();
		test2.start();
		
		logger.info(test1.getMembers());
		logger.info(test2.getMembers());
		
		test1.setSend("Apples");
		test1.setSend("Pears");
		test2.setSend("Oranges");
		
		assertEquals("Apples", messages1.poll(TIMEOUT, TimeUnit.MILLISECONDS));
		assertEquals("Pears", messages1.poll(TIMEOUT, TimeUnit.MILLISECONDS));
		assertEquals("Oranges", messages1.poll(TIMEOUT, TimeUnit.MILLISECONDS));
		
		Set<String> expected = new HashSet<>(Arrays.asList("Oranges", "Apples", "Pears"));
		
		assertEquals(true, expected.remove(messages2.poll(TIMEOUT, TimeUnit.MILLISECONDS)));
		assertEquals(true, expected.remove(messages2.poll(TIMEOUT, TimeUnit.MILLISECONDS)));
		assertEquals(true, expected.remove(messages2.poll(TIMEOUT, TimeUnit.MILLISECONDS)));
		
		test1.stop();
		test2.stop();
		
		assertEquals(null, test1.getAddress());
		assertEquals(null, test2.getAddress());
	}
}

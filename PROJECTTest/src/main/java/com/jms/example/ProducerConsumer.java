package com.jms.example;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

//import com.jms.example.Consumer.HelloWorldConsumer;
//import com.jms.example.Producer.HelloWorldProducer;

import java.sql.PreparedStatement;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

public class ProducerConsumer {
	public static void main(String[] args) throws Exception {
        thread(new HelloWorldProducer(), false);
        thread(new HelloWorldConsumer(), false);
    }
    public static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }
    
    public static class HelloWorldProducer implements Runnable {
        public void run() {
            try {
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
 
                Connection connection = connectionFactory.createConnection();
                connection.start();
 
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
 
                Destination destination = session.createQueue("TEST");
 
                MessageProducer producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
 
                String text ="DONE , HERE";
                TextMessage message = session.createTextMessage(text);
 
                System.out.println(text );
                System.out.println("Message Send by Producer");
                producer.send(message);
 
                session.close();
                connection.close();
            }
            catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            }
        }
    }
    
    public static class HelloWorldConsumer implements Runnable, ExceptionListener {
        public void run() {
            try {
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
                
                Connection connection = connectionFactory.createConnection();
                connection.start();
 
                connection.setExceptionListener(this);
 
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
 
                Destination destination = session.createQueue("TEST");
 
                MessageConsumer consumer = session.createConsumer(destination);
 
                Message message = consumer.receive(1000);
 
                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    String text = textMessage.getText();
                    System.out.println(text);
                    System.out.println("Message Received by consumer");
                    ApplicationContext context = new ClassPathXmlApplicationContext("SpringJdbc.xml");
            		DriverManagerDataSource dataSource = (DriverManagerDataSource) context.getBean("dataSource");
            		java.sql.Connection connection1 = dataSource.getConnection();
            		PreparedStatement pstat = connection1.prepareStatement("INSERT INTO info VALUES(?)");
            		pstat.setString(1, text);
            		pstat.executeUpdate();
                } else {
                    System.out.println("Received: " + message);
                }
                
                consumer.close();
                session.close();
                connection.close();
                
            } catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            }
        }
		
	
        public synchronized void onException(JMSException ex) {
            System.out.println("JMS Exception occured.  Shutting down client.");
        }
    }

}

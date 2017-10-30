package com.papajohns.pjwebapp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.cloud.cloudfoundry.com.fasterxml.jackson.databind.JsonNode;
import org.springframework.cloud.cloudfoundry.com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.json.JSONArray;
import org.json.JSONObject;
import java.io.*;
import java.lang.reflect.Field;
import org.springframework.util.FileCopyUtils;
import java.util.*;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import com.papajohns.domain.Order;

@RestController
public class PJOrderController {
	
	JsonNode email;
	JsonNode psname;
	String projectId;
	JsonNode uniqueId;
	String topic_name;
	String subscription_name;
	private static final String GOOGLE_APPLICATION_CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS";
    private static final String GCP_CREDENTIALS_FILE_NAME = "GCP_credentials.json";
	
	@RequestMapping(value ="/add", method = RequestMethod.PUT)
	public String submitOrder(@RequestBody Order order)
	{
		System.out.println("order is : " + order.getName() + "," + order.getZipcode()); 
		return "UUID is: " + UUID.randomUUID().toString();
	}

	@RequestMapping("/blah")
    public String index() {
//		String VCAP_SERVICES = System.getenv("VCAP_SERVICES");
//		
//		
//		ObjectMapper mapper = new ObjectMapper();
//		try
//		{
//			JsonNode json = mapper.readTree(VCAP_SERVICES);
//			
//			Iterator<String> names = json.fieldNames();
//			while (names.hasNext())
//			{
//				String name = names.next();
//			    System.out.println("names are " + name);
//			    List<JsonNode> values = json.findValues(name);
//			    for (JsonNode value : values)
//			    {
//			        System.out.println("value is: " + value);
//			        List<JsonNode> credentials = value.findValues("credentials");
//			        
//			        for (JsonNode creds : credentials)
//			        {
//			        	//System.out.println("creds are : " + creds);
//			        	  email = creds.findValue("Email");
//			        	 System.out.println("email is " +  email);
//			        	  psname = creds.findValue("Name");
//			        	 System.out.println("name is " +  psname);
//			        	  projectId = creds.findValue("ProjectId");
//			        	 System.out.println("projectId is " +  projectId);
//			        	  uniqueId = creds.findValue("UniqueId");
//			        	 System.out.println("uniqueId is " +  uniqueId);
//			        	  topic_name = creds.findValue("topic_name");
//			        	 System.out.println("topic_name is " +  topic_name);
//			        	 subscription_name = creds.findValue("subscription_name");
//			        	 System.out.println("subscription_name is " +  subscription_name);
//			        	 
//			        }
//			    }
//			}
//			
//			
//			//System.out.println("*****json values are: " + json.toString());
//		}
//		catch (Exception e)
//		{
//			System.out.println("error parsing vcap_services:" + e);
//		}
//		
		//System.out.println("VCAP_SERVICES is:" + VCAP_SERVICES);
		
		try
		{
			parseVcapServices();
			publishMessage();
			
			  SubscriptionName subscriptionName = SubscriptionName.create(projectId, subscription_name);
				// Instantiate an asynchronous message receiver
				MessageReceiver receiver =
				    new MessageReceiver() {
				      @Override
				      public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
				        // handle incoming message, then ack/nack the received message
				        System.out.println("Id : " + message.getMessageId());
				        System.out.println("Data : " + message.getData().toStringUtf8());
				        consumer.ack();
				      }
				    };

				Subscriber subscriber = null;
				try {
				  // Create a subscriber for "my-subscription-id" bound to the message receiver
					System.out.println("receiving mesage...");
				  subscriber = Subscriber.defaultBuilder(subscriptionName, receiver).build();
				  System.out.println("receiving mesage with subscriber... " + subscriber.toString());
				  subscriber.startAsync();
				  System.out.println("started Async()...");
				  // ...
				} finally {
				  // stop receiving messages
				  if (subscriber != null) {
				    subscriber.stopAsync();
				  }
				}
			
			
			
			
			
		}
		catch (Exception e)
		{
			System.out.println("error publishing message: " + e);
		}
		return "Greetings from PapaJohns!";
    }

	private String publishMessage() throws Exception {
		
		TopicName topicName = TopicName.create(projectId, topic_name);
		Publisher publisher = null;
	    List<ApiFuture<String>> apiFutures = new ArrayList<>();
	    try {
	        // Create a publisher instance with default settings bound to the topic
	        publisher = Publisher.defaultBuilder(topicName).build();
	        for (int i = 0; i < 5; i++) {
	          String message = "Sending message-" + i;
	          ApiFuture<String> messageId = publishMessage(publisher, message);
	          apiFutures.add(messageId);
	        }
	      } finally {
	        // Once published, returns server-assigned message ids (unique within the topic)
	        List<String> messageIds = ApiFutures.allAsList(apiFutures).get();
	        for (String messageId : messageIds) {
	          System.out.println("server-assigned message id: " + messageId);
	        }
	        if (publisher != null) {
	          // When finished with the publisher, shutdown to free up resources.
	          publisher.shutdown();
	        }
	      }
		
		return "";
		
	}
	
	//schedule a message to be published, messages are automatically batched
	  private static ApiFuture<String> publishMessage(Publisher publisher, String message)
	      throws Exception {
	    // convert message to bytes
	    ByteString data = ByteString.copyFromUtf8(message);
	    PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
	    return publisher.publish(pubsubMessage);
	  }
	  
	  private JSONObject getCredObj (String vcapKey) throws Exception {
	        String env = System.getenv("VCAP_SERVICES");
	        JSONObject json = new JSONObject(env);
	        JSONArray root = json.getJSONArray(vcapKey);
	        JSONObject obj0 = root.getJSONObject(0);
	        return obj0.getJSONObject("credentials");
	    }
	  
	  private void setupCredentials(String privateKey) {
	        InputStream in = new ByteArrayInputStream(Base64.getDecoder().decode(privateKey));
	        File gcpJsonFile = new File(System.getProperty("java.io.tmpdir"), GCP_CREDENTIALS_FILE_NAME);
	        writeInputStreamToFile(in, gcpJsonFile);

	        Map<String, String> replEnv = new HashMap<>();
	        replEnv.put(GOOGLE_APPLICATION_CREDENTIALS, gcpJsonFile.getPath());
	        setEnv(replEnv);
	    }
	  
	  private static void writeInputStreamToFile (InputStream is, File outFile) {
	        try {
	            FileCopyUtils.copy(is, new FileOutputStream(outFile));
	        } catch (IOException e) {
	            throw new RuntimeException("Failed while creating " + GCP_CREDENTIALS_FILE_NAME + " file", e);
	        }
	    }
	
	  private static void setEnv(Map<String, String> newenv) {
	        try {
	            Class<?>[] classes = Collections.class.getDeclaredClasses();
	            Map<String, String> env = System.getenv();
	            for (Class<?> cl : classes) {
	                if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
	                    Field field = cl.getDeclaredField("m");
	                    field.setAccessible(true);
	                    Object obj = field.get(env);
	                    @SuppressWarnings("unchecked")
	                    Map<String, String> map = (Map<String, String>) obj;
	                    map.clear();
	                    map.putAll(newenv);
	                }
	            }
	        } catch (Exception e) {
	            throw new RuntimeException("Failed while setting " + GOOGLE_APPLICATION_CREDENTIALS + " environment variable.", e);
	        }
	    }
	  
	  private void parseVcapServices() {
		  
		  try
		  {
	        // 1. Get the MySQL details.
	         projectId = null; // Get this later, from a different service binding

	        // 2. Get remaining parts from Storage binding.
	        JSONObject pubsubCreds = getCredObj("google-pubsub");
	        projectId = pubsubCreds.getString("ProjectId");
	        System.out.println("projectId is " + projectId);
	        
		        String email = pubsubCreds.getString("Email");
	       	 System.out.println("email is " +  email);
	       	  String psname = pubsubCreds.getString("Name");
	       	 System.out.println("name is " +  psname);
	       	  projectId = pubsubCreds.getString("ProjectId");
	       	 System.out.println("projectId is " +  projectId);
	       	  String uniqueId = pubsubCreds.getString("UniqueId");
	       	 System.out.println("uniqueId is " +  uniqueId);
	       	   topic_name = pubsubCreds.getString("topic_name");
	       	 System.out.println("topic_name is " +  topic_name);
	       	 subscription_name = pubsubCreds.getString("subscription_name");
	       	 System.out.println("subscription_name is " +  subscription_name);
	        

	        // 3. Write out the GOOGLE_APPLICATION_CREDENTIALS file and set up environment variable.
	        String privateKeyData = pubsubCreds.getString("PrivateKeyData");
	        System.out.println("privateKeyData is " +  privateKeyData);
	        
	        setupCredentials(privateKeyData);
		  }
		  catch (Exception e)
		  {
			  System.out.println("something went wrong here: " + e);
		  }
	    }
	  
	
	  
}

/*
 * // NEED: TopicName, Publisher, Subscriber
    @Bean
    TopicName topicName() {
        if (topicName == null && properties.getTopicName() != null) {
            topicName = TopicName.create(properties.getProjectId(), properties.getTopicName());
        }
        return topicName;
    }

    @Bean
    Publisher publisher() {
        Publisher rv = null;
        if (properties.getTopicName() != null) {
            try {
                rv = Publisher.defaultBuilder(topicName()).build();
            } catch (IOException ioe) {
                throw new GcpRuntimeException(ioe);
            }
        }
        return rv;
    }

    
     * Handling it this way since getting a Subscriber instance requires you to pass in
     * an implementation of the MessageReceiver interface, and we can't know in advance
     * what someone would want theirs to do.
     
    @Bean
    SubscriberFactory subscriberFactory() {
        SubscriberFactory rv = null;
        if (properties.getSubscriptionName() != null) {
            SubscriptionName subscriptionName = SubscriptionName.create(properties.getProjectId(),
                    properties.getSubscriptionName());
            rv = new SubscriberFactory(subscriptionName);
            // Ref. https://stackoverflow.com/questions/43526675/google-pub-sub-reuse-existing-subsription
            try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
                try {
                    subscriptionAdminClient.getSubscription(subscriptionName);
                    LOG.info("Got Subscription \"" + subscriptionName.getSubscription() + "\"");
                } catch (Exception noSub) {
                    subscriptionAdminClient.createSubscription(subscriptionName, topicName(), PushConfig.getDefaultInstance(), 0);
                    LOG.info("Created Subscription \"" + subscriptionName.getSubscription() + "\"");
                }
            } catch (Exception e) {
                throw new GcpRuntimeException(e);
            }
        }
        return rv;
    }
 */

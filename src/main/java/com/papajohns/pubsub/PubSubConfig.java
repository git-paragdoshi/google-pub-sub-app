package com.papajohns.pubsub;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.util.FileCopyUtils;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;


public class PubSubConfig {
	private String projectId;
	private static final String GOOGLE_APPLICATION_CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS";
    private static final String GCP_CREDENTIALS_FILE_NAME = "GCP_credentials.json";
	private String topic_name;
	private String subscription_name;
	private Publisher publisher;
	private TopicName topicName;
	private SubscriberFactory subscriberFactory;
	private Subscriber subscriber = null;
	
	
	public String getProjectId() {
		return projectId;
	}


	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}


	public String getTopic_name() {
		return topic_name;
	}


	public void setTopic_name(String topic_name) {
		this.topic_name = topic_name;
	}


	public String getSubscription_name() {
		return subscription_name;
	}


	public void setSubscription_name(String subscription_name) {
		this.subscription_name = subscription_name;
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
	
	public void parseVcapServices(String vcapServices) {
		  
		  try
		  {

	        // 2. Get remaining parts from PubSub binding.
	        JSONObject pubsubCreds = getCredObj(vcapServices, "google-pubsub");
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

	 public void cleanup() {
	        if (publisher != null) {
	            try {
	                publisher.shutdown();
	            } catch (Exception e) {
	            }
	            publisher = null;
	        }
	        if (subscriber != null) {
	            subscriber.stopAsync();
	            subscriber = null;
	        }
	    }
	 
	 private JSONObject getCredObj (String vcapServices, String vcapKey) throws Exception {
	        String env = System.getenv(vcapServices);
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
	  
	  public TopicName getTopicName() {
          topicName = TopicName.create(projectId, topic_name);
      return topicName;
  }

  public Publisher getPublisher(TopicName topicName) {
      Publisher rv = null;
      if (topicName != null) {
          try {
              rv = Publisher.defaultBuilder(topicName).build();
          } catch (IOException ioe) {
              throw new RuntimeException(ioe);
          }
      }
      return rv;
  }
  
  public SubscriberFactory getSubscriberFactory(TopicName topicName) {
      SubscriberFactory rv = null;
          SubscriptionName subscriptionName = SubscriptionName.create(projectId,
                  subscription_name);
          rv = new SubscriberFactory(subscriptionName);
          // Ref. https://stackoverflow.com/questions/43526675/google-pub-sub-reuse-existing-subsription
          try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
              try {
                  subscriptionAdminClient.getSubscription(subscriptionName);
              } catch (Exception noSub) {
                  subscriptionAdminClient.createSubscription(subscriptionName, topicName, PushConfig.getDefaultInstance(), 0);
              }
          } catch (Exception e) {
              throw new RuntimeException(e);
          }
      return rv;
  }

	
}

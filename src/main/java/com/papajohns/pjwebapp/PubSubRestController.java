package com.papajohns.pjwebapp;

import com.google.api.core.ApiFuture;


import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import com.papajohns.domain.Order;
import com.papajohns.pubsub.MessageWrapper;
import com.papajohns.pubsub.PubSubConfig;
import com.papajohns.pubsub.SubscriberFactory;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.cloudfoundry.com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PreDestroy;


import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.List;

@RestController
public class PubSubRestController {

	JsonNode email;
	JsonNode psname;
	//String projectId;
	JsonNode uniqueId;
//	String topic_name;
	//String subscription_name;
	
	
    private static final Logger LOG = LoggerFactory
            .getLogger(PubSubRestController.class);

    private static final int QUEUE_CAPACITY = 100;
    private static final BlockingQueue<PubsubMessage> queue = new LinkedBlockingDeque<>(QUEUE_CAPACITY);
    private static final long T_SLEEP_MS = 1000;

    private Publisher publisher;
    private TopicName topicName;
    private SubscriberFactory subscriberFactory;
    private Subscriber subscriber = null;
    private PubSubConfig pubsubConfig = null;
    private String ORDER_STATUS_APPROVED_PAYMENT="Approved Payment";
    private String ORDER_STATUS_ACCEPTED_ORDER="Accepted Order";
    private String ORDER_STATUS_BEGAN_MAKING="Began Making";
    private String ORDER_STATUS_BEGAN_BAKING="Began Baking";
    private String ORDER_STATUS_OUT_FOR_DELIVERY="Out For Delivery";
    private String ORDER_STATUS_DELIVERED="Delivered";
    private Map<String, String> orders = new HashMap<>();
   

    @PreDestroy
    public void cleanup() {
    	pubsubConfig.cleanup();
    }

    public PubSubRestController() {
    	pubsubConfig = new PubSubConfig();
    	pubsubConfig.parseVcapServices("VCAP_SERVICES");
        this.topicName = pubsubConfig.getTopicName();
        this.publisher = pubsubConfig.getPublisher(topicName);
        this.subscriberFactory = pubsubConfig.getSubscriberFactory(topicName);
        
        
//        // setup topic and subscriber.
//        if (topicName != null && subscriber == null) {
//            LOG.info(this.getClass().getCanonicalName() + "TopicName: " + topicName.getTopic());
//            LOG.info("Setting up Subscriber ...");
//            MessageReceiver receiver = new MessageReceiver() {
//                @Override
//                public void receiveMessage(final PubsubMessage message, final AckReplyConsumer consumer) {
//                    synchronized (queue) {
//                        if (queue.offer(message)) {
//                            consumer.ack();
//                            LOG.info("ACK'd");
//                        } else {
//                            consumer.nack();
//                            LOG.info("NACK'd");
//                        }
//                    }
//                }
//            };
//            subscriber = subscriberFactory.newSubscriber(receiver);
//            LOG.info("Adding listener ...");
//            subscriber.addListener(
//                    new Subscriber.Listener() {
//                        @Override
//                        public void failed(Subscriber.State from, Throwable failure) {
//                            // Handle failure. This is called when the Subscriber encounters a fatal error and is shutting down.
//                            LOG.error("Failure " + failure.getMessage());
//                        }
//                    },
//                    MoreExecutors.directExecutor());
//            LOG.info("Starting async Subscriber ...");
//            subscriber.startAsync();
//            try {
//                Thread.sleep(T_SLEEP_MS);
//            } catch (InterruptedException ie) {
//                LOG.error(ie.getMessage());
//            }
//        }
    }
    
   
    @CrossOrigin
    @RequestMapping (method=RequestMethod.GET, value="/orderStatus", produces = "application/json")
    public @ResponseBody Map<String, String>  getOrderStatus(@RequestParam("id") String orderId)
    {
    	LOG.info("orderId in request is: " + orderId);
    	
    	String currentOrderStatus = orders.get(orderId);
    	LOG.info("currentOrderStatus is: " + currentOrderStatus);
    	if(currentOrderStatus == null)
    	{
    		currentOrderStatus = ORDER_STATUS_ACCEPTED_ORDER;
    	}
    	
    	String nextOrderStatus = getNextOrderStatus(currentOrderStatus);
    	LOG.info("nextOrderStatus is: " + nextOrderStatus);
    	
    	orders.put(orderId, nextOrderStatus);
    	return Collections.singletonMap("status", nextOrderStatus);
    	
    }
    // GET
//    @RequestMapping(method = RequestMethod.GET, value = "/fetch")
//    public MessageWrapper fetch() {
//        MessageWrapper rv = null;
//        synchronized (queue) {
//            if (!queue.isEmpty()) {
//                LOG.info("Taking a message off the queue");
//                try {
//                    PubsubMessage msg = queue.take();
//                    // message.getData().toStringUtf8())
//                    rv = new MessageWrapper(msg.getMessageId(), msg.getPublishTime().getSeconds(), msg.getData().toStringUtf8());
//                } catch (InterruptedException e) {
//                    LOG.info("error"  + e.getMessage());
//                }
//            } else {
//                LOG.info("Queue is empty");
//            }
//        }
//        return rv;
//    }
   
    @CrossOrigin
    @RequestMapping (method=RequestMethod.POST, value="/insert", produces=MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody Map<String, String> insertOrder(@RequestBody Order order)
    {
    	//sending message to topic..
    	String msgId;
    	String uuid = UUID.randomUUID().toString();
    	LOG.info("uuid :" + uuid);
    	ByteString data = ByteString.copyFromUtf8(order.getName());
    	PubsubMessage msg = PubsubMessage.newBuilder().setData(data).build();
    	ApiFuture<String> apiFuture = publisher.publish(msg);
        try {
            msgId = apiFuture.get();
        } catch (Exception e) {
            throw new RuntimeException("error occurred: " + e);
        }
        
    	orders.put(uuid, ORDER_STATUS_APPROVED_PAYMENT);
    	return Collections.singletonMap("id", uuid);
    	
    }
    
   
    @RequestMapping(method = RequestMethod.POST, value = "/send")
    public String send(@RequestBody String msgStr) {
        String msgId = "";
        LOG.info("Sending \"" + msgStr + "\"");
        ByteString data = ByteString.copyFromUtf8(msgStr);
        PubsubMessage msg = PubsubMessage.newBuilder().setData(data).build();
        ApiFuture<String> apiFuture = publisher.publish(msg);
        try {
            msgId = apiFuture.get();
        } catch (Exception e) {
            throw new RuntimeException("error occurred" + e);
        }
        return msgId;
    }
    
//    @RequestMapping(method = RequestMethod.GET, value = "/fetchSync")
//    public List<String> fetchSync() 
//    {
//    	try
//    	{
//    		return createSubscriberWithSyncPull(pubsubConfig.getProjectId(), pubsubConfig.getSubscription_name(), 100);
//    	}
//    	catch(Exception e)
//    	{
//    		LOG.error(e.getMessage());
//    	}
//    	
//    	return null;
//    }
//    
//     private List<String> createSubscriberWithSyncPull(
//    	      String projectId, String subscriptionId, int numOfMessages) throws Exception 
//     {
//    	    // [START subscriber_sync_pull]
//    	 	List<String> mess = new ArrayList<>();
//    	    SubscriptionAdminSettings subscriptionAdminSettings =
//    	        SubscriptionAdminSettings.newBuilder().build();
//    	    try (SubscriberStub subscriber = GrpcSubscriberStub.create(subscriptionAdminSettings)) {
//    	    	String subscriptionName = SubscriptionName.create(projectId, subscriptionId).toString();
//    	    	PullRequest pullRequest =
//    	          PullRequest.newBuilder()
//    	              .setMaxMessages(numOfMessages)
//    	              .setReturnImmediately(false) // return immediately if messages are not available
//    	              .setSubscription(subscriptionName)
//    	              .build();
//    	      // use pullCallable().futureCall to asynchronously perform this operation
//    	      PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
//    	      List<String> ackIds = new ArrayList<>();
//    	      mess = new ArrayList<>();
//    	      for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
//    	        // handle received message
//    	        // ...
//    	        ackIds.add(message.getAckId());
//    	        LOG.info("AckId is: " + message.getAckId());
//    	        LOG.info("message is: " + message.getMessage().getData().toStringUtf8());
//    	        mess.add(message.getMessage().getData().toString());
//    	      }
//    	      // acknowledge received messages
//    	      AcknowledgeRequest acknowledgeRequest =
//    	          AcknowledgeRequest.newBuilder()
//    	              .setSubscription(subscriptionName)
//    	              .addAllAckIds(ackIds)
//    	              .build();
//    	      // use acknowledgeCallable().futureCall to asynchronously perform this operation
//    	      subscriber.acknowledgeCallable().call(acknowledgeRequest);
////    	      List<ReceivedMessage> messages = pullResponse.getReceivedMessagesList();
////    	      LOG.info("messages is :" + messages.size());
//    	      return mess;
//    	    }
//    	    // [END subscriber_sync_pull]
//    	  }
//
//     
     private String getNextOrderStatus(String currentOrderStatus)
     {
  	   if((ORDER_STATUS_APPROVED_PAYMENT).equals(currentOrderStatus))
  	   {
  		   return ORDER_STATUS_ACCEPTED_ORDER;
  	   }
  	   else if((ORDER_STATUS_ACCEPTED_ORDER).equals(currentOrderStatus))
  	   {
  		   return ORDER_STATUS_BEGAN_MAKING;
  	   }
  	   else if((ORDER_STATUS_BEGAN_MAKING).equals(currentOrderStatus))
  	   {
  		   return ORDER_STATUS_BEGAN_BAKING;
  	   }
  	   else if((ORDER_STATUS_BEGAN_BAKING).equals(currentOrderStatus))
  	   {
  		   return ORDER_STATUS_OUT_FOR_DELIVERY;
  	   }
  	   else if((ORDER_STATUS_OUT_FOR_DELIVERY).equals(currentOrderStatus))
  	   {
  		   return ORDER_STATUS_DELIVERED;
  	   }
  	   else
  	   return ORDER_STATUS_DELIVERED;
     }
}
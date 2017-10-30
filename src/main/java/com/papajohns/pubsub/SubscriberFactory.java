package com.papajohns.pubsub;

import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.SubscriptionName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriberFactory {

	private static final Logger LOG = LoggerFactory
            .getLogger(SubscriberFactory.class);

    private SubscriptionName subscriptionName;

    public SubscriberFactory(SubscriptionName subscriptionName) {
        this.subscriptionName = subscriptionName;
        LOG.info("SubscriptionName: " + subscriptionName.toString());
    }

    public Subscriber newSubscriber(MessageReceiver messageReceiver) {
        return Subscriber.defaultBuilder(subscriptionName, messageReceiver).build();
    }

}

package com.papajohns.config;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.cloud.config.java.AbstractCloudConfig;
import org.springframework.cloud.config.java.ServiceScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@ServiceScan
@Profile("cloud")
public class CloudConfig extends AbstractCloudConfig {
	
//	@Bean
//	public ConnectionFactory rabbitConnectionFactory()
//	{
//		return connectionFactory().rabbitConnectionFactory();
//	}
}

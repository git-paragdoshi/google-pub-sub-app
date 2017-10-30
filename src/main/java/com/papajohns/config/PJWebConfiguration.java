package com.papajohns.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

@Configuration
public class PJWebConfiguration {
	
	    @Bean
	    public WebMvcConfigurerAdapter corsConfigurer() {
	        return new WebMvcConfigurerAdapter() {
	            @Override
	            public void addCorsMappings(CorsRegistry registry) {
	                registry.addMapping("/**")
	                .allowedMethods("PUT", "DELETE","GET","POST","OPTIONS")
	                .maxAge(3600);
	            }
	        };
	    }
	}


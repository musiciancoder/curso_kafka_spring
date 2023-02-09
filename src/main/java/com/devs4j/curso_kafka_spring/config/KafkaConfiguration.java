package com.devs4j.curso_kafka_spring.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {

    //Mapa con las configuraciones del producer
    public Map<String, Object> producerProperties() {

        Map<String, Object> props=new HashMap<>();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    "localhost:9092");

            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    IntegerSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class);
            return props;
        }

        //template
        @Bean
        public KafkaTemplate<String, String> kafkaTemplate(){
           DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerProperties());
            KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory);
            return template;
        }

    //Mapa con las configuraciones del consumer
    public Map<String, Object> consumerProperties() {
            Map<String, Object>props=new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    "localhost:9092");
            props.put(ConsumerConfig.GROUP_ID_CONFIG,
                    "group");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class);
            return props;
    }

    //factory para el consumer
    @Bean
    public ConsumerFactory<String, String>consumerFactory(){
           return new DefaultKafkaConsumerFactory<>(consumerProperties());
    }

    @Bean(name="listenerContainerFactory")
    //listener
    public ConcurrentKafkaListenerContainerFactory<String, String> listenerContainerFactory(){
        ConcurrentKafkaListenerContainerFactory<String, String> listenerContainerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        listenerContainerFactory.setConsumerFactory(consumerFactory());
        listenerContainerFactory.setBatchListener(true); //para batch
           return listenerContainerFactory;
    }


}

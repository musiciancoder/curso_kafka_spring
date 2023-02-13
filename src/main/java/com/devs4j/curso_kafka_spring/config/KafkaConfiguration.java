package com.devs4j.curso_kafka_spring.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.HashMap;
import java.util.Map;



@Configuration
@EnableScheduling //anotacion de spring que permite realizar algo cada cierto tiempo
public class KafkaConfiguration {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfiguration.class);

    //Mapa con las configuraciones del producer
    public Map<String, Object> producerProperties() {

        Map<String, Object> props=new HashMap<>();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    "localhost:9092");

            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class);
            return props;
        }

        //template
        @Bean
        public KafkaTemplate<String, String> kafkaTemplate(){
           DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerProperties());
           producerFactory.addListener(new MicrometerProducerListener<String, String>(meterRegistry())); //Métricas utilizando Micrometer
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
        listenerContainerFactory.setConcurrency(3); //tan solo con esta linea definimos 3 consumers (o sea 3 threads). Por ello en los logs van a salir [ntainer#0-0-C-1] , [ntainer#0-1-C-1] , [ntainer#0-2-C-1]
           return listenerContainerFactory;
    }

    //Métricas utilizando Micrometer
    @Bean
    public MeterRegistry meterRegistry(){
        PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        return meterRegistry;
    }


}

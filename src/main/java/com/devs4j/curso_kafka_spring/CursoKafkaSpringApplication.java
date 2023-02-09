package com.devs4j.curso_kafka_spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class CursoKafkaSpringApplication implements CommandLineRunner {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate; //guardado con @Bean en clase KafkaConfiguration

	private static final Logger log = LoggerFactory.getLogger(CursoKafkaSpringApplication.class);

	@KafkaListener(topics= "devs4j-topic", groupId = "devs4j-group") //con @KafkaListener podemos recibir mensajes enviados desde consola comandos del producer
     public void listen(String message){ //este metodo va a recibir mensajes de kafka
		log.info("Message received {}", message);
	}

	public static void main(String[] args) {

		SpringApplication.run(CursoKafkaSpringApplication.class, args);
	}



	//ver mensajes que se estan enviando en los logger de spring
	@Override
	public void run(String... args) throws Exception {
		//kafkaTemplate.send("devs4j-topic","Sample message"); //al principio solo corrió esta linea para probar

		//código sincrono
		kafkaTemplate.send("devs4j-topic","Sample message").get(100, TimeUnit.MILLISECONDS);

		/*

		//Código asincrono
		ListenableFuture<SendResult<String,String>>future = (ListenableFuture<SendResult<String, String>>) kafkaTemplate.send("devs4j-topic","Sample message");
	    future.addCallback(new KafkaSendCallback<String, String>(){

		@Override
		public void onSuccess(SendResult<String, String> result) {
          log.info("Message sent", result.getRecordMetadata().offset());
		}

		@Override
		public void onFailure(KafkaProducerException e) {
              log.error("Error",e);
		}
	});*/
	}
}

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

import java.util.List;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class CursoKafkaSpringApplication implements CommandLineRunner {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate; //guardado con @Bean en clase KafkaConfiguration

	private static final Logger log = LoggerFactory.getLogger(CursoKafkaSpringApplication.class);

	//con @KafkaListener podemos recibir mensajes enviados desde consola comandos del producer

	//Sin Batch
	/*
	@KafkaListener(topics= "devs4j-topic",groupId = "devs4j-group")
	public void listen(String message){
		log.info("Message received {}", message);
	}

	 */


	//Con Batch
	@KafkaListener(topics= "devs4j-topic",
			containerFactory = "listenerContainerFactory", //esto es para los batch. debemos escribir el nombre del bean que le dimos en KafkaConfiguration
			groupId = "devs4j-group",
	properties = {"max.poll.interval.ms:4000",//los batch cada 4 segundos
	"max.poll.records:10"})//los batch de 10 en 10
     public void listen(List<String> message){ //este metodo va a recibir mensajes de kafka
		log.info("Start reading messages");
		for (String msg:message
			 ) {
			log.info("Message received {}", msg);
		}
		log.info("Batch complete");
	}

	public static void main(String[] args) {

		SpringApplication.run(CursoKafkaSpringApplication.class, args);
	}



	//ver mensajes que se estan enviando en los logger de spring
	@Override
	public void run(String... args) throws Exception {
		//kafkaTemplate.send("devs4j-topic","Sample message"); //al principio solo corrió esta linea para probar

		//código sincrono
	//	kafkaTemplate.send("devs4j-topic","Sample message").get(100, TimeUnit.MILLISECONDS);

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

		//Codigo con batch (ni sincrono ni asincrono)
		for (int i = 0; i < 100; i++) {
			kafkaTemplate.send("devs4j-topic",String.format("Sample message %d", i));
		}

	}
}

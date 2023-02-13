package com.devs4j.curso_kafka_spring;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
//public class CursoKafkaSpringApplication implements CommandLineRunner { //lo quitó (junto con el metodo run) en la seccion Acceso a métricas y lo reemplazó linea de abajo
public class CursoKafkaSpringApplication {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate; //guardado con @Bean en clase KafkaConfiguration

    @Autowired
    private MeterRegistry meterRegistry; //lo agrego en seccion Acceso a métricas

	/*
	@Autowired
	private KafkaListenerEndpointRegistry registry; //Lo agregó en seccion "Pausando y reanudando consumo de mensajes"

*/


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
    @KafkaListener(
            id = "devs4jId",
            autoStartup = "true", //por defecto esta en true. Lo agregó en seccion "Pausando y reanudando consumo de mensajes". En seccion Acceso a métricas lo dejo en true nuevamente
            topics = "devs4j-topic",
            containerFactory = "listenerContainerFactory", //esto es para los batch. debemos escribir el nombre del bean que le dimos en KafkaConfiguration
            groupId = "devs4j-group",
            properties = {"max.poll.interval.ms:4000",//los batch cada 4 segundos
                    "max.poll.records:50"})//los batch de 10 en 10 (despues lo cambió a 50)
    public void listen //este metodo va a recibir mensajes de kafka
    (List<ConsumerRecord<String, String>> messages) {   //al principio recibia una lista de string como argumento...pero en seccion Accediendo a la información completa del mensaje (para mostrar particion, offset, key y value) cambio a ConsumerRecord
        log.info("Messages received {}", messages.size());
        //	log.info("Start reading messages");
		/*
		//lo comentó en seccion  Acceso a métricas
		for (ConsumerRecord<String,String>msg:messages) {
			log.info("Partition= {}, Offset={}, Key={}, Value= {}", msg.partition(), msg.offset(), msg.key(), msg.value());
		}

		 **/
        //	log.info("Batch complete");
    }

    public static void main(String[] args) {

        SpringApplication.run(CursoKafkaSpringApplication.class, args);
    }

    @Scheduled(fixedDelay = 2000, initialDelay = 100)
    //cada cierto tiempo se ejecuta esto. Se usa en combinacion con @EnableScheduling
    public void sendKafkaMessages() {
        //	log.info("Devs4j rocks!");
        for (int i = 0; i < 100; i++) {
            kafkaTemplate.send("devs4j-topic", String.valueOf(i), String.format("Sample message %d", i));
        }
    }

    //Acceso a métricas
    @Scheduled(fixedDelay = 2000, initialDelay = 500)
    public void printMetrics() {
        double count = meterRegistry.get("kafka.producer.record.send.total").functionCounter().count();
        log.info("Count {} ", count); //cuenta de cuantos mensajes se han entregado

    }


    //ver mensajes que se estan enviando en los logger de spring

	/*
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
		/*
		for (int i = 0; i < 100; i++) {
		}
		//código agregado en sección Pausando y reanudando el consumo de mensajes
		/*
		 */
		/*
		log.info("Waiting to start");
		Thread.sleep(5000);
		log.info("starting...");
		registry.getListenerContainer("devs4jId").start(); //partir automaticamente consumer luego de 5 segundos (ojo, solo para la primera vez, no para reiniciar!)
		Thread.sleep(5000);
		registry.getListenerContainer("devs4jId").stop(); //parar automaticamente consumer luego de 5 sefundos
*/

}




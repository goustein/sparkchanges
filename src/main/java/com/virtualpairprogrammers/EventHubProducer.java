package com.virtualpairprogrammers;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;

public class EventHubProducer {
	
	
public static void main(String[] args) {
	  final String connectionString = "EVENT HUBS NAMESPACE CONNECTION STRING";
      final String eventHubName = "EVENT HUB NAME";

      // create a producer using the namespace connection string and event hub name
      EventHubProducerClient producer = new EventHubClientBuilder()
          .connectionString(connectionString, eventHubName)
          .buildProducerClient();

      // prepare a batch of events to send to the event hub    
      EventDataBatch batch = producer.createBatch();
      batch.tryAdd(new EventData("First event"));
      batch.tryAdd(new EventData("Second event"));
      batch.tryAdd(new EventData("Third event"));
      batch.tryAdd(new EventData("Fourth event"));
      batch.tryAdd(new EventData("Fifth event"));

      // send the batch of events to the event hub
      producer.send(batch);

      // close the producer
      producer.close();
}
}

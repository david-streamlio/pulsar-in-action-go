/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// Note: relace JWT token, tenant, namespace, and topic
func main() {
	log.Println("Pulsar Producer")

	uri := "pulsar://localhost:6650"
	topicName := "persistent://public/default/my-topic"

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                   uri,
		OperationTimeout:      30 * time.Second,
		ConnectionTimeout:     30 * time.Second,
	})

	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	defer client.Close()

	log.Printf("creating producer...")

	// Use the client to instantiate a producer
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topicName,
	})

	log.Printf("checking error of producer creation...")
	if producer == nil {
		log.Print("producer is null")
	}
	if err != nil {
		log.Fatal(err)
	}

	defer producer.Close()

	ctx := context.Background()

	deliveryTime := (time.Minute * time.Duration(1)) + (time.Second * time.Duration(30))
	
	for i := 0; i < 3; i++ {
		// Create a message
		msg := pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("Delayed-messageId-%d", i)),
			Key: "message-key",
		    Properties: map[string]string{
		        "delayed": "90sec",
		    },
		    EventTime: time.Now(),
		    DeliverAfter: deliveryTime,
		}

		// Attempt to send the message
		messageID, err := producer.Send(ctx, &msg)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("the %s successfully published with the message ID %v\n", string(msg.Payload), messageID)
	}
}


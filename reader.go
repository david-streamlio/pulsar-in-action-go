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

// Note: relace JWT token, host, tenant, namespace, and topic
func main() {
	fmt.Println("Pulsar Reader")

	uri := "pulsar://localhost:6650"
	topicName := "persistent://public/default/my-topic"

	// Pulsar client
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                   uri,
		OperationTimeout:      30 * time.Second,
		ConnectionTimeout:     30 * time.Second,
	})

	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	reader, err := client.CreateReader(pulsar.ReaderOptions{
		Topic:          topicName,
		StartMessageID: pulsar.EarliestMessageID(),
	})

	if err != nil {
		log.Fatal(err)
	}

	defer reader.Close()

	ctx := context.Background()

	// infinite loop to receive messages
	for {
		msg, err := reader.Next(ctx)
		if err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Received message : %v\n", string(msg.Payload()))
		}
	}

}


/*
 * Copyright (c) 2021 Cable Television Laboratories, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.siddhi.extension.io.udp.source;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Class for listening to Kafka topics.
 */
class KafkaRunnable implements Runnable {

    final Consumer<Long, String> consumer;
    int numRecordsCount = 0;
    final List<String> events = new ArrayList<>();

    KafkaRunnable(final Consumer<Long, String> consumer) {
        this.consumer = consumer;
    }

    private volatile boolean running = true;

    public void stop() {
        running = false;
    }

    @Override
    public void run() {
        while (running) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(10));
            if (consumerRecords.count() != 0) {
                numRecordsCount += consumerRecords.count();
                consumerRecords.forEach(record -> {
                    System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                            record.key(), record.value(),
                            record.partition(), record.offset());
                    events.add(record.value());
                });
            }
            consumer.commitSync();
        }
        consumer.close();
    }

    public static KafkaRunnable runConsumer(final String kafkaServer, final String topic) {
        final Consumer<Long, String> consumer = createConsumer(kafkaServer, topic);
        final KafkaRunnable out = new KafkaRunnable(consumer);
        final Thread kafkaConsumerThread = new Thread(out);
        kafkaConsumerThread.start();
        return out;
    }

    private static Consumer<Long, String> createConsumer(final String kafkaServer, final String topic) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "UDPSourceKafkaSinkTelemetryReportTestCase-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

}

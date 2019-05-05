/*
 * Copyright (c) 2016 Intel Corporation
 *
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
 *
 */

package org.oisp.transformation;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.oisp.conf.Config;
import org.oisp.conf.ConfigFactory;


public class KafkaSourceProcessor {

    public static final String KAFKA_URI_PROPERTY = Config.KAFKA_URI_PROPERTY;
    public static final String KAFKA_ZOOKEEPER_PROPERTY = Config.KAFKA_ZOOKEEPER_PROPERTY;


    private KafkaIO.Read<String, byte[]> transform = null;

    public KafkaIO.Read<String, byte[]> getTransform() {
        return transform;
    }

    public KafkaSourceProcessor(Config userConfig, String topic) {


        String zookeeperQuorum = userConfig.get(KAFKA_ZOOKEEPER_PROPERTY).toString();
        String serverUri = userConfig.get(KAFKA_URI_PROPERTY).toString();

        Config consumerProperties = new ConfigFactory().getConfig();
        consumerProperties.put("zookeeper.connect", zookeeperQuorum);
        consumerProperties.put("group.id", "beam");
        consumerProperties.put("enable.auto.commit", "true");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        transform = KafkaIO.<String, byte[]>read()
                .withBootstrapServers(serverUri)
                .withTopic(topic)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(ByteArrayDeserializer.class)
                .updateConsumerProperties(consumerProperties.getHash())
                .withReadCommitted()
                .commitOffsetsInFinalize();

    }
}

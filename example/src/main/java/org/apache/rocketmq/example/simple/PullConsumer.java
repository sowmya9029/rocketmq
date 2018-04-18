/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.simple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

public class PullConsumer {
    private static final Map<MessageQueue, Long> OFFSE_TABLE = new HashMap<MessageQueue, Long>();

    public static void main(String[] args) throws MQClientException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("g2");
        List<Message> messages = new ArrayList<>();
        Map<Integer,Message> buffer = new HashMap<>();
        int expectedMessage = 0;
        consumer.start();

        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("tp4");
//        while (true) {
//            mqs.forEach((mq) -> {
//                try {
//                    PullResult pullResult =
//                        consumer.pull(mq, null, getMessageQueueOffset(mq), 1);
//                } catch (MQClientException | RemotingException | InterruptedException | MQBrokerException e) {
//                    e.printStackTrace();
//                }
//            });
//        break;
//        }

        for (MessageQueue mq : mqs) {
            System.out.printf("Consume from the queue: %s%n", mq);
            SINGLE_MQ:
            while (true) {
                try {
                    PullResult pullResult =
                        consumer.pull(mq, null, getMessageQueueOffset(mq), 1);
//                    System.out.printf("%s%n", pullResult);
                    System.out.printf("%s %n", pullResult.getMsgFoundList());
                    if(pullResult.getMsgFoundList() == null || pullResult.getMsgFoundList().size() == 0)
                        break;

                    String id = pullResult.getMsgFoundList().get(0).getKeys();

                    // KEY+i => remove key and parse i
                    int idd = Integer.parseInt(id.substring(3));

                    System.out.printf("key is %d",idd);

                    if(idd == expectedMessage) {
                        messages.add(pullResult.getMsgFoundList().get(0));
                        expectedMessage++;
                        putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                    } else {
                        while(buffer.containsKey(expectedMessage)) {
                            messages.add(buffer.remove(expectedMessage));
                            expectedMessage++;
                        }
                        buffer.put(expectedMessage,pullResult.getMsgFoundList().get(0));
                        putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                    }
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            break;
                        case NO_MATCHED_MSG:
                            break;
                        case NO_NEW_MSG:
                            break SINGLE_MQ;
                        case OFFSET_ILLEGAL:
                            break;
                        default:
                            break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        for(int i=0;i<messages.size();i++) {
            System.out.printf("index is %d \n", i);
            System.out.printf("message is %s \n", messages.get(i));
        }
        consumer.shutdown();
    }

    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = OFFSE_TABLE.get(mq);
        if (offset != null)
            return offset;

        return 0;
    }

    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        OFFSE_TABLE.put(mq, offset);
    }

}

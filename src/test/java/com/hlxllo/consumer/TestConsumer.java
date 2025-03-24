package com.hlxllo.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import java.util.List;

/**
 * @author hlxllo
 * @description 消费者测试
 * @date 2025/3/23
 */
public class TestConsumer {
    // 乱序消费
    @Test
    public void testConsumer() throws Exception {
        // 指定消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-group");
        // 设置nameserver
        consumer.setNamesrvAddr("192.168.26.3:9876");
        // 订阅消费主题，"*"表示无过滤参数
        consumer.subscribe("testTopic", "*");
        // 注册监听（默认20个线程）
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
                System.out.println(Thread.currentThread().getName() + "----" + list);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
    }

    // 顺序消费
    @Test
    public void testOrderlyConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-group");
        consumer.setNamesrvAddr("192.168.26.3:9876");
        consumer.subscribe("testTopic", "*");
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext context) {
                MessageExt messageExt = list.get(0);
                System.out.println(new String(messageExt.getBody()) + messageExt.getQueueId());
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
    }

    // 标签消息
    @Test
    public void testTagConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-group");
        consumer.setNamesrvAddr("192.168.26.3:9876");
        // 订阅一个主题来消费  表达式，默认是*,支持"tagA || tagB || tagC" 这样或者的写法 只要是符合任何一个标签都可以消费
        consumer.subscribe("testTopic", "tagA || tagB || tagC");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messageExts, ConsumeConcurrentlyContext context) {
                System.out.println(Thread.currentThread().getName() + "----" + new String(messageExts.get(0).getBody()));
                System.out.println(messageExts.get(0).getTags());
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
    }

    // 消费者重试
    @Test
    public void testRetryConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-group");
        consumer.setNamesrvAddr("192.168.26.3:9876");
        consumer.subscribe("testTopic", "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
                try {
                    System.out.println(list);
                    int i = 10 / 0;
                } catch (Exception e) {
                    MessageExt messageExt = list.get(0);
                    int times = messageExt.getReconsumeTimes();
                    if (times >= 3) {
                        // 走人工
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    } else {
                        // 重试
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
    }

    // 死信消息（初次消费）
    @Test
    public void testDeadConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("dead-group");
        consumer.setNamesrvAddr("192.168.26.3:9876");
        consumer.subscribe("deadTopic", "*");
        // 最多重试2次
        consumer.setMaxReconsumeTimes(2);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
                System.out.println(list);
                // 消费失败
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });
        consumer.start();
        System.in.read();
    }

    // 死信消息消费者
    @Test
    public void testDeadMq() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("dead-group");
        consumer.setNamesrvAddr("192.168.26.3:9876");
        // 死信队列默认名称%DLQ% + 消费者组名
        consumer.subscribe("%DLQ%dead-group", "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
                System.out.println(list);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();

    }
}

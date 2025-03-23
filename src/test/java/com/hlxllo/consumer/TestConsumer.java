package com.hlxllo.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import java.util.List;

/**
 * @author hlxllo
 * @description 消费者测试
 * @date 2025/3/23
 */
public class TestConsumer {
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

}

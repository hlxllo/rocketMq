package com.hlxllo.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;

/**
 * @author hlxllo
 * @description 生产者测试
 * @date 2025/3/23
 */
public class TestProducer {
    // 同步消息
    @Test
    public void testProducer() throws Exception {
        // 指定生产者
        DefaultMQProducer producer = new DefaultMQProducer("producer-group");
        // 指定nameserver
        producer.setNamesrvAddr("192.168.26.3:9876");
        // 启动实例
        producer.start();
        for (int i = 0; i < 50; i++) {
            Message message = new Message("testTopic", ("hello rocketmq" + i).getBytes());
            SendResult send = producer.send(message);
            System.out.println(send);
        }
        // 关闭实例
        producer.shutdown();
    }

    // 异步消息
    @Test
    public void testAsyncProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-group");
        producer.setNamesrvAddr("192.168.26.3:9876");
        producer.start();
        Message message = new Message("testTopic", ("异步消息").getBytes());
        producer.send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("发送成功");
            }

            @Override
            public void onException(Throwable throwable) {
                System.out.println("发送失败");
            }
        });
        System.in.read();
        producer.shutdown();
    }

    // 单向消息
    @Test
    public void testOneWayProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-group");
        producer.setNamesrvAddr("192.168.26.3:9876");
        producer.start();
        Message message = new Message("testTopic", ("单向消息").getBytes());
        producer.sendOneway(message);
        producer.shutdown();
    }
}

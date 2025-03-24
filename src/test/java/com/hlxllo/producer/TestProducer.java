package com.hlxllo.producer;

import com.hlxllo.domain.Order;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

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

    // 延迟消息
    @Test
    public void testDelayProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-group");
        producer.setNamesrvAddr("192.168.26.3:9876");
        producer.start();
        Message message = new Message("testTopic", ("延迟消息").getBytes());
        // 设定延迟等级
        message.setDelayTimeLevel(3);
        producer.sendOneway(message);
        producer.shutdown();
    }

    // 顺序消息
    @Test
    public void testOrderlyProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-group");
        producer.setNamesrvAddr("192.168.26.3:9876");
        producer.start();
        List<Order> orderList = Arrays.asList(
                new Order(1, 111, 59, new Date(), "下订单"),
                new Order(2, 111, 59, new Date(), "物流"),
                new Order(3, 111, 59, new Date(), "签收"),
                new Order(4, 112, 89, new Date(), "下订单"),
                new Order(5, 112, 89, new Date(), "物流"),
                new Order(6, 112, 89, new Date(), "拒收")
        );
        // 循环发送
        orderList.forEach(order -> {
            Message message = new Message("testTopic", order.toString().getBytes());
            try {
                producer.send(message, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                        int size = list.size();
                        int orderNumber = (int) o;
                        return list.get(orderNumber % size);
                    }
                }, order.getOrderNumber());
            } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
                throw new RuntimeException("发送失败");
            }
        });
        producer.shutdown();
    }

    // 批量生产
    @Test
    public void testBatchProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-group");
        producer.setNamesrvAddr("192.168.26.3:9876");
        producer.start();
        List<Message> messages = Arrays.asList(
                new Message("testTopic", "我是一组消息的A消息".getBytes()),
                new Message("testTopic", "我是一组消息的B消息".getBytes()),
                new Message("testTopic", "我是一组消息的C消息".getBytes())
        );
        SendResult send = producer.send(messages);
        System.out.println(send);
        producer.shutdown();
    }

    // 事务消息
    @Test
    public void testTransactionProducer() throws Exception {
        TransactionMQProducer producer = new TransactionMQProducer("producer-group");
        producer.setNamesrvAddr("192.168.26.3:9876");
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object o) {
                System.out.println(new Date());
                System.out.println("执行本地方法");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return LocalTransactionState.UNKNOW;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                System.err.println(new Date());
                System.err.println("本地方法超时或状态未确定，执行回查方法");
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
        producer.start();
        Message message = new Message("testTopic", ("事务消息").getBytes());
        producer.sendMessageInTransaction(message, null);
        System.in.read();
    }

    // 标签消息
    @Test
    public void testTagProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-group");
        producer.setNamesrvAddr("192.168.26.3:9876");
        producer.start();
        Message message = new Message("testTopic", "tagA", ("标签消息").getBytes());
        SendResult send = producer.send(message);
        System.out.println(send);
        producer.shutdown();
    }

    // 带key消息
    @Test
    public void testKeyProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-group");
        producer.setNamesrvAddr("192.168.26.3:9876");
        producer.start();
        Message msg = new Message("testTopic","tagA","key", "我是一个带标记和key的消息".getBytes());
        SendResult send = producer.send(msg);
        System.out.println(send);
        producer.shutdown();
    }

    // 生产者重试
    @Test
    public void testRetryProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer-group");
        producer.setNamesrvAddr("192.168.26.3:9876");
        producer.start();
        Message msg = new Message("testTopic", "重试消息".getBytes());
        producer.setRetryTimesWhenSendFailed(3);
        producer.send(msg);
        producer.shutdown();
    }

    // 死信消息
    @Test
    public void testDeadProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("dead-group");
        producer.setNamesrvAddr("192.168.26.3:9876");
        producer.start();
        Message message = new Message("deadTopic", "死信消息".getBytes());
        producer.sendOneway(message);
        producer.shutdown();
    }
}

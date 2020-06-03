package com.rocketmq.batch;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yangbin
 * @date 2020年05月19日
 */
public class SimpleBatchProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
        producer.setNamesrvAddr("106.52.208.123:9876");
        producer.setVipChannelEnabled(false);
        producer.start();

        //If you just send messages of no more than 1MiB at a time, it is easy to use batch
        //Messages of the same batch should have: same topic, same waitStoreMsgOK and no schedule support
        String topic = "BatchTest";
        List<Message> messages = new ArrayList<>();
        messages.add(new Message(topic, "Tag", "OrderID001", "Hello world 0".getBytes(RemotingHelper.DEFAULT_CHARSET)));
        messages.add(new Message(topic, "Tag", "OrderID002", "Hello world 1".getBytes(RemotingHelper.DEFAULT_CHARSET)));
        messages.add(new Message(topic, "Tag", "OrderID003", "Hello world 2".getBytes(RemotingHelper.DEFAULT_CHARSET)));

        producer.send(messages);
    }
}

package cn.net.hylink.server;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import java.util.Objects;

public class KafkaListenerErrorHandler implements ConsumerAwareListenerErrorHandler{
    private static final Logger logger = LoggerFactory.getLogger(ConsumerAwareListenerErrorHandler.class);
    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException e, Consumer<?, ?> consumer) {
        e.printStackTrace();
        MessageHeaders headers = message.getHeaders();
        //获取topic
        String topic = headers.get(KafkaHeaders.RECEIVED_TOPIC, String.class);
        //获取分区
        Integer partitionId = headers.get(KafkaHeaders.RECEIVED_PARTITION_ID, Integer.class);
        //获取偏移量
        Long offset = headers.get(KafkaHeaders.OFFSET, Long.class);
        if (Objects.nonNull(partitionId) && Objects.nonNull(offset)) {
            logger.error("【Kafka】消费者出错: {}, 回撤主题:{}分区:{}偏移量至:{}", e.getMessage(), topic, partitionId, offset);
            //充值偏移量
            consumer.seek(new TopicPartition(topic, partitionId), offset);
        }
        return null;
    }
}

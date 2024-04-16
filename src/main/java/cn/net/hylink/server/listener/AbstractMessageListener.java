package cn.net.hylink.server.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;

public abstract class AbstractMessageListener implements CustomMessageListener {
    private  final Logger logger = LoggerFactory.getLogger(AbstractMessageListener.class);

    @Override
    public void consumer(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        System.out.println("===============================1=====================");
           try {
               // List<String> messages = records.stream().map(ConsumerRecord::value).filter(Objects::nonNull).collect(Collectors.toList());
                for (ConsumerRecord<String, String> record : records) {
                    String topic = record.topic();
                    String value = record.value();
                    logger.info("【kafka】消费到消息 topic:{},value:{}",topic,value);
                }
            } catch (Exception e) {
                logger.info("【kafka】 消费时候出现异常: {}",e.getMessage());
                logger.error(e.getMessage(), e);
            }

        ack.acknowledge();
    }
}

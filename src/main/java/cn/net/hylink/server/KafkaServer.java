package cn.net.hylink.server;

import cn.hutool.core.util.RandomUtil;
import cn.net.hylink.server.listener.AbstractMessageListener;
import cn.net.hylink.server.listener.TestListenter;
import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

@Component
public class KafkaServer implements ApplicationContextAware {
    private final Logger logger = LoggerFactory.getLogger(KafkaServer.class);

    private final KafkaListenerErrorHandler errorHandler = new KafkaListenerErrorHandler();
    private final StringMessageConverter converter = new StringMessageConverter();
    private final MessageHandlerMethodFactory methodFactory = new DefaultMessageHandlerMethodFactory();


    /**
     * 管理监听器端点
     */
    @Autowired
    KafkaListenerEndpointRegistry registry;

    @Autowired
    KafkaListenerContainerFactory<MessageListenerContainer> factory;

    @KafkaListener(id = "endpointId-annotation",topics = "topic-annotation")
    public void annotationListener(List<ConsumerRecord<String, String>> records, Acknowledgment ack){
        logger.info("=======注解消费======");
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        logger.info("=====KafkaServer::setApplicationContext=====");
    }

    public void printContainerIds() {
        Set<String> containerIds = registry.getListenerContainerIds();
        // @KafkaListener(id = "endpointId-annotation",topics = "topic-annotation") 也会打印: endpointId-annotation
        logger.info("endpointIds :{}", JSON.toJSONString(containerIds));
    }

    /**
     * 注册但是不开始订阅
     */
    public void register(String endpointId, String topic, String groupId) {
        MethodKafkaListenerEndpoint<String, String> endpoint = new MethodKafkaListenerEndpoint<>();
        //id为容器内的id, 理解为编程式创建消费者的ID, 和kafka没什么关系
        endpoint.setId(endpointId);
        endpoint.setTopics(topic);
        TestListenter testListenter = new TestListenter();
        endpoint.setBean(testListenter);

        Method method = ReflectionUtils.findMethod(testListenter.getClass(), "consumer", List.class, Acknowledgment.class);
        if(Objects.isNull(method)) throw new RuntimeException("method must not be null");
        //消费消息时，调用的方法。 注意 参数和spring.kafka.listener.type有关
        endpoint.setMethod(method);
        //分组id
        endpoint.setGroupId(groupId);
        endpoint.setMessagingConverter(converter);
        //处理错误的handler
        endpoint.setErrorHandler(errorHandler);
        endpoint.setMessageHandlerMethodFactory(methodFactory);

        Properties properties = new Properties();
        //消费者组中消费者的ID
        properties.put("group.instance.id", groupId);
        //消费者poll的条件之一,  当满足fetch.max.wait.ms或者fetch.min.bytes的条件，就进行poll  拉取消息
        properties.put("fetch.max.wait.ms", 1000);
        endpoint.setConsumerProperties(properties);
        registry.registerListenerContainer(endpoint, factory);
        logger.info("注册端点成功: endpointId:{},groupId:{},topic:{}", endpointId, groupId, topic);
    }

    /**
     * 注销
     */
    public void unregister(String endpointId){
        registry.unregisterListenerContainer(endpointId);
    }

    /**
     * 启动订阅
     */
    public void startSubscribe(String endpointId) {
        MessageListenerContainer listenerContainer = registry.getListenerContainer(endpointId);
        if (Objects.isNull(listenerContainer)) return;

        logger.info("endpointId的状态:{}", listenerContainer.isRunning());
        if (!listenerContainer.isRunning()) {
            listenerContainer.start();
            logger.info("endpointId: {} 开始订阅", endpointId);
        }
    }

    /**
     * 停止订阅
     */
    public void stopSubscribe(String endpointId) {
        MessageListenerContainer listenerContainer = registry.getListenerContainer(endpointId);
        if (Objects.isNull(listenerContainer)) return;

        logger.info("endpointId的状态:{}", listenerContainer.isRunning());
        if (listenerContainer.isRunning()) {
            listenerContainer.stop();
            logger.info("endpointId: {} 停止订阅", endpointId);
        }
    }

}

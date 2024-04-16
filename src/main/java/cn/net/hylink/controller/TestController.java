package cn.net.hylink.controller;

import cn.net.hylink.server.KafkaServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/test")
public class TestController {

    @Autowired
    private KafkaServer kafkaServer;

    /**
     * 注册
     * @param endpointId 端的ID
     * @param topic 订阅的主题
     * @param groupId 消费者组ID
     */
    @GetMapping("/register")
    public String register(String endpointId, String topic, String groupId){
        kafkaServer.register(endpointId,topic,groupId);
        return "ok";
    }

    /**
     *  注销
     * @param endpointId 端ID
     */
    @GetMapping("/unregister")
    public String unregister(String endpointId){
        kafkaServer.unregister(endpointId);
        return "ok";
    }


    /**
     * 开始订阅
     * @param endpointId 端ID
     */
    @GetMapping("/subscribe")
    public String subscribe(String endpointId){
        kafkaServer.startSubscribe(endpointId);
        return "ok";
    }

    /**
     * 停止订阅
     * @param endpointId  端ID
     */
    @GetMapping("/stopSubscribe")
    public String stopSubscribe(String endpointId){
        kafkaServer.stopSubscribe(endpointId);
        return "ok";
    }

    /**
     * 打印注册的端ID
     */
    @GetMapping("/printContainerIds")
    public String printContainerIds(){
        kafkaServer.printContainerIds();
        return "ok";
    }
}

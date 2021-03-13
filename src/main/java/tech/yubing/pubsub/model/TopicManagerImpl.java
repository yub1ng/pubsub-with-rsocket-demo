package tech.yubing.pubsub.model;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import tech.yubing.pubsub.exception.PubSubException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Yubing
 * @date 2021/3/12
 */
@Slf4j
@Service
public class TopicManagerImpl implements TopicManager {

    /**
     * topicId, topic
     */
    private static Map<String, Topic> topicMap = new ConcurrentHashMap<>();

    @Override
    public Mono<Void> create(Topic topic) {
        return Mono.create(monoSink -> {
            if (topicMap.containsKey(topic.getId())) {
                monoSink.error(new PubSubException("[Topic already exists] | topicId = " + topic.getId()));
            } else {
                topicMap.put(topic.getId(), topic);
                monoSink.success();
            }
        });
    }

    @Override
    public Flux<String> subscribe(String topicId) {
        if (!topicMap.containsKey(topicId)) {
            return Flux.error(new PubSubException("[Topic does not exists] | topicId = " + topicId));
        }
        return topicMap.get(topicId).subscribe();
    }

    @Override
    public Mono<Void> publish(String topicId, String message) {
        if (!topicMap.containsKey(topicId)) {
            return Mono.error(new PubSubException("[Topic does not exists] | topicId = " + topicId));
        }
        Topic topic = topicMap.get(topicId);
        return topic.publish(message);
    }

    @Override
    public Mono<Void> close(String topicId) {
        if (!topicMap.containsKey(topicId)) {
            return Mono.error(new PubSubException("[Topic does not exists] | topicId = " + topicId));
        }
        Topic topic = topicMap.get(topicId);
        return topic.close();
    }
}

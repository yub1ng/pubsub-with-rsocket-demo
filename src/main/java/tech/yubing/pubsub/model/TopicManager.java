package tech.yubing.pubsub.model;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Yubing
 * @date 2021/3/12
 */
public interface TopicManager {

    Mono<Void> create(Topic topic);

    Flux<String> subscribe(String topicId);

    Mono<Void> publish(String topicId, String message);

    Mono<Void> close(String topicId);
}

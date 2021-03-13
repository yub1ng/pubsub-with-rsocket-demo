package tech.yubing.pubsub.model;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import tech.yubing.pubsub.exception.PubSubException;

/**
 * @author Yubing
 * @date 2021/3/12
 */
public class Topic {

    private String id;

    private String name;

    private Sinks.Many<String> sink;

    public Topic(String id, String name) {
        this.id = id;
        this.name = name;
        this.sink = Sinks.many().multicast().directBestEffort();
    }

    public Flux<String> subscribe() {
        return sink.asFlux();
    }

    public Mono<Void> publish(String message) {
        return Mono.create(monoSink -> {
            // push message to the downstream
            Sinks.EmitResult emitResult = sink.tryEmitNext(message);
            if (emitResult == Sinks.EmitResult.OK) {
                monoSink.success();
            } else {
                monoSink.error(new PubSubException("[Fail to emit message] | emitResult = " + emitResult));
            }
        });
    }

    public Mono<Void> close() {
        return Mono.create(monoSink -> {
            // push complete signal to the downstream
            Sinks.EmitResult emitResult = sink.tryEmitComplete();
            if (emitResult == Sinks.EmitResult.OK) {
                monoSink.success();
            } else {
                monoSink.error(new PubSubException("[Fail to close sink]"));
            }
        });
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Topic{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", sink=" + sink +
                '}';
    }
}

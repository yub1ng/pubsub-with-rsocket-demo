package tech.yubing.pubsub.bootstrap;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import lombok.extern.slf4j.Slf4j;
import tech.yubing.pubsub.model.Topic;
import tech.yubing.pubsub.model.TopicManager;
import tech.yubing.pubsub.model.TopicManagerImpl;

import java.util.concurrent.CountDownLatch;

@Slf4j
public class DemoBootstrap {

    public static void main(String[] args) throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);

        TopicManager topicManager = new TopicManagerImpl();

        // create a test topic
        String topicId = "test-topic";
        Topic topic = new Topic(topicId, "Test topic");
        topicManager.create(topic).subscribe();

        // server request handler
        SocketAcceptor acceptor = SocketAcceptor.forRequestStream(payload -> {
            log.debug("[Server receive clientId] | data = {}", payload.getDataUtf8());
            return topicManager.subscribe(topicId).map(DefaultPayload::create);
        });

        // start server
        RSocketServer.create(acceptor)
                .payloadDecoder(PayloadDecoder.ZERO_COPY)
                .bindNow(TcpServerTransport.create("localhost", 7000));

        // start client
        RSocket socket = RSocketConnector.connectWith(TcpClientTransport.create("localhost", 7000)).block();
        String clientId = "test-client";
        // request-stream model
        socket.requestStream(DefaultPayload.create(clientId))
                .map(Payload::getDataUtf8)
                .doOnNext(data -> log.debug("[Client receive data] | data = {}", clientId))
                .doFinally(o -> {
                    socket.dispose();
                    latch.countDown();
                })
                .subscribe();

        // wait client to subscribe
        Thread.sleep(1000);

        log.debug("Publish message]");
        topicManager.publish(topicId, "This is message from server").subscribe();

        log.debug("Close topic]");
        topicManager.close(topicId).subscribe();

        latch.await();
    }
}

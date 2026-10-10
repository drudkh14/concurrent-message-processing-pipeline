package com.zhbohdanchykov;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.jms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class Consumer implements Callable<Integer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private final Connection connection;
    private final String queue;
    private final MessageRouter router;

    public Consumer(Connection connection, String queue,
                    MessageRouter router) {
        this.connection = connection;
        this.queue = queue;
        this.router = router;
    }

    @Override
    public Integer call() throws JMSException {
        Session session = null;
        MessageConsumer consumer = null;
        AtomicInteger messageCount = new AtomicInteger(0);
        MessagePOJO messagePOJO = null;
        try {
            session = connection.createSession(true, Session.SESSION_TRANSACTED);
            consumer = session.createConsumer(session.createQueue(queue));
            boolean reading = true;

            TextMessage textMessage;
            String text;
            while (reading) {
                textMessage = (TextMessage) consumer.receive();
                text = textMessage.getText();
                LOGGER.debug("Message received: {}", text);
                messagePOJO = MAPPER.readValue(text, MessagePOJO.class);
                LOGGER.debug("Message serialized into MessagePOJO {}", messagePOJO);
                if (!messagePOJO.getIsPoisonPill()) {
                    messageCount.incrementAndGet();
                } else {
                    LOGGER.info("Received poison pill");
                    reading = false;
                    LOGGER.info("Messages from a consumer received: {}", messageCount);
                }
                session.commit();
                router.routeMessage(messagePOJO);
            }

            while ((textMessage = (TextMessage) consumer.receiveNoWait()).getJMSRedelivered()) {
                text = textMessage.getText();
                LOGGER.debug("Redelivered message received: {}", text);
                messagePOJO = MAPPER.readValue(text, MessagePOJO.class);
                LOGGER.debug("Message serializer into MessagePOJO {}", messagePOJO);
                if (!messagePOJO.getIsPoisonPill())
                    messageCount.incrementAndGet();
                else {
                    session.rollback();
                }
            }
        } catch (JMSException e) {
            if (session != null) {
                session.rollback();
            }
            LOGGER.error("Failed to start consumer.", e);
        } catch (JsonProcessingException e) {
            session.rollback();
            LOGGER.error("Failed to parse JSON message.", e);
        } catch (InterruptedException e) {
            session.rollback();
            LOGGER.error("Failed to put a message {} in a BlockingQueue.", messagePOJO, e);
            Thread.currentThread().interrupt();
        } finally {
            if (session != null) {
                session.close();
            }
            if (consumer != null) {
                consumer.close();
            }
        }

        return messageCount.get();
    }
}
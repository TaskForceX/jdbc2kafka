package io.taskforcex.tool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;

/**
 * Created by dgt on 3/1/17.
 */
public class ConsolePublisher implements IPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(ConsolePublisher.class);

    public ConsolePublisher() {
        LOG.info("Console publisher started.");
    }

    @Override
    public Future<?> pubAsync(String topic, String key, String msg) {
        System.out.println(String.format("topic: %s; key: %s; msg: %s", topic, key, msg));

        return null;
    }

    @Override
    public void pubSync(String topic, String key, String msg) {
        System.out.println(String.format("topic: %s; key: %s; msg: %s", topic, key, msg));
    }

    @Override
    public void close() {
        LOG.info("Console publisher ended.");
    }
}

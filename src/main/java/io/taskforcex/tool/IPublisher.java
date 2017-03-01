package io.taskforcex.tool;

import java.util.concurrent.Future;

/**
 * Created by dgt on 3/1/17.
 */
public interface IPublisher extends AutoCloseable {
    Future<?> pubAsync(String topic, String key, String msg);

    void pubSync(String topic, String key, String msg);

    void close();
}
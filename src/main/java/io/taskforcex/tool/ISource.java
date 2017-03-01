package io.taskforcex.tool;


/**
 * Created by dgt on 3/1/17.
 */
public interface ISource extends AutoCloseable {
    void init();

    void close();
}

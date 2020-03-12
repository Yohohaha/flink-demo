package me.yohohaha.demo.flink.util;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * created at 2020/02/05 11:31:18
 *
 * @author Yohohaha
 */
public class SimpleSource<T> implements ParallelSourceFunction<T> {
    private volatile boolean isRunning = true;

    private Object[] elements;
    private long interval = 1000;

    @SafeVarargs
    public SimpleSource(T fisrtElement, T... elements) {
        this.elements = new Object[elements.length + 1];
        this.elements[0] = fisrtElement;
        System.arraycopy(elements, 0, this.elements, 1, elements.length);
    }

    public SimpleSource<T> setInterval(long interval) {
        this.interval = interval;
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        for (int i = 0, len = elements.length; i < len && isRunning; i++) {
            ctx.collect((T) elements[i]);
            Thread.sleep(interval);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}

package demo;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

/**
 * 一个不做任何事情的终端 Sink。
 * 目的是让真正的瓶颈留在算子内部（例如 KeyedProcessFunction 里的 sleep），
 * 避免 Sink 本身成为干扰因素。
 */
public class DiscardSink<T> implements Sink<T> {

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) {
        return new DiscardWriter<>();
    }

    private static class DiscardWriter<T> implements SinkWriter<T> {
        @Override
        public void write(T element, Context context) {
            // 什么都不做
        }

        @Override
        public void flush(boolean endOfInput) {
            // 什么都不做
        }

        @Override
        public void close() {
            // 什么都不做
        }
    }
}

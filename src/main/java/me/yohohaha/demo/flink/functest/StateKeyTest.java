package me.yohohaha.demo.flink.functest;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.github.yohohaha.flink.FlinkStreamJob;
import org.apache.commons.lang3.StringUtils;

/**
 * created at 2020/05/08 19:05:11
 *
 * @author Yohohaha
 */
public class StateKeyTest extends FlinkStreamJob {
    public static void main(String[] args) throws Exception {
        run(new String[]{
            "-flink.runConfiguration", "test_run.properties",
            "-flink.jobName", "key test"
        });
    }

    @Override
    protected void process() {
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = env().fromElements("hello world", "hello hadoop", "hello java", "hadoop flink", "java stream")
            .flatMap(new WordSplitter())
            .map(new MapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(String value) throws Exception {
                    return Tuple2.of(value, 1);
                }
            })
            .keyBy(value -> value.f0);
        keyedStream
            .process(new LocalCounter())
            .print("局部变量处理")
        ;
        keyedStream
            .process(new KeyedStateCounter())
            .print("键值状态处理");
    }

    public static class WordSplitter implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            String[] words = StringUtils.split(value);
            for (String word : words) {
                out.collect(word);
            }
        }
    }

    public static class LocalCounter extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>> {

        // 使用局部变量，这个局部变量不会绑定到key上，只会绑定到算子实例上，而多个键值可能会分配给同一个算子实例，所以计算会出错
        private Integer count = 0;

        public LocalCounter() {
            // 算子只会实例化一次，剩下对象的都是靠序列化反序列化得到
            System.out.println("Counter实例化");
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            // System.out.println(value.f1); // 这个值永远是1
            count = value.f1 + count;
            out.collect(Tuple2.of(value.f0, count));
            System.out.println(value.f0 + ": " + this.hashCode());
        }
    }

    public static class KeyedStateCounter extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>> {

        // 使用键值状态，这个state会绑定到一个key上
        private ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 每个算子实例会调用一次open方法
            ValueStateDescriptor<Integer> countStateDesc = new ValueStateDescriptor<>(
                "count-state",
                Integer.class
            );
            System.out.println("生成一个countState");
            countState = this.getRuntimeContext().getState(countStateDesc);
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            if (countState.value() == null) {
                countState.update(0);
            }
            Integer currentCount = countState.value();
            Integer newCount = currentCount + value.f1;
            countState.update(newCount);
            out.collect(Tuple2.of(value.f0, newCount));
            // countState是每个算子实例一个，但是实际的值会绑定到一个key上，具体实现见org.apache.flink.runtime.state.heap.StateTable
            System.out.println(value.f0 + ": " + this.hashCode() + ", " + countState.hashCode());
        }
    }
}

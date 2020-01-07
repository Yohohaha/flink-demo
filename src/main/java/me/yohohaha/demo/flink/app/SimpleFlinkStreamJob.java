package me.yohohaha.demo.flink.app;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * created at 2020/01/07 15:27:41
 *
 * @author Yohohaha
 */
public class SimpleFlinkStreamJob extends FlinkStreamJob {
    public static void main(String[] args) throws Exception {
        run(args);
    }

    @Override
    protected void process() {
        env().fromElements("hello world", "world big", "one world", "you hello")
            .flatMap(new StringTokenizer())
            .map(new StringToTuple()).returns(new TypeHint<Tuple2<String, Integer>>() {
        })
            .keyBy(value -> value.f0)
            .sum(1).print()
        ;
    }

    static class StringToTuple implements MapFunction<String, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(String value) throws Exception {
            return Tuple2.of(value, 1);
        }
    }

    static class StringTokenizer implements FlatMapFunction<String, String> {

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            for (String word : value.split(" ")) {
                out.collect(word);
            }
        }
    }
}



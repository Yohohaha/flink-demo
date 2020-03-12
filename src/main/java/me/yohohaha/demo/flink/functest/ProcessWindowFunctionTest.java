package me.yohohaha.demo.flink.functest;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import com.github.yohohaha.flink.FlinkStreamJob;
import com.github.yohohaha.java.util.TimeUtils;
import me.yohohaha.demo.flink.util.SimpleSource;

import javax.annotation.Nullable;

/**
 * created at 2020/02/04 18:44:33
 *
 * @author Yohohaha
 */
public class ProcessWindowFunctionTest extends FlinkStreamJob {
    public static void main(String[] args) throws Exception {
        run(args);
    }

    @Override
    protected void process() {
        env().setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env().setParallelism(1);
        env().addSource(new SimpleSource<>(
            Tuple2.of(1L, 10),
            Tuple2.of(2L, 15),
            Tuple2.of(3L, 20),
            Tuple2.of(4L, 20),
            Tuple2.of(2L, 20),
            Tuple2.of(5L, 20),
            Tuple2.of(6L, 20),
            Tuple2.of(6L, 20),
            Tuple2.of(4L, 20),
            Tuple2.of(7L, 20),
            Tuple2.of(8L, 20),
            Tuple2.of(9L, 20)
        ))
            .returns(new TypeHint<Tuple2<Long, Integer>>() {
            })
            .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Long, Integer>>() {
                @Override
                public long extractTimestamp(Tuple2<Long, Integer> element, long previousElementTimestamp) {
                    return element.f0;
                }

                @Nullable
                @Override
                public Watermark checkAndGetNextWatermark(Tuple2<Long, Integer> lastElement, long extractedTimestamp) {
                    System.out.println(extractedTimestamp);
                    return new Watermark(extractedTimestamp);
                }
            })
            .keyBy(v -> "key")
            .timeWindow(Time.milliseconds(3))
            .trigger(MyCountTrigger.of(1))
            .process(new ProcessWindowFunction<Tuple2<Long, Integer>, Object, String, TimeWindow>() {
                @Override
                public void process(String key, Context context, Iterable<Tuple2<Long, Integer>> elements, Collector<Object> out) throws Exception {
                    System.out.println("group start");
                    for (Tuple2<Long, Integer> element : elements) {
                        out.collect(Tuple6.of(key, element, context.currentWatermark(), context.window().getStart(), context.window().getEnd(), TimeUtils.getStringOfTimestamp(context.currentProcessingTime(), "yyyy-MM-dd HH:mm:ss.SSS")));
                    }
                }
            }).print("result");
    }

    private static class MyCountTrigger<W extends Window> extends Trigger<Object, W> {
        private static final long serialVersionUID = 1L;

        private final long maxCount;

        private final ReducingStateDescriptor<Long> stateDesc =
            new ReducingStateDescriptor<>("count", new MyCountTrigger.Sum(), LongSerializer.INSTANCE);

        private MyCountTrigger(long maxCount) {
            this.maxCount = maxCount;
        }

        @Override
        public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
            ReducingState<Long> count = ctx.getPartitionedState(stateDesc);
            count.add(1L);
            if (count.get() >= maxCount) {
                count.clear();
                return TriggerResult.FIRE;
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, W window, TriggerContext ctx) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(W window, TriggerContext ctx) throws Exception {
            System.out.println("window clear at " + ctx.getCurrentWatermark());
            ctx.getPartitionedState(stateDesc).clear();
        }

        @Override
        public boolean canMerge() {
            return true;
        }

        @Override
        public void onMerge(W window, OnMergeContext ctx) throws Exception {
            ctx.mergePartitionedState(stateDesc);
        }

        @Override
        public String toString() {
            return "CountTrigger(" + maxCount + ")";
        }

        /**
         * Creates a trigger that fires once the number of elements in a pane reaches the given count.
         *
         * @param maxCount The count of elements at which to fire.
         * @param <W>      The type of {@link Window Windows} on which this trigger can operate.
         */
        public static <W extends Window> MyCountTrigger<W> of(long maxCount) {
            return new MyCountTrigger<>(maxCount);
        }

        private static class Sum implements ReduceFunction<Long> {
            private static final long serialVersionUID = 1L;

            @Override
            public Long reduce(Long value1, Long value2) throws Exception {
                return value1 + value2;
            }

        }
    }
}

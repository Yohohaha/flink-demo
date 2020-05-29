package me.yohohaha.demo.flink.functest;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSONObject;
import com.github.yohohaha.flink.FlinkStreamJob;

/**
 * created at 2020/05/09 14:52:30
 *
 * @author Yohohaha
 */
public class RecordDeduplicateFunctionTest extends FlinkStreamJob {
    public static void main(String[] args) throws Exception {
        run(new String[]{
            "-flink.runConfiguration", "test_run.properties",
            "-flink.jobName", "RecordDeduplicateFunctionTest"
        });
    }

    @Override
    protected void process() {
        env()
            .fromElements(1, 2, 3, 3)
            .map(new MapFunction<Integer, JSONObject>() {
                @Override
                public JSONObject map(Integer value) throws Exception {
                    return new JSONObject().fluentPut("KEY", value);
                }
            })
            .keyBy(new KeySelector<JSONObject, JSONObject>() {
                @Override
                public JSONObject getKey(JSONObject value) throws Exception {
                    return value;
                }
            })
            .process(new DeduplicateProcessFunction<>(1000L * 3))
            .print();

    }

    public static class DeduplicateProcessFunction<T> extends KeyedProcessFunction<T, T, T> {

        private final long messageSaveTimeout;

        public DeduplicateProcessFunction(long messageSaveTimeout) {
            this.messageSaveTimeout = messageSaveTimeout;
        }


        private ValueState<Boolean> existState;

        @Override
        public void open(Configuration parameters) throws Exception {
            StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.milliseconds(messageSaveTimeout))
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .cleanupInRocksdbCompactFilter(1000 * 100)
                .build();
            ValueStateDescriptor<Boolean> existStateDesc = new ValueStateDescriptor<>(
                "record-dedup-state",
                Boolean.class
            );
            existStateDesc.enableTimeToLive(stateTtlConfig);

            existState = this.getRuntimeContext().getState(existStateDesc);
        }

        @Override
        public void processElement(T value, Context ctx, Collector<T> out) throws Exception {
            if (existState.value() == null) {
                existState.update(true);
                out.collect(value);
            } else {
                System.out.println("被去重掉: " + ctx.getCurrentKey());
            }
        }
    }

}
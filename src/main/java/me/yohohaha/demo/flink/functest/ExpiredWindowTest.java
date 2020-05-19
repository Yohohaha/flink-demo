package me.yohohaha.demo.flink.functest;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import com.github.yohohaha.flink.FlinkStreamJob;
import me.yohohaha.demo.flink.util.RandomContinuousProcessingTimeTrigger;

import java.util.Properties;

/**
 * created at 2020/05/15 15:13:44
 *
 * @author Yohohaha
 */
public class ExpiredWindowTest extends FlinkStreamJob {
    public static void main(String[] args) throws Exception {
        run(new String[]{
            "-flink.runConfiguration", "test_run.properties",
            "-flink.jobName", "ExpiredWindowTest"
        });
    }

    @Override
    protected void process() {
        // 测试watermark超过窗口最大时间后会不会被触发：会
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("flink-test", new SimpleStringSchema(), props);
        kafkaConsumer.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String element) {
                return Long.parseLong(element.split(",")[0]);
            }
        });
        env().setParallelism(3);
        env().setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env()
            .addSource(kafkaConsumer)
            .map(new MapFunction<String, String>() {
                @Override
                public String map(String value) throws Exception {
                    return value.split(",")[1];
                }
            })
            .keyBy(new KeySelector<String, String>() {
                @Override
                public String getKey(String value) throws Exception {
                    return value;
                }
            })
            .timeWindow(Time.milliseconds(3))
            .trigger(RandomContinuousProcessingTimeTrigger.of(Time.seconds(3)))
            .allowedLateness(Time.milliseconds(1))
            .aggregate(new AggregateFunction<String, Long, Long>() {
                @Override
                public Long createAccumulator() {
                    return 0L;
                }

                @Override
                public Long add(String value, Long accumulator) {
                    return accumulator + 1L;
                }

                @Override
                public Long getResult(Long accumulator) {
                    return accumulator;
                }

                @Override
                public Long merge(Long a, Long b) {
                    return a + b;
                }
            }, new ProcessWindowFunction<Long, String, String, TimeWindow>() {
                @Override
                public void process(String key, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
                    Long ele = elements.iterator().next();
                    long watermark = context.currentWatermark();
                    String info = "key: " + key +
                        ", ele: " + ele +
                        ", watermark: " + watermark +
                        ", window: [" + context.window().getStart() + ", " + (context.window().getEnd() - 1) + "]";
                    out.collect(info);
                }
            })
            .print()

        ;
    }
}
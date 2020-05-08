package me.yohohaha.demo.flink.functest;

import org.apache.flink.api.common.functions.RichMapFunction;

import com.github.yohohaha.flink.FlinkStreamJob;

/**
 * created at 2020/03/03 18:48:48
 *
 * @author Yohohaha
 */
public class NoSinkTest extends FlinkStreamJob {
    public static void main(String[] args) throws Exception {
        run(new String[]{
            "-flink.runConfiguration", "test_run.properties",
            "-flink.jobName", "noSinkTest"
        });
    }

    @Override
    protected void process() {
        // source后面必须跟其他算子，sink算子非必要
        env().fromElements(1, 2, 3)
            .map(new RichMapFunction<Integer, Object>() {
                @Override
                public Object map(Integer value) throws Exception {
                    System.out.println(getRuntimeContext().getIndexOfThisSubtask() + ": " + value);
                    return null;
                }
            });
    }
}

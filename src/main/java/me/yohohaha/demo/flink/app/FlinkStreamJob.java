package me.yohohaha.demo.flink.app;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import me.yohohaha.demo.flink.util.SimpleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Properties;

import static me.yohohaha.demo.flink.app.FlinkJobOptions.FLINK_JOB_NAME;
import static me.yohohaha.demo.flink.app.FlinkJobOptions.RUN_CONFIGURATION;

/**
 * created at 2020/01/07 14:24:37
 *
 * @author Yohohaha
 */
public abstract class FlinkStreamJob {
    private static final Logger log = LoggerFactory.getLogger(FlinkStreamJob.class);

    static void run(String[] args) throws Exception {
        Class<? extends FlinkStreamJob> c = SimpleUtils.getInvocationClass();
        Constructor<? extends FlinkStreamJob> constructor = c.getConstructor();
        FlinkStreamJob flinkStreamJob = constructor.newInstance();
        flinkStreamJob.init(args);
        flinkStreamJob.process();
        flinkStreamJob.finish();
    }

    private ParameterTool paras;
    private StreamExecutionEnvironment env;

    final protected void init(String[] args) {
        ParameterTool argParas = ParameterTool.fromArgs(args);
        String runConfigurationPath = argParas.getRequired(RUN_CONFIGURATION.key());
        log.info("runConfigurationPath={}", runConfigurationPath);
        Properties runProperties;
        try {
            runProperties = SimpleUtils.readProperties(runConfigurationPath);
        } catch (IOException e) {
            log.error("exception occured when reading `run.configuration`", e);
            runProperties = new Properties();
        }
        paras = ParameterTool.fromMap((Map) runProperties).mergeWith(argParas);

        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    abstract protected void process();

    final protected void finish() throws Exception {
        env.execute(flinkJobName());
    }

    protected ParameterTool paras() {
        return paras;
    }

    protected String flinkJobName() {
        return paras.getRequired(FLINK_JOB_NAME.key());
    }

    protected StreamExecutionEnvironment env() {
        return env;
    }
}

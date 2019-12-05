package me.yohohaha.demo.flink.util;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Properties;

/**
 * created at 2019/07/08 11:24:24
 *
 * @author Yohohaha
 */
public class SimpleUtils {
    /**
     * 获取当前类名
     *
     * @return
     */
    public static String getCurrentClassName() {
        return Thread.currentThread().getStackTrace()[1].getClassName();
    }

    /**
     * 获取调用该方法的类名
     *
     * @return
     */
    public static String getInvocationClassName() {
        return Thread.currentThread().getStackTrace()[3].getClassName();
    }

    /**
     * 获取调用该方法的类
     *
     * @return
     */
    public static Class getInvocationClass() throws ClassNotFoundException {
        return Class.forName(Thread.currentThread().getStackTrace()[3].getClassName());
    }

    /**
     * 获取异常信息
     *
     * @param e
     *
     * @return
     */
    public static String getExceptionInfo(Throwable e) {
        Writer w = new StringWriter();
        e.printStackTrace(new PrintWriter(w));
        return w.toString();
    }


    /**
     * 读取资源文件夹下的文件内容，并返回字符串
     *
     * @param in
     *
     * @return
     *
     * @throws IOException
     */
    public static String readResourceContent(InputStream in) throws IOException {
        return IOUtils.toString(in, StandardCharsets.UTF_8);
    }

    /**
     * 读取资源文件夹下的文件内容，并返回字符串
     *
     * @param inPath
     *
     * @return
     *
     * @throws IOException
     */
    public static String readResourceContent(String inPath) throws IOException {
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(inPath)) {
            if (in == null) {
                return null;
            }
            return IOUtils.toString(in, StandardCharsets.UTF_8);
        }
    }


    public static Properties readProperties(String inPath) throws IOException {
        final Properties properties = new Properties();
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(inPath)) {
            properties.load(in);
        }
        return properties;
    }

    public static String getAbsoulutePathFromResourcePath(String resourcePath) {
        return Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource(resourcePath)).getPath();
    }

}

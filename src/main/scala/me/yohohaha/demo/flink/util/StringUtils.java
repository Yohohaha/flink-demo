package me.yohohaha.demo.flink.util;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;

/**
 * created at 2019/12/05 15:52:14
 *
 * @author Yohohaha
 */
public class StringUtils {
    public static ConcurrentHashMap<String, Field> CLASS_FIELD_MAP = new ConcurrentHashMap<>();

    /**
     * @param origin 未格式化的字符串
     * @param pojo   目标对象
     *
     * @return 格式化后的字符串
     */
    @Nullable
    public static String interpolation(@Nullable String origin, @Nullable Object pojo) {
        if (pojo == null) {
            return origin;
        }
        if (origin == null) {
            return null;
        }
        Class<?> cls = pojo.getClass();
        String clsName = cls.getTypeName();
        int len = origin.length();
        StringBuilder builder = new StringBuilder(len);
        int lastStrEndIdx = 0;
        int i = 0;
        while (i + 1 < len) {
            String substr = origin.substring(i, i + 2);
            if ("${".equals(substr)) {
                builder.append(origin, lastStrEndIdx, i);
                int varNameStartIdx = i + 2;
                int varNameIdx = varNameStartIdx;
                while (varNameIdx < len) {
                    String subVarNameStr = origin.substring(varNameIdx, varNameIdx + 1);
                    if ("}".equals(subVarNameStr)) {
                        break;
                    }
                    varNameIdx++;
                }
                String varName = origin.substring(varNameStartIdx, varNameIdx);
                i = varNameIdx + 1;
                String fieldKey = clsName + "." + varName;
                Field field = CLASS_FIELD_MAP.computeIfAbsent(fieldKey, s -> {
                    try {
                        return cls.getDeclaredField(varName);
                    } catch (NoSuchFieldException e) {
                        throw new RuntimeException("no such field, class=" + clsName + ", name=" + varName, e);
                    }
                });
                try {
                    builder.append(field.get(pojo));
                    lastStrEndIdx = i;
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("can't access field, name = " + varName);
                }

            } else {
                i += 1;
            }
        }
        builder.append(origin, lastStrEndIdx, len);
        return builder.toString();
    }
}

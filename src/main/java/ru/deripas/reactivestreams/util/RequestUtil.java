package ru.deripas.reactivestreams.util;

import lombok.experimental.UtilityClass;

import java.util.concurrent.atomic.AtomicLong;

@UtilityClass
public class RequestUtil {

    public static void safeAdd(AtomicLong number, long delta) {
        number.updateAndGet(value -> {
            if (value == Long.MAX_VALUE) {
                return Long.MAX_VALUE;
            }
            long r = value + delta;
            if (value > 0 && r < 0) {
                return Long.MAX_VALUE;
            }
            return r;
        });
    }
}

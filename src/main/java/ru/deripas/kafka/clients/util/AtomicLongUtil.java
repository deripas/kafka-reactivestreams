package ru.deripas.kafka.clients.util;

import lombok.experimental.UtilityClass;

import java.util.concurrent.atomic.AtomicLong;

@UtilityClass
public class AtomicLongUtil {

    public static void safeAdd(AtomicLong number, long delta) {
        number.updateAndGet(value -> {
            if (value == Long.MAX_VALUE) {
                return Long.MAX_VALUE;
            }
            long r = value + delta;
            if (r <= 0) {
                return Long.MAX_VALUE;
            }
            return r;
        });
    }
}

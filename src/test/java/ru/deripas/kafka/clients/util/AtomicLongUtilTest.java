package ru.deripas.kafka.clients.util;

import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.assertEquals;


public class AtomicLongUtilTest {

    @Test
    public void test0() {
        AtomicLong number = new AtomicLong();
        AtomicLongUtil.safeAdd(number, 10);
        assertEquals(number.get(), 10);
    }

    @Test
    public void test1() {
        AtomicLong number = new AtomicLong(Long.MAX_VALUE - 10);
        AtomicLongUtil.safeAdd(number, 10);
        assertEquals(number.get(), Long.MAX_VALUE);
    }

    @Test
    public void test2() {
        AtomicLong number = new AtomicLong(Long.MAX_VALUE - 10);
        AtomicLongUtil.safeAdd(number, 10);
        AtomicLongUtil.safeAdd(number, 10);
        assertEquals(number.get(), Long.MAX_VALUE);
    }

    @Test
    public void test3() {
        AtomicLong number = new AtomicLong(Long.MAX_VALUE);
        AtomicLongUtil.safeAdd(number, 10);
        assertEquals(number.get(), Long.MAX_VALUE);
    }
}

package com.github.dao.reactivestreams.util;

import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.assertEquals;


public class RequestUtilTest {

    @Test
    public void test0() {
        AtomicLong number = new AtomicLong();
        RequestUtil.safeAdd(number, 10);
        assertEquals(number.get(), 10);
    }

    @Test
    public void test1() {
        AtomicLong number = new AtomicLong(Long.MAX_VALUE - 10);
        RequestUtil.safeAdd(number, 10);
        assertEquals(number.get(), Long.MAX_VALUE);
    }

    @Test
    public void test2() {
        AtomicLong number = new AtomicLong(Long.MAX_VALUE - 10);
        RequestUtil.safeAdd(number, 10);
        RequestUtil.safeAdd(number, 10);
        assertEquals(number.get(), Long.MAX_VALUE);
    }

    @Test
    public void test3() {
        AtomicLong number = new AtomicLong(Long.MAX_VALUE);
        RequestUtil.safeAdd(number, 10);
        assertEquals(number.get(), Long.MAX_VALUE);
    }

    @Test
    public void test4() {
        AtomicLong number = new AtomicLong(-10);
        RequestUtil.safeAdd(number, 10);
        assertEquals(number.get(), 0);
    }

    @Test
    public void test5() {
        AtomicLong number = new AtomicLong(Long.MIN_VALUE);
        RequestUtil.safeAdd(number, 10);
        assertEquals(number.get(), Long.MIN_VALUE + 10);
    }

    @Test
    public void test6() {
        AtomicLong number = new AtomicLong(0);
        RequestUtil.safeAdd(number, -10);
        assertEquals(number.get(), -10);
    }

    @Test
    public void test7() {
        AtomicLong number = new AtomicLong(Long.MAX_VALUE);
        RequestUtil.safeAdd(number, -10);
        assertEquals(number.get(), Long.MAX_VALUE);
    }

    @Test
    public void test8() {
        AtomicLong number = new AtomicLong(1);
        RequestUtil.safeAdd(number, -1);
        assertEquals(number.get(), 0);
    }
}

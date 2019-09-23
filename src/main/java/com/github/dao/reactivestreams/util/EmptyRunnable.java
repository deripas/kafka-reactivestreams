package com.github.dao.reactivestreams.util;

public class EmptyRunnable implements Runnable {

    public static final Runnable INSTANCE = new EmptyRunnable();

    @Override
    public void run() {
        // no action
    }

    @Override
    public String toString() {
        return "EmptyRunnable";
    }
}

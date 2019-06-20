package org.apache.ignite.thread;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class FastThreadExecutor  extends ThreadPoolExecutor {
    public FastThreadExecutor(int size, String name)
    {
        super(size, size, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new IgniteThreadFactory("instance", name));
    }
}

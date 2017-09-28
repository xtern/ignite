package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class GridSemaphoreCloseFailedRepro extends GridCommonAbstractTest {

    public void testBrokenSemaphore() throws Exception {

        final AtomicBoolean failed = new AtomicBoolean();

        try (Ignite node = startGrid(0)) {

            for (; ; ) {

                final AtomicReference<IgniteSemaphore> ref = new AtomicReference<>();

                Thread t = new Thread(new Runnable() {
                    @Override public void run() {
                        IgniteSemaphore semaphore = node.semaphore("test", 1, true, true);
                        try {
                            ref.set(semaphore);

                            while (semaphore.availablePermits() > 0)
                                ;

                            semaphore.acquire(1);
                        }
                        catch (IgniteInterruptedException ignore) {
                            // ignore - normal behavior interrupted state in most cases was cleared
                        }
                        finally {
                            try {
                                semaphore.close();
                            }
                            catch (IgniteInterruptedException e) {
                                e.printStackTrace();
                                // test failed
                                failed.set(true);
                            }
                        }
                    }

                });

                t.start();

                IgniteSemaphore semaphore;

                while ((semaphore = ref.get()) == null)
                    ;

                semaphore.acquire(1);

                while (!semaphore.hasQueuedThreads())
                    ;

                t.interrupt();

                t.join();

                if (failed.get())
                    fail("Failed to close semaphore");
            }
        }
    }

}

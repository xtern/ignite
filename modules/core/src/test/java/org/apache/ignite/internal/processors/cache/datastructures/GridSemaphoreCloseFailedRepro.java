package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class GridSemaphoreCloseFailedRepro extends GridCommonAbstractTest {

    public void testBrokenSemaphore() throws Exception {

        final boolean[] success = new boolean[1];

        try (Ignite node = startGrid(0)) {

            for ( ; ; ) {
                final AtomicReference<IgniteSemaphore> ref = new AtomicReference<>();

                success[0] = false;

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
                            // Ignore - normal behavior.
                        }
                        finally {
                            semaphore.close();
                            // Here expected exception when thread interrupted state is set.
                            success[0] = true;
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

                if (!success[0])
                    fail("Failed to close semaphore");
            }
        }
    }

}

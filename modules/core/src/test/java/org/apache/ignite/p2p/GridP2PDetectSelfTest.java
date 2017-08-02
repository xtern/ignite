package org.apache.ignite.p2p;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test P2P deployment task which was loaded with external classloader and was wrapped by class loaded with current
 */
public class GridP2PDetectSelfTest extends GridCommonAbstractTest {
    /** fully qualified name of java class that will be loaded by external classloader. */
    private static final String EXT_REMOTE_TASK_CLASS = "org.apache.ignite.tests.p2p.GridP2PSimpleDeploymentTask";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        return super.getConfiguration(name)
            .setPeerClassLoadingEnabled(true)
            .setClientMode(!isFirstGrid(name));
    }

    /**
     * Test deserialization of object with nested object loaded with different classloader.
     *
     * @throws Exception if error occur.
     */
    @SuppressWarnings("unchecked")
    public void testNestedObjectsClassloader() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-3935");

        startGrid(0);

        try (Ignite client = startGrid(1)) {
            URL[] classpath = {new URL(GridTestProperties.getProperty("p2p.uri.cls"))};
            GridTestExternalClassLoader ldr = new GridTestExternalClassLoader(classpath);

            Class<IgniteCallable<Integer>> extCls = (Class<IgniteCallable<Integer>>)ldr.loadClass(EXT_REMOTE_TASK_CLASS);
            IgniteCallable<Integer> external = extCls.newInstance();

            // wrapping with callable with current classloader
            IgniteCallable<Integer> current = new CurrentCallable<>(external);
            // starting remote task - serializer must detect classloader for each nested object
            Integer result = client.compute().call(current);

            assert result == 1;
        }
        catch (MalformedURLException | ClassNotFoundException e) {
            throw new RuntimeException("Properly define property p2p.uri.cls", e);
        }
        finally {
            stopGrid(0);
        }
    }

    /**
     * Wraps callable to confuse classloader detection.
     *
     * @param <T> result type.
     */
    private static class CurrentCallable<T> implements IgniteCallable<T> {

        private final IgniteCallable<T> parent;

        /**
         * @param parent callable
         */
        private CurrentCallable(IgniteCallable<T> parent) {
            this.parent = parent;
        }

        /** {@inheritDoc} */
        @Override public T call() throws Exception {
            return parent.call();
        }
    }
}
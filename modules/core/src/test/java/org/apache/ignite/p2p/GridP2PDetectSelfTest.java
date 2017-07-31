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

    /** Configuration switch to client mode. Used in {@link #getConfiguration(String)}. */
    private boolean clientMode = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        return super.getConfiguration(name)
            .setPeerClassLoadingEnabled(true)
            .setClientMode(clientMode);
    }

    /**
     * Test deserialization of object with nested object loaded with different classloader.
     *
     * @throws Exception if error occur.
     */
    @SuppressWarnings("unchecked")
    public void testNestedObjectsClassloader() throws Exception {
        startGrid(0);
        clientMode = true;
        Ignite client = startGrid(1);
        try {
            URL[] classpath = {new URL(GridTestProperties.getProperty("p2p.uri.cls"))};
            GridTestExternalClassLoader ldr = new GridTestExternalClassLoader(classpath);
            Class<IgniteCallable<Integer>> extCls = (Class<IgniteCallable<Integer>>)ldr.loadClass(EXT_REMOTE_TASK_CLASS);
            IgniteCallable<Integer> externalCallable = extCls.newInstance();
            // wrapping with callable with current classloader
            IgniteCallable<Integer> currentCallable = new CurrentCallable<>(externalCallable);
            // starting remote task - serializer must detect classloader for each nested object
            Integer result = client.compute().call(currentCallable);
            assert result == 1;
        }
        catch (MalformedURLException | ClassNotFoundException e) {
            throw new RuntimeException("Properly define property p2p.uri.cls or recompile module ignite-extdata-p2p", e);
        }
        finally {
            stopGrid(1);
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
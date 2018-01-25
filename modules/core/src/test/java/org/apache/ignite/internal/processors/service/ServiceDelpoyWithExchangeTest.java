package org.apache.ignite.internal.processors.service;

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.ServiceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class ServiceDelpoyWithExchangeTest extends GridCommonAbstractTest {
    /** */
    private static final String SVC_NAME = "testService";

    /** */
    private static final long MAX_EXCHANGE_AWAIT = 10_000L;

    /** */
    private static final int NODES_DIFF = 2;

    /** */
    private static final int MAX_RETRY = 10;

    /** */
    @Override public void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** */
    public void testServiceDeploy() throws Exception {
        IgniteEx crd = startGrid(0);;

        crd.services(crd.cluster()).deployNodeSingleton(SVC_NAME, new TestServiceImpl());

        int retry = MAX_RETRY;

        int startIdx = 1;

        GridDhtPartitionsExchangeFuture fut;

        do {
            startGridsMultiThreaded(startIdx, NODES_DIFF);

            fut = crd.context().cache().context().exchange().lastTopologyFuture();

            assert fut.context().mergeExchanges() : "Test should be run with merge exchanges support.";

            assert GridTestUtils.waitForCondition(fut::isDone, MAX_EXCHANGE_AWAIT);

            startIdx += NODES_DIFF;
        }
        while (fut.initialVersion().equals(fut.topologyVersion()) && retry-- > 0);

        assert retry >= 0 : "Unable to simulate exchange merge in " + MAX_RETRY + " attempts.";

        crd.compute().run(new IgniteRunnable() {
            @ServiceResource(serviceName = SVC_NAME)
            private TestService svc;

            @Override public void run() {
                assert svc != null : "Service reassignment failed after last partition exchange.";
            }
        });
    }

    /** */
    public interface TestService {
        /** */
        int call();
    }

    /** */
    public static class TestServiceImpl implements Service, TestService {
        /** {@inheritDoc} */
        @Override public int call() {
            return 1;
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op.
        }
    }
}

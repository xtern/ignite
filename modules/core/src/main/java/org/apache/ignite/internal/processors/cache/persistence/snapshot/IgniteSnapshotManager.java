/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.MarshallerMappingWriter;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.communication.TransmissionCancelledException;
import org.apache.ignite.internal.managers.communication.TransmissionHandler;
import org.apache.ignite.internal.managers.communication.TransmissionMeta;
import org.apache.ignite.internal.managers.communication.TransmissionPolicy;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.store.PageWriteListener;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PagesAllocationRange;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PartitionAllocationMap;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.cacheobject.BinaryTypeWriter;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.GridBusyLock;
import org.apache.ignite.internal.util.GridIntIterator;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static java.nio.file.StandardOpenOption.READ;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.IgniteFeatures.PERSISTENCE_CACHE_SNAPSHOT;
import static org.apache.ignite.internal.IgniteFeatures.nodeSupports;
import static org.apache.ignite.internal.MarshallerContextImpl.addPlatformMappings;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.MAX_PARTITION_ID;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFile;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFileName;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor.DB_DEFAULT_FOLDER;
import static org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId.getFlagByPartId;

/** */
public class IgniteSnapshotManager extends GridCacheSharedManagerAdapter {
    /** File with delta pages suffix. */
    public static final String DELTA_SUFFIX = ".delta";

    /** File name template consists of delta pages. */
    public static final String PART_DELTA_TEMPLATE = PART_FILE_TEMPLATE + DELTA_SUFFIX;

    /** File name template for index delta pages. */
    public static final String INDEX_DELTA_NAME = INDEX_FILE_NAME + DELTA_SUFFIX;

    /** The reason of checkpoint start for needs of snapshot. */
    public static final String SNAPSHOT_CP_REASON = "Wakeup for checkpoint to take snapshot [name=%s]";

    /** Default working directory for snapshot temporary files. */
    public static final String DFLT_LOCAL_SNAPSHOT_DIRECTORY = "snapshots";

    /** Default snapshot directory for loading remote snapshots. */
    public static final String DFLT_SNAPSHOT_WORK_DIRECTORY = "snp";

    /** Prefix for snapshot threads. */
    private static final String SNAPSHOT_RUNNER_THREAD_PREFIX = "snapshot-runner";

    /** Total number of thread to perform local snapshot. */
    private static final int SNAPSHOT_THREAD_POOL_SIZE = 4;

    /** Timeout in milliseconsd to wait while a previous requested snapshot completed. */
    private static final long DFLT_CREATE_SNAPSHOT_TIMEOUT = 15_000L;

    /** Default snapshot topic to receive snapshots from remote node. */
    private static final Object DFLT_INITIAL_SNAPSHOT_TOPIC = GridTopic.TOPIC_SNAPSHOT.topic("rmt_snp");

    /** File transmission parameter of cache group id. */
    private static final String SNP_GRP_ID_PARAM = "grpId";

    /** File transmission parameter of cache partition id. */
    private static final String SNP_PART_ID_PARAM = "partId";

    /** File transmission parameter of node-sender directory path with its consistentId (e.g. db/IgniteNode0). */
    private static final String SNP_DB_NODE_PATH_PARAM = "dbNodePath";

    /** File transmission parameter of a cache directory with is currently sends its partitions. */
    private static final String SNP_CACHE_DIR_NAME_PARAM = "cacheDirName";

    /** Snapshot parameter name for a file transmission. */
    private static final String SNP_NAME_PARAM = "snpName";

    /** Map of registered cache snapshot processes and their corresponding contexts. */
    private final ConcurrentMap<String, LocalSnapshotContext> locSnpCtxs = new ConcurrentHashMap<>();

    /** Lock to protect the resources is used. */
    private final GridBusyLock busyLock = new GridBusyLock();

    /** Requested snapshot from remote node. */
    private final AtomicReference<SnapshotRequestFuture> snpReq = new AtomicReference<>();

    /** Map of requests from remote node on snapshot creation. */
    private final ConcurrentMap<UUID, IgniteInternalFuture<Boolean>> rmtSnps = new ConcurrentHashMap<>();

    /** Main snapshot directory to save created snapshots. */
    private File locSnpDir;

    /**
     * Working directory for loaded snapshots from the remote nodes and storing
     * temporary partition delta-files of locally started snapshot process.
     */
    private File tmpWorkDir;

    /** Factory to working with delta as file storage. */
    private volatile FileIOFactory ioFactory = new RandomAccessFileIOFactory();

    /** Factory to create page store for restore. */
    private volatile BiFunction<Integer, Boolean, FilePageStoreFactory> storeFactory;

    /** Snapshot thread pool to perform local partition snapshots. */
    private ExecutorService snpRunner;

    /** Checkpoint listener to handle scheduled snapshot requests. */
    private DbCheckpointListener cpLsnr;

    /** System discovery message listener. */
    private DiscoveryEventListener discoLsnr;

    /** Snapshot listener on created snapshots. */
    private volatile SnapshotListener snpLsnr;

    /** Database manager for enabled persistence. */
    private GridCacheDatabaseSharedManager dbMgr;

    /** Configured data storage page size. */
    private int pageSize;

    /**
     * @param ctx Kernal context.
     */
    public IgniteSnapshotManager(GridKernalContext ctx) {
        // No-op.
    }

    /**
     * @param snapshotCacheDir Snapshot directory to store files.
     * @param partId Cache partition identifier.
     * @return A file representation.
     */
    private static File getPartionDeltaFile(File snapshotCacheDir, int partId) {
        return new File(snapshotCacheDir, getPartitionDeltaFileName(partId));
    }

    /**
     * @param partId Partitoin id.
     * @return File name of delta partition pages.
     */
    public static String getPartitionDeltaFileName(int partId) {
        assert partId <= MAX_PARTITION_ID || partId == INDEX_PARTITION;

        return partId == INDEX_PARTITION ? INDEX_DELTA_NAME : String.format(PART_DELTA_TEMPLATE, partId);
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        GridKernalContext kctx = cctx.kernalContext();

        if (kctx.clientNode())
            return;

        if (!CU.isPersistenceEnabled(cctx.kernalContext().config()))
            return;

        pageSize = kctx.config()
            .getDataStorageConfiguration()
            .getPageSize();

        assert pageSize > 0;

        snpRunner = new IgniteThreadPoolExecutor(
            SNAPSHOT_RUNNER_THREAD_PREFIX,
            cctx.igniteInstanceName(),
            SNAPSHOT_THREAD_POOL_SIZE,
            SNAPSHOT_THREAD_POOL_SIZE,
            30_000,
            new LinkedBlockingQueue<>(),
            SYSTEM_POOL,
            (t, e) -> kctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e)));

        assert cctx.pageStore() instanceof FilePageStoreManager;

        FilePageStoreManager storeMgr = (FilePageStoreManager)cctx.pageStore();

        // todo must be available on storage configuration
        locSnpDir = U.resolveWorkDirectory(kctx.config().getWorkDirectory(), DFLT_LOCAL_SNAPSHOT_DIRECTORY, false);
        tmpWorkDir = Paths.get(storeMgr.workDir().getAbsolutePath(), DFLT_SNAPSHOT_WORK_DIRECTORY).toFile();

        U.ensureDirectory(locSnpDir, "local snapshots directory", log);
        U.ensureDirectory(tmpWorkDir, "work directory for snapshots creation", log);

        storeFactory = storeMgr::getPageStoreFactory;
        dbMgr = (GridCacheDatabaseSharedManager)cctx.database();

        dbMgr.addCheckpointListener(cpLsnr = new DbCheckpointListener() {
            @Override public void beforeCheckpointBegin(Context ctx) {
                for (LocalSnapshotContext sctx0 : locSnpCtxs.values()) {
                    // Gather partitions metainfo for thouse which will be copied.
                    if (!sctx0.state(SnapshotState.MARK))
                        continue;

                    ctx.collectPartStat(sctx0.parts);

                    ctx.cpFinishFut().listen(f -> {
                        if (f.error() == null)
                            sctx0.cpEndFut.complete(true);
                        else
                            sctx0.cpEndFut.completeExceptionally(f.error());
                    });
                }
            }

            @Override public void onMarkCheckpointBegin(Context ctx) {
                // Write lock is helded. Partition counters has been collected under write lock
                // in another checkpoint listeners.
            }

            @Override public void onMarkCheckpointEnd(Context ctx) {
                // Under the write lock here. It's safe to add new stores.
                for (LocalSnapshotContext sctx0 : locSnpCtxs.values()) {
                    if (!sctx0.state(SnapshotState.START))
                        continue;

                    try {
                        PartitionAllocationMap allocationMap = ctx.partitionStatMap();

                        allocationMap.prepareForSnapshot();

                        for (GroupPartitionId pair : sctx0.parts) {
                            PagesAllocationRange allocRange = allocationMap.get(pair);

                            GridDhtLocalPartition part = pair.getPartitionId() == INDEX_PARTITION ? null :
                                cctx.cache()
                                    .cacheGroup(pair.getGroupId())
                                    .topology()
                                    .localPartition(pair.getPartitionId());

                            // Partition can be reserved.
                            // Partition can be MOVING\RENTING states.
                            // Index partition will be excluded if not all partition OWNING.
                            // There is no data assigned to partition, thus it haven't been created yet.
                            assert allocRange != null || part == null || part.state() != GridDhtPartitionState.OWNING :
                                "Partition counters has not been collected " +
                                "[pair=" + pair + ", snpName=" + sctx0.snpName + ", part=" + part + ']';

                            if (allocRange == null) {
                                List<GroupPartitionId> missed = sctx0.parts.stream()
                                    .filter(allocationMap::containsKey)
                                    .collect(Collectors.toList());

                                sctx0.acceptException(new IgniteCheckedException("Snapshot operation cancelled due to " +
                                    "not all of requested partitions has OWNING state [missed=" + missed + ']'));

                                break;
                            }

                            PageStore store = storeMgr.getStore(pair.getGroupId(), pair.getPartitionId());

                            sctx0.partFileLengths.put(pair, store.size());
                            sctx0.partDeltaWriters.get(pair).init(allocRange.getCurrAllocatedPageCnt());
                        }
                    }
                    catch (IgniteCheckedException e) {
                        sctx0.acceptException(e);
                    }
                }
            }

            @Override public void onCheckpointBegin(Context ctx) {
                for (LocalSnapshotContext sctx0 : locSnpCtxs.values()) {
                    // Close outpdated snapshots
                    if (sctx0.state == SnapshotState.STOPPING)
                        CompletableFuture.runAsync(sctx0::close, sctx0.exec);

                    if (!sctx0.state(SnapshotState.STARTED))
                        continue;

                    // Submit all tasks for partitions and deltas processing.
                    List<CompletableFuture<Void>> futs = new ArrayList<>();
                    FilePageStoreManager storeMgr = (FilePageStoreManager)cctx.pageStore();

                    if (log.isInfoEnabled())
                        log.info("Submit partition processings tasks with partition allocated lengths: " + sctx0.partFileLengths);

                    // Process binary meta.
                    futs.add(CompletableFuture.runAsync(
                        wrapExceptionally(() ->
                                sctx0.snpSndr.sendBinaryMeta(cctx.kernalContext()
                                    .cacheObjects()
                                    .metadataTypes()),
                            sctx0),
                        sctx0.exec));

                    // Process marshaller meta.
                    futs.add(CompletableFuture.runAsync(
                        wrapExceptionally(() ->
                                sctx0.snpSndr.sendMarshallerMeta(cctx.kernalContext()
                                    .marshallerContext()
                                    .getCachedMappings()),
                            sctx0),
                        sctx0.exec));

                    // Process cache group configuration files.
                    sctx0.parts.stream()
                        .map(GroupPartitionId::getGroupId)
                        .collect(Collectors.toSet())
                        .forEach(grpId ->
                            futs.add(CompletableFuture.runAsync(() -> {
                                    wrapExceptionally(() -> {
                                            CacheConfiguration ccfg = cctx.cache()
                                                .cacheGroup(grpId)
                                                .config();

                                            assert ccfg != null : "Cache configuraction cannot be empty on " +
                                                "a snapshot creation: " + grpId;

                                            List<File> ccfgs = storeMgr.configurationFiles(ccfg);

                                            if (ccfgs == null)
                                                return;

                                            for (File ccfg0 : ccfgs)
                                                sctx0.snpSndr.sendCacheConfig(ccfg0, cacheDirName(ccfg));
                                        },
                                        sctx0);
                                },
                                sctx0.exec)
                            )
                        );

                    // Process partitions.
                    for (GroupPartitionId pair : sctx0.parts) {
                        CacheConfiguration ccfg = cctx.cache().cacheGroup(pair.getGroupId()).config();

                        assert ccfg != null : "Cache configuraction cannot be empty on snapshot creation: " + pair;

                        String cacheDirName = cacheDirName(ccfg);
                        Long partLen = sctx0.partFileLengths.get(pair);

                        CompletableFuture<Void> fut0 = CompletableFuture.runAsync(
                            wrapExceptionally(() -> {
                                    sctx0.snpSndr.sendPart(
                                        getPartitionFile(storeMgr.workDir(), cacheDirName, pair.getPartitionId()),
                                        cacheDirName,
                                        pair,
                                        partLen);

                                    // Stop partition writer.
                                    sctx0.partDeltaWriters.get(pair).markPartitionProcessed();
                                },
                                sctx0),
                            sctx0.exec)
                            // Wait for the completion of both futures - checkpoint end, copy partition.
                            .runAfterBothAsync(sctx0.cpEndFut,
                                wrapExceptionally(() -> {
                                        File delta = getPartionDeltaFile(cacheWorkDir(sctx0.nodeSnpDir, cacheDirName),
                                            pair.getPartitionId());

                                        sctx0.snpSndr.sendDelta(delta, cacheDirName, pair);

                                        boolean deleted = delta.delete();

                                        assert deleted;
                                    },
                                    sctx0),
                                sctx0.exec);

                        futs.add(fut0);
                    }

                    int futsSize = futs.size();

                    CompletableFuture.allOf(futs.toArray(new CompletableFuture[futsSize]))
                        .whenComplete((res, t) -> {
                            assert t == null : "Excepction must never be thrown since a wrapper is used " +
                                "for each snapshot task: " + t;

                            LocalSnapshotContext snpCtx = locSnpCtxs.remove(sctx0.snpName);

                            snpCtx.close();
                        });
                }
            }
        });

        // Receive remote snapshots requests.
        cctx.gridIO().addMessageListener(DFLT_INITIAL_SNAPSHOT_TOPIC, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                if (!busyLock.enterBusy())
                    return;

                try {
                    if (msg instanceof SnapshotRequestMessage) {
                        SnapshotRequestMessage reqMsg0 = (SnapshotRequestMessage)msg;
                        String snpName = reqMsg0.snapshotName();
                        GridCacheSharedContext cctx0 = cctx;
                        IgniteCheckedException ex = null;

                        try {
                            synchronized (rmtSnps) {
                                IgniteInternalFuture<Boolean> snpResp = rmtSnps.remove(nodeId);

                                if (snpResp != null) {
                                    snpResp.cancel();

                                    log.info("Snapshot request has been cancelled due to another request recevied " +
                                        "[prevSnpResp=" + snpResp + ", msg0=" + reqMsg0 + ']');
                                }

                                IgniteInternalFuture<Boolean> snpFut = scheduleSnapshot(snpName,
                                    nodeId,
                                    reqMsg0.parts(),
                                    new SerialExecutor(cctx0.kernalContext()
                                        .pools()
                                        .poolForPolicy(plc)),
                                    remoteSnapshotSender(snpName,
                                        nodeId));

                                snpFut.listen(f -> rmtSnps.remove(nodeId, snpFut));
                                rmtSnps.put(nodeId, snpFut);
                            }
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Failed to proccess request of creating a snapshot " +
                                "[from=" + nodeId + ", msg=" + reqMsg0 + ']');

                            ex = e;
                        }

                        if (ex == null)
                            return;

                        try {
                            cctx.gridIO().sendToCustomTopic(nodeId,
                                DFLT_INITIAL_SNAPSHOT_TOPIC,
                                new SnapshotResponseMessage(reqMsg0.snapshotName(), ex.getMessage()),
                                SYSTEM_POOL);
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Fail to send the response message with processing snapshot request " +
                                "error [request=" + reqMsg0 + ", nodeId=" + nodeId + ']', ex);
                        }
                    }
                    else if (msg instanceof SnapshotResponseMessage) {

                        SnapshotResponseMessage respMsg0 = (SnapshotResponseMessage)msg;

                        SnapshotRequestFuture fut0 = snpReq.get();

                        if (fut0 == null || !fut0.snpName.equals(respMsg0.snapshotName())) {
                            if (log.isInfoEnabled()) {
                                log.info("A stale snapshot response message has been received. Will be ignored " +
                                    "[fromNodeId=" + nodeId + ", response=" + respMsg0 + ']');
                            }

                            return;
                        }

                        if (respMsg0.errorMessage() != null) {
                            fut0.onDone(new IgniteCheckedException("Request cancelled. The snapshot operation stopped " +
                                "on the remote node with an error: " + respMsg0.errorMessage()));
                        }
                    }
                }
                finally {
                    busyLock.leaveBusy();
                }
            }
        });

        cctx.gridEvents().addDiscoveryEventListener(discoLsnr = (evt, discoCache) -> {
            if (!busyLock.enterBusy())
                return;

            try {
                for (LocalSnapshotContext sctc : locSnpCtxs.values()) {
                    if (sctc.srcNodeId.equals(evt.eventNode().id())) {
                        sctc.acceptException(new ClusterTopologyCheckedException("The node which requested snapshot" +
                            "creation has left the grid"));
                    }
                }

                SnapshotRequestFuture snpTrFut = snpReq.get();

                if (snpTrFut == null)
                    return;

                if (snpTrFut.rmtNodeId.equals(evt.eventNode().id())) {
                    snpTrFut.onDone(new ClusterTopologyCheckedException("The node from which a snapshot has been " +
                        "requested left the grid"));
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }, EVT_NODE_LEFT, EVT_NODE_FAILED);

        // Remote snapshot handler.
        cctx.kernalContext().io().addTransmissionHandler(DFLT_INITIAL_SNAPSHOT_TOPIC, new TransmissionHandler() {
            /** {@inheritDoc} */
            @Override public void onException(UUID nodeId, Throwable err) {
                SnapshotRequestFuture fut = snpReq.get();

                if (fut == null)
                    return;

                if (fut.rmtNodeId.equals(nodeId)) {
                    fut.onDone(err);

                    if(snpLsnr != null)
                        snpLsnr.onException(nodeId, err);
                }
            }

            /** {@inheritDoc} */
            @Override public String filePath(UUID nodeId, TransmissionMeta fileMeta) {
                Integer partId = (Integer)fileMeta.params().get(SNP_PART_ID_PARAM);
                String snpName = (String)fileMeta.params().get(SNP_NAME_PARAM);
                String rmtDbNodePath = (String)fileMeta.params().get(SNP_DB_NODE_PATH_PARAM);
                String cacheDirName = (String)fileMeta.params().get(SNP_CACHE_DIR_NAME_PARAM);

                SnapshotRequestFuture transFut = snpReq.get();

                if (transFut == null || !transFut.snpName.equals(snpName)) {
                    throw new TransmissionCancelledException("Stale snapshot transmission will be ignored " +
                        "[snpName=" + snpName + ", transFut=" + transFut + ']');
                }

                assert transFut.snpName.equals(snpName) &&  transFut.rmtNodeId.equals(nodeId) :
                    "Another transmission in progress [fut=" + transFut + ", nodeId=" + snpName + ", nodeId=" + nodeId +']';

                try {
                    File cacheDir = U.resolveWorkDirectory(tmpWorkDir.getAbsolutePath(),
                        cacheSnapshotPath(snpName, rmtDbNodePath, cacheDirName),
                        false);

                    return new File(cacheDir, getPartitionFileName(partId)).getAbsolutePath();
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }

            /**
             * @param snpTrans Current snapshot transmission.
             * @param rmtNodeId Remote node which sends partition.
             * @param grpPartId Pair of group id and its partition id.
             */
            private void finishRecover(
                SnapshotRequestFuture snpTrans,
                UUID rmtNodeId,
                GroupPartitionId grpPartId
            ) {
                FilePageStore pageStore = null;

                try {
                    pageStore = snpTrans.stores.remove(grpPartId);

                    pageStore.finishRecover();

                    String partAbsPath = pageStore.getFileAbsolutePath();

                    cctx.kernalContext().closure().runLocalSafe(() -> {
                        if (snpLsnr == null)
                            return;

                        snpLsnr.onPartition(rmtNodeId,
                            new File(partAbsPath),
                            grpPartId.getGroupId(),
                            grpPartId.getPartitionId());
                    });

                    if (snpTrans.partsLeft.decrementAndGet() == 0) {
                        cctx.kernalContext().closure().runLocalSafe(() -> {
                            if (snpLsnr == null)
                                return;

                            snpLsnr.onEnd(rmtNodeId);
                        });

                        snpTrans.onDone(true);
                    }
                }
                catch (StorageException e) {
                    throw new IgniteException(e);
                }
                finally {
                    U.closeQuiet(pageStore);
                }
            }

            /** {@inheritDoc} */
            @Override public Consumer<ByteBuffer> chunkHandler(UUID nodeId, TransmissionMeta initMeta) {
                Integer grpId = (Integer)initMeta.params().get(SNP_GRP_ID_PARAM);
                Integer partId = (Integer)initMeta.params().get(SNP_PART_ID_PARAM);
                String snpName = (String)initMeta.params().get(SNP_NAME_PARAM);

                GroupPartitionId grpPartId = new GroupPartitionId(grpId, partId);
                SnapshotRequestFuture snpTrFut = snpReq.get();

                if (snpTrFut == null || !snpTrFut.snpName.equals(snpName)) {
                    throw new TransmissionCancelledException("Stale snapshot transmission will be ignored " +
                        "[snpName=" + snpName + ", grpId=" + grpId + ", partId=" + partId + ", snpTrFut=" + snpTrFut + ']');
                }

                assert snpTrFut.snpName.equals(snpName) &&  snpTrFut.rmtNodeId.equals(nodeId) :
                    "Another transmission in progress [snpTrFut=" + snpTrFut + ", nodeId=" + snpName + ", nodeId=" + nodeId +']';

                FilePageStore pageStore = snpTrFut.stores.get(grpPartId);

                if (pageStore == null) {
                    throw new IgniteException("Partition must be loaded before applying snapshot delta pages " +
                        "[snpName=" + snpName + ", grpId=" + grpId + ", partId=" + partId + ']');
                }

                pageStore.beginRecover();

                // No snapshot delta pages received. Finalize recovery.
                if (initMeta.count() == 0) {
                    finishRecover(snpTrFut,
                        nodeId,
                        grpPartId);
                }

                return new Consumer<ByteBuffer>() {
                    final LongAdder transferred = new LongAdder();

                    @Override public void accept(ByteBuffer buff) {
                        try {
                            assert initMeta.count() != 0 : initMeta;

                            SnapshotRequestFuture fut0 = snpReq.get();

                            if (fut0 == null || !fut0.equals(snpTrFut) || fut0.isCancelled()) {
                                throw new TransmissionCancelledException("Snapshot request is cancelled " +
                                    "[snpName=" + snpName + ", grpId=" + grpId + ", partId=" + partId + ']');
                            }

                            pageStore.write(PageIO.getPageId(buff), buff, 0, false);

                            transferred.add(buff.capacity());

                            if (transferred.longValue() == initMeta.count()) {
                                finishRecover(snpTrFut,
                                    nodeId,
                                    grpPartId);
                            }
                        }
                        catch (IgniteCheckedException e) {
                            throw new IgniteException(e);
                        }
                    }
                };
            }

            /** {@inheritDoc} */
            @Override public Consumer<File> fileHandler(UUID nodeId, TransmissionMeta initMeta) {
                Integer grpId = (Integer)initMeta.params().get(SNP_GRP_ID_PARAM);
                Integer partId = (Integer)initMeta.params().get(SNP_PART_ID_PARAM);
                String snpName = (String)initMeta.params().get(SNP_NAME_PARAM);

                assert grpId != null;
                assert partId != null;
                assert snpName != null;
                assert storeFactory != null;

                SnapshotRequestFuture transFut = snpReq.get();

                if (transFut == null) {
                    throw new IgniteException("Snapshot transmission with given name doesn't exists " +
                        "[snpName=" + snpName + ", grpId=" + grpId + ", partId=" + partId + ']');
                }

                return new Consumer<File>() {
                    @Override public void accept(File file) {
                        SnapshotRequestFuture fut0 = snpReq.get();

                        if (fut0 == null || !fut0.equals(transFut) || fut0.isCancelled()) {
                            throw new TransmissionCancelledException("Snapshot request is cancelled [snpName=" + snpName +
                                ", grpId=" + grpId + ", partId=" + partId + ']');
                        }

                        busyLock.enterBusy();

                        try {
                            FilePageStore pageStore = (FilePageStore)storeFactory
                                .apply(grpId, false)
                                .createPageStore(getFlagByPartId(partId),
                                    file::toPath,
                                    new LongAdderMetric("NO_OP", null));

                            transFut.stores.put(new GroupPartitionId(grpId, partId), pageStore);

                            pageStore.init();
                        }
                        catch (IgniteCheckedException e) {
                            throw new IgniteException(e);
                        }
                        finally {
                            busyLock.leaveBusy();
                        }
                    }
                };
            }
        });
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        busyLock.block();

        try {
            dbMgr.removeCheckpointListener(cpLsnr);

            for (LocalSnapshotContext sctx : locSnpCtxs.values()) {
                // Try stop all snapshot processing if not yet.
                sctx.close(new NodeStoppingException("Snapshot has been cancelled due to the local node " +
                    "is stopping"));
            }

            locSnpCtxs.clear();

            SnapshotRequestFuture snpTrFut = snpReq.get();

            if (snpTrFut != null)
                snpTrFut.cancel();

            snpRunner.shutdown();

            cctx.kernalContext().io().removeMessageListener(DFLT_INITIAL_SNAPSHOT_TOPIC);
            cctx.kernalContext().event().removeDiscoveryEventListener(discoLsnr);
            cctx.kernalContext().io().removeTransmissionHandler(DFLT_INITIAL_SNAPSHOT_TOPIC);
        }
        finally {
            busyLock.unblock();
        }
    }

    /**
     * @return Relative configured path of presistence data storage directory for the local node.
     */
    public String relativeStoragePath() throws IgniteCheckedException {
        PdsFolderSettings pCfg = cctx.kernalContext().pdsFolderResolver().resolveFolders();

        return Paths.get(DB_DEFAULT_FOLDER, pCfg.folderName()).toString();
    }

    /**
     * @param snpLsnr Snapshot listener instance.
     */
    public void addSnapshotListener(SnapshotListener snpLsnr) {
        this.snpLsnr = snpLsnr;
    }

    /**
     * @param snpName Snapshot name.
     * @return Local snapshot directory for snapshot with given name.
     */
    public File localSnapshotDir(String snpName) {
        return new File(localSnapshotWorkDir(), snpName);
    }

    /**
     * @return Snapshot directory used by manager for local snapshots.
     */
    public File localSnapshotWorkDir() {
        assert locSnpDir != null;

        return locSnpDir;
    }

    /**
     * @return Node snapshot working directory.
     */
    public File snapshotWorkDir() {
        assert tmpWorkDir != null;

        return tmpWorkDir;
    }

    /**
     * @return Node snapshot working directory with given snapshot name.
     */
    public File snapshotWorkDir(String snpName) {
        return new File(snapshotWorkDir(), snpName);
    }

    /**
     * @param snpName Unique snapshot name.
     * @return Future which will be completed when snapshot is done.
     */
    public IgniteInternalFuture<Boolean> createLocalSnapshot(String snpName, List<Integer> grpIds) {
        // Collection of pairs group and appropratate cache partition to be snapshotted.
        Map<Integer, GridIntList> parts = grpIds.stream()
            .collect(Collectors.toMap(grpId -> grpId,
                grpId -> {
                    Set<Integer> grpParts = new HashSet<>();

                    cctx.cache()
                        .cacheGroup(grpId)
                        .topology()
                        .currentLocalPartitions()
                        .forEach(p -> grpParts.add(p.id()));

                    grpParts.add(INDEX_PARTITION);

                    return GridIntList.valueOf(grpParts);
                }));

        File rootSnpDir0 = localSnapshotDir(snpName);

        try {
            IgniteInternalFuture<Boolean> snpFut = scheduleSnapshot(snpName,
                cctx.localNodeId(),
                parts,
                snpRunner,
                localSnapshotSender(rootSnpDir0));

            LocalSnapshotContext sctx = locSnpCtxs.get(snpName);

            assert sctx != null : "Just started snapshot cannot has an empty context: " + snpName;

            dbMgr.forceCheckpoint(String.format(SNAPSHOT_CP_REASON, snpName))
                .beginFuture()
                .get();

            // Snapshot is still in the INIT state. beforeCheckpoint has been skipped
            // due to checkpoint aready running and we need to schedule the next one
            // right afther current will be completed.
            if (sctx.state.ordinal() == SnapshotState.INIT.ordinal())
                dbMgr.forceCheckpoint(String.format(SNAPSHOT_CP_REASON, snpName));

            U.await(sctx.startedLatch);

            return snpFut;
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * @param parts Collection of pairs group and appropratate cache partition to be snapshotted.
     * @param rmtNodeId The remote node to connect to.
     * @return Snapshot name.
     * @throws IgniteCheckedException If initialiation fails.
     */
    public IgniteInternalFuture<Boolean> createRemoteSnapshot(UUID rmtNodeId, Map<Integer, Set<Integer>> parts) throws IgniteCheckedException {
        ClusterNode rmtNode = cctx.discovery().node(rmtNodeId);

        if (!nodeSupports(rmtNode, PERSISTENCE_CACHE_SNAPSHOT))
            throw new IgniteException("Snapshot on remote node is not supported: " + rmtNode.id());

        if (rmtNode == null) {
            throw new ClusterTopologyCheckedException("Snapshot request cannot be performed. Remote node left the grid " +
                "[rmtNodeId=" + rmtNodeId + ']');
        }

        for (Map.Entry<Integer, Set<Integer>> e : parts.entrySet()) {
            int grpId = e.getKey();

            GridDhtPartitionMap partMap = cctx.cache()
                .cacheGroup(grpId)
                .topology()
                .partitions(rmtNodeId);

            Set<Integer> owningParts = partMap.entrySet()
                .stream()
                .filter(p -> p.getValue() == GridDhtPartitionState.OWNING)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

            if (!owningParts.containsAll(e.getValue())) {
                Set<Integer> substract = new HashSet<>(e.getValue());

                substract.removeAll(owningParts);

                throw new IgniteCheckedException("Only owning partitions allowed to be requested from the remote node " +
                    "[rmtNodeId=" + rmtNodeId + ", grpId=" + grpId + ", missed=" + substract + ']');
            }
        }

        String snpName = "snapshot_" + UUID.randomUUID().getMostSignificantBits();

        SnapshotRequestFuture snpTransFut = new SnapshotRequestFuture(rmtNodeId, snpName,
            parts.values().stream().mapToInt(Set::size).sum());

        busyLock.enterBusy();
        SnapshotRequestMessage msg0;

        try {
            msg0 = new SnapshotRequestMessage(snpName,
                parts.entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey,
                        e -> GridIntList.valueOf(e.getValue()))));

            SnapshotRequestFuture fut = snpReq.get();

            if (fut != null && !fut.isCancelled())
                throw new IgniteCheckedException("Previous snapshot request has not been finished yet: " + fut);

            try {
                long startTime = U.currentTimeMillis();

                // todo loop can be replaced with wait on future
                while (true) {
                    if (snpReq.compareAndSet(null, snpTransFut)) {
                        cctx.gridIO().sendOrderedMessage(rmtNode, DFLT_INITIAL_SNAPSHOT_TOPIC, msg0, SYSTEM_POOL,
                            Long.MAX_VALUE, true);

                        break;
                    }
                    else if (U.currentTimeMillis() - startTime > DFLT_CREATE_SNAPSHOT_TIMEOUT)
                        throw new IgniteException("Error waiting for a previous requested snapshot completed: " + snpTransFut);

                    U.sleep(200);
                }
            }
            catch (IgniteCheckedException e) {
                snpReq.compareAndSet(snpTransFut, null);

                throw e;
            }
        }
        finally {
            busyLock.leaveBusy();
        }

        if (log.isInfoEnabled()) {
            log.info("Snapshot request is sent to the remote node [rmtNodeId=" + rmtNodeId +
                ", msg0=" + msg0 + ", snpTransFut=" + snpTransFut +
                ", topVer=" + cctx.discovery().topologyVersionEx() + ']');
        }

        return snpTransFut;
    }

    /**
     * @param snpName Unique snapshot name.
     * @param srcNodeId Node id which cause snapshot operation.
     * @param parts Collection of pairs group and appropratate cache partition to be snapshotted.
     * @param snpSndr Factory which produces snapshot receiver instance.
     * @return Future which will be completed when snapshot is done.
     * @throws IgniteCheckedException If initialization fails.
     */
    IgniteInternalFuture<Boolean> scheduleSnapshot(
        String snpName,
        UUID srcNodeId,
        Map<Integer, GridIntList> parts,
        Executor exec,
        SnapshotSender snpSndr
    ) throws IgniteCheckedException {
        if (locSnpCtxs.containsKey(snpName))
            throw new IgniteCheckedException("Snapshot with requested name is already scheduled: " + snpName);

        isCacheSnapshotSupported(parts.keySet(),
            (grpId) -> !CU.isPersistentCache(cctx.cache().cacheGroup(grpId).config(),
            cctx.kernalContext().config().getDataStorageConfiguration()),
            "in-memory cache groups are not allowed");
        isCacheSnapshotSupported(parts.keySet(),
            (grpId) -> cctx.cache().cacheGroup(grpId).config().isEncryptionEnabled(),
            "encryption cache groups are not allowed");

        LocalSnapshotContext sctx = null;
        File nodeSnpDir = null;

        if (!busyLock.enterBusy())
            throw new IgniteCheckedException("Snapshot manager is stopping [locNodeId=" + cctx.localNodeId() + ']');

        try {
            String dbNodePath = relativeStoragePath();
            nodeSnpDir = U.resolveWorkDirectory(new File(tmpWorkDir, snpName).getAbsolutePath(), dbNodePath, false);

            sctx = new LocalSnapshotContext(log,
                snpName,
                srcNodeId,
                snapshotWorkDir(snpName),
                nodeSnpDir,
                parts,
                exec,
                snpSndr);

            final LocalSnapshotContext sctx0 = sctx;

            for (Map.Entry<Integer, GridIntList> e : parts.entrySet()) {
                final CacheGroupContext gctx = cctx.cache().cacheGroup(e.getKey());

                // Create cache snapshot directory if not.
                File grpDir = U.resolveWorkDirectory(sctx.nodeSnpDir.getAbsolutePath(),
                    cacheDirName(gctx.config()), false);

                U.ensureDirectory(grpDir,
                    "snapshot directory for cache group: " + gctx.groupId(),
                    null);

                CompletableFuture<Boolean> cpEndFut0 = sctx.cpEndFut;

                GridIntIterator iter = e.getValue().iterator();

                while (iter.hasNext()) {
                    int partId = iter.next();

                    GroupPartitionId pair = new GroupPartitionId(e.getKey(), partId);
                    PageStore store = ((FilePageStoreManager)cctx.pageStore()).getStore(pair.getGroupId(),
                        pair.getPartitionId());

                    sctx.partDeltaWriters.put(pair,
                        new PageStoreSerialWriter(log,
                            store,
                            () -> cpEndFut0.isDone() && !cpEndFut0.isCompletedExceptionally(),
                            () -> sctx0.state == SnapshotState.STOPPED || sctx0.state == SnapshotState.STOPPING,
                            sctx0::acceptException,
                            getPartionDeltaFile(grpDir, partId),
                            ioFactory,
                            pageSize));
                }
            }

            LocalSnapshotContext ctx0 = locSnpCtxs.putIfAbsent(snpName, sctx);

            assert ctx0 == null : ctx0;

            if (log.isInfoEnabled()) {
                log.info("Snapshot operation is scheduled on local node and will be handled by the checkpoint " +
                    "listener [sctx=" + sctx + ", topVer=" + cctx.discovery().topologyVersionEx() + ']');
            }
        }
        catch (IOException e) {
            locSnpCtxs.remove(snpName, sctx);

            sctx.close(e);

            if (nodeSnpDir != null)
                nodeSnpDir.delete();

            throw new IgniteCheckedException(e);
        }
        finally {
            busyLock.leaveBusy();
        }

        return sctx.snpFut;
    }

    /**
     *
     * @param rootSnpDir Absolute snapshot directory.
     * @return Snapshot receiver instance.
     */
    SnapshotSender localSnapshotSender(File rootSnpDir) throws IgniteCheckedException {
        // Relative path to snapshot storage of local node.
        // Example: snapshotWorkDir/db/IgniteNodeName0
        String dbNodePath = relativeStoragePath();

        U.ensureDirectory(new File(rootSnpDir, dbNodePath), "local snapshot directory", log);

        return new LocalSnapshotSender(log,
            new File(rootSnpDir, dbNodePath),
            ioFactory,
            storeFactory,
            cctx.kernalContext()
                .cacheObjects()
                .binaryWriter(rootSnpDir.getAbsolutePath()),
            cctx.kernalContext()
                .marshallerContext()
                .marshallerMappingWriter(cctx.kernalContext(), rootSnpDir.getAbsolutePath()),
            pageSize);
    }

    /**
     * @param snpName Snapshot name.
     * @param rmtNodeId Remote node id to send snapshot to.
     * @return Snapshot sender instance.
     */
    SnapshotSender remoteSnapshotSender(
        String snpName,
        UUID rmtNodeId
    ) throws IgniteCheckedException {
        // Relative path to snapshot storage of local node.
        // Example: snapshotWorkDir/db/IgniteNodeName0
        String dbNodePath = relativeStoragePath();

        return new RemoteSnapshotSender(log,
            cctx.gridIO().openTransmissionSender(rmtNodeId, DFLT_INITIAL_SNAPSHOT_TOPIC),
            errMsg -> cctx.gridIO().sendToCustomTopic(rmtNodeId,
                DFLT_INITIAL_SNAPSHOT_TOPIC,
                new SnapshotResponseMessage(snpName, errMsg),
                SYSTEM_POOL),
            snpName,
            dbNodePath);
    }

    /**
     * @return The executor service used to run snapshot tasks.
     */
    ExecutorService snapshotExecutorService() {
        assert snpRunner != null;

        return snpRunner;
    }

    /**
     * @param snpName Unique snapshot name.
     */
    public void stopCacheSnapshot(String snpName) {

    }

    /**
     * @param ioFactory Factory to create IO interface over a page stores.
     */
    void ioFactory(FileIOFactory ioFactory) {
        this.ioFactory = ioFactory;
    }

    /**
     * @param nodeId Remote node id on which requests has been registered.
     * @return Snapshot future related to given node id.
     */
    IgniteInternalFuture<Boolean> snapshotRemoteRequest(UUID nodeId) {
        return rmtSnps.get(nodeId);
    }

    /**
     * @param rslvr RDS resolver.
     * @param dirPath Relative working directory path.
     * @param errorMsg Error message in case of make direcotry fail.
     * @return Resolved working direcory.
     * @throws IgniteCheckedException If fails.
     */
    private static File initWorkDirectory(
        PdsFolderSettings rslvr,
        String dirPath,
        IgniteLogger log,
        String errorMsg
    ) throws IgniteCheckedException {
        File rmtSnpDir = U.resolveWorkDirectory(rslvr.persistentStoreRootPath().getAbsolutePath(), dirPath, false);

        File target = new File (rmtSnpDir, rslvr.folderName());

        U.ensureDirectory(target, errorMsg, log);

        return target;
    }

    /**
     * @param dbNodePath Persistence node path.
     * @param snpName Snapshot name.
     * @param cacheDirName Cache directory name.
     * @return Relative cache path.
     */
    private static String cacheSnapshotPath(String snpName, String dbNodePath, String cacheDirName) {
        return Paths.get(snpName, dbNodePath, cacheDirName).toString();
    }

    /**
     * @param grps Set of cache groups to check.
     * @param grpPred Checking predicate.
     * @param errCause Cause of error message if fails.
     */
    private static void isCacheSnapshotSupported(Set<Integer> grps, Predicate<Integer> grpPred, String errCause) {
        Set<Integer> notAllowdGrps = grps.stream()
            .filter(grpPred)
            .collect(Collectors.toSet());

        if (!notAllowdGrps.isEmpty()) {
            throw new IgniteException("Snapshot is not supported for these groups [cause=" + errCause +
                ", grps=" + notAllowdGrps + ']');
        }
    }

    /**
     * @param exec Runnable task to execute.
     * @param sctx Future to notify.
     * @return Wrapped task.
     */
    private static Runnable wrapExceptionally(Runnable exec, LocalSnapshotContext sctx) {
        return () -> {
            try {
                if (sctx.state == SnapshotState.STARTED)
                    exec.run();
            }
            catch (Throwable t) {
                sctx.acceptException(t);
            }
        };
    }

    /**
     *
     */
    private static class PageStoreSerialWriter implements PageWriteListener, Closeable {
        /** Ignite logger to use. */
        @GridToStringExclude
        private final IgniteLogger log;

        /** Page store to which current writer is related to. */
        private final PageStore store;

        /** Busy lock to protect write opertions. */
        private final ReadWriteLock lock = new ReentrantReadWriteLock();

        /** Local buffer to perpform copy-on-write operations. */
        private final ThreadLocal<ByteBuffer> localBuff;

        /** {@code true} if need the original page from PageStore instead of given buffer. */
        private final BooleanSupplier checkpointComplete;

        /** {@code true} if snapshot process is stopping or alredy stopped. */
        private final BooleanSupplier interrupt;

        /** Callback to stop snapshot if an error occurred. */
        private final Consumer<Throwable> exConsumer;

        /** IO over the underlying file */
        private volatile FileIO fileIo;

        /** {@code true} if partition file has been copied to external resource. */
        private volatile boolean partProcessed;

        /** {@code true} means current writer is allowed to handle page writes. */
        private volatile boolean inited;

        /**
         * Array of bits. 1 - means pages written, 0 - the otherwise.
         * Size of array can be estimated only under checkpoint write lock.
         */
        private volatile AtomicBitSet pagesWrittenBits;

        /**
         * @param log Ignite logger to use.
         * @param checkpointComplete Checkpoint finish flag.
         * @param pageSize Size of page to use for local buffer.
         * @param cfgFile Configuration file provider.
         * @param factory Factory to produce an IO interface over underlying file.
         */
        public PageStoreSerialWriter(
            IgniteLogger log,
            PageStore store,
            BooleanSupplier checkpointComplete,
            BooleanSupplier interrupt,
            Consumer<Throwable> exConsumer,
            File cfgFile,
            FileIOFactory factory,
            int pageSize
        ) throws IOException {
            assert store != null;

            this.checkpointComplete = checkpointComplete;
            this.interrupt = interrupt;
            this.exConsumer = exConsumer;
            this.log = log.getLogger(PageStoreSerialWriter.class);

            localBuff = ThreadLocal.withInitial(() ->
                ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder()));

            fileIo = factory.create(cfgFile);

            this.store = store;

            store.addWriteListener(this);
        }

        /**
         * @param allocPages Total number of tracking pages.
         */
        public void init(int allocPages) {
            lock.writeLock().lock();

            try {
                pagesWrittenBits = new AtomicBitSet(allocPages);
                inited = true;
            }
            finally {
                lock.writeLock().unlock();
            }
        }

        /**
         * @return {@code true} if writer is stopped and cannot write pages.
         */
        public boolean stopped() {
            return (checkpointComplete.getAsBoolean() && partProcessed) || interrupt.getAsBoolean();
        }

        /**
         * Mark partition has been processed by another thread.
         */
        public void markPartitionProcessed() {
            lock.writeLock().lock();

            try {
                partProcessed = true;
            }
            finally {
                lock.writeLock().unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public void accept(long pageId, ByteBuffer buf) {
            assert buf.position() == 0 : buf.position();
            assert buf.order() == ByteOrder.nativeOrder() : buf.order();

            lock.readLock().lock();

            try {
                if (!inited)
                    return;

                if (stopped())
                    return;

                if (checkpointComplete.getAsBoolean()) {
                    int pageIdx = PageIdUtils.pageIndex(pageId);

                    // Page already written.
                    if (!pagesWrittenBits.touch(pageIdx))
                        return;

                    final ByteBuffer locBuf = localBuff.get();

                    assert locBuf.capacity() == store.getPageSize();

                    locBuf.clear();

                    if (!store.read(pageId, locBuf, true))
                        return;

                    locBuf.flip();

                    writePage0(pageId, locBuf);
                }
                else {
                    // Direct buffre is needs to be written, associated checkpoint not finished yet.
                    writePage0(pageId, buf);
                }
            }
            catch (Throwable ex) {
                exConsumer.accept(ex);
            }
            finally {
                lock.readLock().unlock();
            }
        }

        /**
         * @param pageId Page ID.
         * @param pageBuf Page buffer to write.
         * @throws IOException If page writing failed (IO error occurred).
         */
        private void writePage0(long pageId, ByteBuffer pageBuf) throws IOException {
            assert fileIo != null : "Delta pages storage is not inited: " + this;
            assert pageBuf.position() == 0;
            assert pageBuf.order() == ByteOrder.nativeOrder() : "Page buffer order " + pageBuf.order()
                + " should be same with " + ByteOrder.nativeOrder();

            int crc = PageIO.getCrc(pageBuf);
            int crc32 = FastCrc.calcCrc(pageBuf, pageBuf.limit());

            if (log.isDebugEnabled()) {
                log.debug("onPageWrite [pageId=" + pageId +
                    ", pageIdBuff=" + PageIO.getPageId(pageBuf) +
                    ", fileSize=" + fileIo.size() +
                    ", crcBuff=" + crc32 +
                    ", crcPage=" + crc + ']');
            }

            pageBuf.rewind();

            // Write buffer to the end of the file.
            fileIo.writeFully(pageBuf);
        }

        /** {@inheritDoc} */
        @Override public void close() {
            lock.writeLock().lock();

            try {
                U.closeQuiet(fileIo);

                fileIo = null;

                store.removeWriteListener(this);

                inited = false;
            }
            finally {
                lock.writeLock().unlock();
            }
        }
    }

    /**
     *
     */
    private static class AtomicBitSet {
        /** Container of bits. */
        private final AtomicIntegerArray arr;

        /** Size of array of bits. */
        private final int size;

        /**
         * @param size Size of array.
         */
        public AtomicBitSet(int size) {
            this.size = size;

            arr = new AtomicIntegerArray((size + 31) >>> 5);
        }

        /**
         * @param off Bit position to change.
         * @return {@code true} if bit has been set,
         * {@code false} if bit changed by another thread or out of range.
         */
        public boolean touch(long off) {
            if (off > size)
                return false;

            int bit = 1 << off;
            int bucket = (int)(off >>> 5);

            while (true) {
                int cur = arr.get(bucket);
                int val = cur | bit;

                if (cur == val)
                    return false;

                if (arr.compareAndSet(bucket, cur, val))
                    return true;
            }
        }
    }

    /**
     *
     */
    private static class SnapshotFuture extends GridFutureAdapter<Boolean> {
        /** Set cancelling state to snapshot. */
        private final Runnable doCancel;

        /**
         * @param doCancel Set cancelling state to snapshot.
         */
        public SnapshotFuture(Runnable doCancel) {
            this.doCancel = doCancel;
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() throws IgniteCheckedException {
            doCancel.run();

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Boolean res, @Nullable Throwable err, boolean cancel) {
            return  super.onDone(res, err, cancel);
        }
    }

    /**
     *
     */
    private class SnapshotRequestFuture extends GridFutureAdapter<Boolean> {
        /** Remote node id to request snapshot from. */
        private final UUID rmtNodeId;

        /** Snapshot name to create on remote. */
        private final String snpName;

        /** Collection of partition to be received. */
        private final Map<GroupPartitionId, FilePageStore> stores = new ConcurrentHashMap<>();

        /** Counter which show how many partitions left to be received. */
        private final AtomicInteger partsLeft;

        /**
         * @param cnt Partitions to receive.
         */
        public SnapshotRequestFuture(UUID rmtNodeId, String snpName, int cnt) {
            this.rmtNodeId = rmtNodeId;
            this.snpName = snpName;
            partsLeft = new AtomicInteger(cnt);
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() {
            if (onCancelled()) {
                // Close non finished file storages
                for (Map.Entry<GroupPartitionId, FilePageStore> entry : stores.entrySet()) {
                    FilePageStore store = entry.getValue();

                    try {
                        store.stop(true);
                    }
                    catch (StorageException e) {
                        log.warning("Error stopping received file page store", e);
                    }
                }
            }

            return isCancelled();
        }

        /** {@inheritDoc} */
        @Override protected boolean onDone(@Nullable Boolean res, @Nullable Throwable err, boolean cancel) {
            assert err != null || cancel || stores.isEmpty() : "Not all file storages processed: " + stores;

            boolean changed = super.onDone(res, err, cancel);

            if (changed)
                snpReq.compareAndSet(this, null);

            return changed;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            SnapshotRequestFuture future = (SnapshotRequestFuture)o;

            return rmtNodeId.equals(future.rmtNodeId) &&
                snpName.equals(future.snpName);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(rmtNodeId, snpName);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SnapshotRequestFuture.class, this);
        }
    }

    /**
     * Valid state transitions:
     * <p>
     * {@code INIT -> MARK -> START -> STARTED -> STOPPED}
     * <p>
     * {@code INIT (or any other) -> STOPPING}
     * <p>
     * {@code CANCELLING -> STOPPED}
     */
    private enum SnapshotState {
        /** Requested partitoins must be registered to collect its partition counters. */
        INIT,

        /** All counters must be collected under the checkpoint write lock. */
        MARK,

        /** Tasks must be scheduled to create requested snapshot. */
        START,

        /** Snapshot tasks has been started. */
        STARTED,

        /** Indicates that snapshot operation must be cancelled and is awaiting resources to be freed. */
        STOPPING,

        /** Snapshot operation has been interruped or an exception occurred. */
        STOPPED
    }

    /**
     *
     */
    private static class LocalSnapshotContext implements Closeable {
        /** Ignite logger */
        private final IgniteLogger log;

        /** Node id which cause snapshot operation. */
        private final UUID srcNodeId;

        /** Unique identifier of snapshot process. */
        private final String snpName;

        /** Snapshot workgin directory on file system. */
        private final File snpWorkDir;

        /** Absolute snapshot storage path. */
        private final File nodeSnpDir;

        /** Service to perform partitions copy. */
        private final Executor exec;

        /**
         * The length of file size per each cache partiton file.
         * Partition has value greater than zero only for partitons in OWNING state.
         * Information collected under checkpoint write lock.
         */
        private final Map<GroupPartitionId, Long> partFileLengths = new HashMap<>();

        /**
         * Map of partitions to snapshot and theirs corresponding delta PageStores.
         * Writers are pinned to the snapshot context due to controlling partition
         * processing supplier.
         */
        private final Map<GroupPartitionId, PageStoreSerialWriter> partDeltaWriters = new HashMap<>();

        /** Future of result completion. */
        @GridToStringExclude
        private final SnapshotFuture snpFut = new SnapshotFuture(() -> {
            cancelled = true;

            close();
        });

        /** Snapshot data sender. */
        @GridToStringExclude
        private final SnapshotSender snpSndr;

        /** Collection of partition to be snapshotted. */
        private final List<GroupPartitionId> parts = new ArrayList<>();

        /** Checkpoint end future. */
        private final CompletableFuture<Boolean> cpEndFut = new CompletableFuture<>();

        /** An exception which has been ocurred during snapshot processing. */
        private volatile Throwable lastTh;

        /** {@code true} if operation has been cancelled. */
        private volatile boolean cancelled;

        /** Latch to wait until checkpoint mark pahse will be finished and snapshot tasks scheduled. */
        private final CountDownLatch startedLatch = new CountDownLatch(1);

        /** Phase of the current snapshot process run. */
        private volatile SnapshotState state = SnapshotState.INIT;

        /**
         * @param snpName Unique identifier of snapshot process.
         * @param nodeSnpDir snapshot storage directory.
         * @param exec Service to perform partitions copy.
         */
        public LocalSnapshotContext(
            IgniteLogger log,
            String snpName,
            UUID srcNodeId,
            File snpWorkDir,
            File nodeSnpDir,
            Map<Integer, GridIntList> parts,
            Executor exec,
            SnapshotSender snpSndr
        ) {
            A.notNull(snpName, "snapshot name cannot be empty or null");
            A.notNull(nodeSnpDir, "You must secify correct snapshot directory");
            A.ensure(nodeSnpDir.isDirectory(), "Specified path is not a directory");
            A.notNull(exec, "Executor service must be not null");
            A.notNull(snpSndr, "Snapshot sender which handles execution tasks must be not null");

            this.log = log.getLogger(LocalSnapshotContext.class);
            this.snpName = snpName;
            this.srcNodeId = srcNodeId;
            this.snpWorkDir = snpWorkDir;
            this.nodeSnpDir = nodeSnpDir;
            this.exec = exec;
            this.snpSndr = snpSndr;

            for (Map.Entry<Integer, GridIntList> e : parts.entrySet()) {
                GridIntIterator iter = e.getValue().iterator();

                while (iter.hasNext())
                    this.parts.add(new GroupPartitionId(e.getKey(), iter.next()));
            }
        }

        /**
         * @param state A new snapshot state to set.
         * @return {@code true} if given state has been set by this call.
         */
        public boolean state(SnapshotState state) {
            if (this.state == state)
                return false;

            synchronized (this) {
                if (this.state == state)
                    return false;

                if (state == SnapshotState.STOPPING) {
                    this.state = SnapshotState.STOPPING;

                    startedLatch.countDown();

                    return true;
                }

                if (state.ordinal() > this.state.ordinal()) {
                    this.state = state;

                    if (state == SnapshotState.STARTED)
                        startedLatch.countDown();

                    return true;
                }
                else
                    return false;
            }
        }

        /**
         * @param th An exception which occurred during snapshot processing.
         */
        public void acceptException(Throwable th) {
            if (state(SnapshotState.STOPPING))
                lastTh = th;
        }

        /**
         * @param th Occurred exception during processing or {@code null} if not.
         */
        public void close(Throwable th) {
            if (state(SnapshotState.STOPPED)) {
                if (lastTh == null)
                    lastTh = th;

                for (PageStoreSerialWriter writer : partDeltaWriters.values())
                    U.closeQuiet(writer);

                snpSndr.close(lastTh);

                U.delete(nodeSnpDir);

                // Delete snapshot directory if no other files exists.
                try {
                    if (U.fileCount(snpWorkDir.toPath()) == 0)
                        U.delete(snpWorkDir.toPath());
                }
                catch (IOException e) {
                    log.error("Snapshot directory doesn't exist [snpName=" + snpName + ", dir=" + snpWorkDir + ']');
                }

                snpFut.onDone(true, lastTh, cancelled);
            }
        }

        /** {@inheritDoc} */
        @Override public void close() {
            close(null);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            LocalSnapshotContext ctx = (LocalSnapshotContext)o;

            return snpName.equals(ctx.snpName);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(snpName);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LocalSnapshotContext.class, this);
        }
    }

    /**
     *
     */
    private static class SerialExecutor implements Executor {
        /** */
        private final Queue<Runnable> tasks = new ArrayDeque<>();

        /** */
        private final Executor executor;

        /** */
        private volatile Runnable active;

        /**
         * @param executor Executor to run tasks on.
         */
        public SerialExecutor(Executor executor) {
            this.executor = executor;
        }

        /** {@inheritDoc} */
        @Override public synchronized void execute(final Runnable r) {
            tasks.offer(new Runnable() {
                /** {@inheritDoc} */
                @Override public void run() {
                    try {
                        r.run();
                    }
                    finally {
                        scheduleNext();
                    }
                }
            });

            if (active == null) {
                scheduleNext();
            }
        }

        /**
         *
         */
        protected synchronized void scheduleNext() {
            if ((active = tasks.poll()) != null) {
                executor.execute(active);
            }
        }
    }

    /**
     *
     */
    private static class RemoteSnapshotSender extends SnapshotSender {
        /** The sender which sends files to remote node. */
        private final GridIoManager.TransmissionSender sndr;

        /** Error handler which will be triggered in case of transmission sedner not started yet. */
        private final IgniteThrowableConsumer<String> errHnd;

        /** Snapshot name */
        private final String snpName;

        /** Local node persistent directory with consistent id. */
        private final String dbNodePath;

        /**
         * @param log Ignite logger.
         * @param sndr File sender instance.
         * @param errHnd Snapshot error handler if transmission sender not started yet.
         * @param snpName Snapshot name.
         */
        public RemoteSnapshotSender(
            IgniteLogger log,
            GridIoManager.TransmissionSender sndr,
            IgniteThrowableConsumer<String> errHnd,
            String snpName,
            String dbNodePath
        ) {
            super(log);

            this.sndr = sndr;
            this.errHnd = errHnd;
            this.snpName = snpName;
            this.dbNodePath = dbNodePath;
        }

        /** {@inheritDoc} */
        @Override public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long len) {
            try {
                assert part.exists();
                assert len > 0 : "Requested partitions has incorrect file length " +
                    "[pair=" + pair + ", cacheDirName=" + cacheDirName + ']';

                sndr.send(part, 0, len, transmissionParams(snpName, cacheDirName, pair), TransmissionPolicy.FILE);

                if (log.isInfoEnabled()) {
                    log.info("Partition file has been send [part=" + part.getName() + ", pair=" + pair +
                        ", length=" + len + ']');
                }
            }
            catch (TransmissionCancelledException e) {
                if (log.isInfoEnabled()) {
                    log.info("Transmission partition file has been interrupted [part=" + part.getName() +
                        ", pair=" + pair + ']');
                }
            }
            catch (IgniteCheckedException | InterruptedException | IOException e) {
                U.error(log, "Error sending partition file [part=" + part.getName() + ", pair=" + pair +
                    ", length=" + len + ']', e);

                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void sendDelta0(File delta, String cacheDirName, GroupPartitionId pair) {
            try {
                sndr.send(delta, transmissionParams(snpName, cacheDirName, pair), TransmissionPolicy.CHUNK);

                if (log.isInfoEnabled())
                    log.info("Delta pages storage has been send [part=" + delta.getName() + ", pair=" + pair + ']');
            }
            catch (TransmissionCancelledException e) {
                if (log.isInfoEnabled()) {
                    log.info("Transmission delta pages has been interrupted [part=" + delta.getName() +
                        ", pair=" + pair + ']');
                }
            }
            catch (IgniteCheckedException | InterruptedException | IOException e) {
                U.error(log, "Error sending delta file  [part=" + delta.getName() + ", pair=" + pair + ']', e);

                throw new IgniteException(e);
            }
        }

        /**
         * @param cacheDirName Cache directory name.
         * @param pair Cache group id with corresponding partition id.
         * @return Map of params.
         */
        private Map<String, Serializable> transmissionParams(String snpName, String cacheDirName, GroupPartitionId pair) {
            Map<String, Serializable> params = new HashMap<>();

            params.put(SNP_GRP_ID_PARAM, pair.getGroupId());
            params.put(SNP_PART_ID_PARAM, pair.getPartitionId());
            params.put(SNP_DB_NODE_PATH_PARAM, dbNodePath);
            params.put(SNP_CACHE_DIR_NAME_PARAM, cacheDirName);
            params.put(SNP_NAME_PARAM, snpName);

            return params;
        }

        /** {@inheritDoc} */
        @Override public void close0(@Nullable Throwable th) {
            try {
                if (th != null && !sndr.opened())
                    errHnd.accept(th.getMessage());
            }
            catch (IgniteCheckedException e) {
                th.addSuppressed(e);
            }

            U.closeQuiet(sndr);

            if (th == null) {
                if (log.isInfoEnabled())
                    log.info("The remote snapshot sender closed normally [snpName=" + snpName + ']');
            }
            else {
                U.warn(log, "The remote snapshot sender closed due to an error occurred while processing " +
                    "snapshot operation [snpName=" + snpName + ']', th);
            }
        }
    }

    /**
     *
     */
    private static class LocalSnapshotSender extends SnapshotSender {
        /**
         * Local node snapshot directory calculated on snapshot directory.
         */
        private final File dbNodeSnpDir;

        /** Facotry to produce IO interface over a file. */
        private final FileIOFactory ioFactory;

        /** Factory to create page store for restore. */
        private final BiFunction<Integer, Boolean, FilePageStoreFactory> storeFactory;

        /** Store binary files. */
        private final BinaryTypeWriter binaryWriter;

        /** Marshaller mapping writer. */
        private final MarshallerMappingWriter mappingWriter;

        /** Size of page. */
        private final int pageSize;

        /**
         * @param log Ignite logger to use.
         * @param snpDir Local node snapshot directory.
         * @param ioFactory Facotry to produce IO interface over a file.
         * @param storeFactory Factory to create page store for restore.
         * @param pageSize Size of page.
         */
        public LocalSnapshotSender(
            IgniteLogger log,
            File snpDir,
            FileIOFactory ioFactory,
            BiFunction<Integer, Boolean, FilePageStoreFactory> storeFactory,
            BinaryTypeWriter binaryWriter,
            MarshallerMappingWriter mappingWriter,
            int pageSize
        ) {
            super(log);

            dbNodeSnpDir = snpDir;
            this.ioFactory = ioFactory;
            this.storeFactory = storeFactory;
            this.pageSize = pageSize;
            this.binaryWriter = binaryWriter;
            this.mappingWriter = mappingWriter;
        }

        /** {@inheritDoc} */
        @Override public void sendCacheConfig0(File ccfg, String cacheDirName) {
            try {
                File cacheDir = U.resolveWorkDirectory(dbNodeSnpDir.getAbsolutePath(), cacheDirName, false);

                copy(ccfg, new File(cacheDir, ccfg.getName()), ccfg.length());
            }
            catch (IgniteCheckedException | IOException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void sendMarshallerMeta0(List<Map<Integer, MappedName>> mappings) {
            if (mappings == null)
                return;

            for (int platformId = 0; platformId < mappings.size(); platformId++) {
                Map<Integer, MappedName> cached = mappings.get(platformId);

                try {
                    addPlatformMappings((byte)platformId,
                        cached,
                        (typeId, clsName) -> true,
                        (typeId, mapping) -> {
                        },
                        mappingWriter);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void sendBinaryMeta0(Map<Integer, BinaryType> types) {
            if (types == null)
                return;

            for (Map.Entry<Integer, BinaryType> e : types.entrySet())
                binaryWriter.writeMeta(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long len) {
            try {
                if (len == 0)
                    return;

                File cacheDir = U.resolveWorkDirectory(dbNodeSnpDir.getAbsolutePath(), cacheDirName, false);

                File snpPart = new File(cacheDir, part.getName());

                if (!snpPart.exists() || snpPart.delete())
                    snpPart.createNewFile();

                copy(part, snpPart, len);

                if (log.isInfoEnabled()) {
                    log.info("Partition has been snapshotted [snapshotDir=" + dbNodeSnpDir.getAbsolutePath() +
                        ", cacheDirName=" + cacheDirName + ", part=" + part.getName() +
                        ", length=" + part.length() + ", snapshot=" + snpPart.getName() + ']');
                }
            }
            catch (IOException | IgniteCheckedException ex) {
                throw new IgniteException(ex);
            }
        }

        /** {@inheritDoc} */
        @Override public void sendDelta0(File delta, String cacheDirName, GroupPartitionId pair) {
            File snpPart = getPartitionFile(dbNodeSnpDir, cacheDirName, pair.getPartitionId());

            if (log.isInfoEnabled()) {
                log.info("Start partition snapshot recovery with the given delta page file [part=" + snpPart +
                    ", delta=" + delta + ']');
            }

            try (FileIO fileIo = ioFactory.create(delta, READ);
                 FilePageStore pageStore = (FilePageStore)storeFactory
                     .apply(pair.getGroupId(), false)
                     .createPageStore(getFlagByPartId(pair.getPartitionId()),
                         snpPart::toPath,
                         new LongAdderMetric("NO_OP", null))
            ) {
                ByteBuffer pageBuf = ByteBuffer.allocate(pageSize)
                    .order(ByteOrder.nativeOrder());

                long totalBytes = fileIo.size();

                assert totalBytes % pageSize == 0 : "Given file with delta pages has incorrect size: " + fileIo.size();

                pageStore.beginRecover();

                for (long pos = 0; pos < totalBytes; pos += pageSize) {
                    long read = fileIo.readFully(pageBuf, pos);

                    assert read == pageBuf.capacity();

                    pageBuf.flip();

                    long pageId = PageIO.getPageId(pageBuf);

                    int crc32 = FastCrc.calcCrc(pageBuf, pageBuf.limit());

                    int crc = PageIO.getCrc(pageBuf);

                    if (log.isDebugEnabled()) {
                        log.debug("Read page given delta file [path=" + delta.getName() +
                            ", pageId=" + pageId + ", pos=" + pos + ", pages=" + (totalBytes / pageSize) +
                            ", crcBuff=" + crc32 + ", crcPage=" + crc + ']');
                    }

                    pageBuf.rewind();

                    pageStore.write(PageIO.getPageId(pageBuf), pageBuf, 0, false);

                    pageBuf.flip();
                }

                pageStore.finishRecover();
            }
            catch (IOException | IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override protected void close0(@Nullable Throwable th) {
            if (th == null) {
                if (log.isInfoEnabled())
                    log.info("Local snapshot sender closed, resouces released [dbNodeSnpDir=" + dbNodeSnpDir + ']');
            }
            else
                U.error(log, "Local snapshot sender closed due to an error occurred", th);
        }

        /**
         * @param from Copy from file.
         * @param to Copy data to file.
         * @param length Number of bytes to copy from beginning.
         * @throws IOException If fails.
         */
        private void copy(File from, File to, long length) throws IOException {
            try (FileIO src = ioFactory.create(from, READ);
                 FileChannel dest = new FileOutputStream(to).getChannel()) {
                if (src.size() < length)
                    throw new IgniteException("The source file to copy has to enought length [expected=" + length + ", actual=" + src.size() + ']');

                src.position(0);

                long written = 0;

                while (written < length)
                    written += src.transferTo(written, length - written, dest);
            }
        }
    }
}

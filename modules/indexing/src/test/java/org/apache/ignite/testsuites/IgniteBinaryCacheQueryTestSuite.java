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

package org.apache.ignite.testsuites;

import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.IndexingCachePartitionLossPolicySelfTest;
import org.apache.ignite.testframework.IgniteTestSuite;

/**
 * Test suite for cache queries.
 */
public class IgniteBinaryCacheQueryTestSuite extends TestSuite {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        IgniteTestSuite suite = new IgniteTestSuite("Ignite Cache Queries Test Suite");

//        suite.addTestSuite(AffinityKeyNameAndValueFieldNameConflictTest.class);
//        suite.addTestSuite(DmlInsideTransactionTest.class);
//        suite.addTestSuite(ComplexPrimaryKeyUnwrapSelfTest.class);
//        suite.addTestSuite(SqlNestedQuerySelfTest.class);
//        suite.addTestSuite(ExplainSelfTest.class);
//        suite.addTestSuite(RunningQueriesTest.class);
//
//        suite.addTestSuite(ComplexPrimaryKeyUnwrapSelfTest.class);
//
//        suite.addTestSuite(PartitionedSqlTest.class);
//        suite.addTestSuite(ReplicatedSqlTest.class);
//
//        suite.addTestSuite(SqlParserCreateIndexSelfTest.class);
//        suite.addTestSuite(SqlParserDropIndexSelfTest.class);
//        suite.addTestSuite(SqlParserTransactionalKeywordsSelfTest.class);
//        suite.addTestSuite(SqlParserBulkLoadSelfTest.class);
//        suite.addTestSuite(SqlParserSetStreamingSelfTest.class);
//
//        suite.addTestSuite(SqlConnectorConfigurationValidationSelfTest.class);
//        suite.addTestSuite(ClientConnectorConfigurationValidationSelfTest.class);
//
//        suite.addTestSuite(SqlSchemaSelfTest.class);
//        suite.addTestSuite(SqlIllegalSchemaSelfTest.class);
//        suite.addTestSuite(MultipleStatementsSqlQuerySelfTest.class);
//
//        suite.addTestSuite(BasicIndexTest.class);
//
//        // Misc tests.
//        suite.addTestSuite(QueryEntityValidationSelfTest.class);
//        suite.addTestSuite(DuplicateKeyValueClassesSelfTest.class);
//        suite.addTestSuite(GridCacheLazyQueryPartitionsReleaseTest.class);
//
//        // Dynamic index create/drop tests.
//        suite.addTestSuite(SchemaExchangeSelfTest.class);
//
//        suite.addTestSuite(DynamicIndexServerCoordinatorBasicSelfTest.class);
//        suite.addTestSuite(DynamicIndexServerBasicSelfTest.class);
//        suite.addTestSuite(DynamicIndexServerNodeFilterCoordinatorBasicSelfTest.class);
//        suite.addTestSuite(DynamicIndexServerNodeFIlterBasicSelfTest.class);
//        suite.addTestSuite(DynamicIndexClientBasicSelfTest.class);
//
//        // H2 tests.
//
//        suite.addTestSuite(GridH2IndexingInMemSelfTest.class);
//        suite.addTestSuite(GridH2IndexingOffheapSelfTest.class);
//
//        // Parsing
//        suite.addTestSuite(GridQueryParsingTest.class);
//        suite.addTestSuite(IgniteCacheSqlQueryErrorSelfTest.class);
//
//        // Config.
//        suite.addTestSuite(IgniteCacheDuplicateEntityConfigurationSelfTest.class);
//        suite.addTestSuite(IncorrectQueryEntityTest.class);
//        suite.addTestSuite(IgniteDynamicSqlRestoreTest.class);
//
//        // Queries tests.
//        suite.addTestSuite(LazyQuerySelfTest.class);
//        suite.addTestSuite(IgniteSqlSplitterSelfTest.class);
//        suite.addTestSuite(SqlPushDownFunctionTest.class);
//        suite.addTestSuite(IgniteSqlSegmentedIndexSelfTest.class);
//        suite.addTestSuite(IgniteCachelessQueriesSelfTest.class);
//        suite.addTestSuite(IgniteSqlSegmentedIndexMultiNodeSelfTest.class);
//        suite.addTestSuite(IgniteSqlSchemaIndexingTest.class);
//        suite.addTestSuite(GridCacheQueryIndexDisabledSelfTest.class);
//        suite.addTestSuite(IgniteCacheQueryLoadSelfTest.class);
//        suite.addTestSuite(IgniteCacheLocalQuerySelfTest.class);
//        suite.addTestSuite(IgniteCacheLocalAtomicQuerySelfTest.class);
//        suite.addTestSuite(IgniteCacheReplicatedQuerySelfTest.class);
//        suite.addTestSuite(IgniteCacheReplicatedQueryP2PDisabledSelfTest.class);
//        suite.addTestSuite(IgniteCacheReplicatedQueryEvtsDisabledSelfTest.class);
//        suite.addTestSuite(IgniteCachePartitionedQuerySelfTest.class);
//        suite.addTestSuite(IgniteCachePartitionedSnapshotEnabledQuerySelfTest.class);
//        suite.addTestSuite(IgniteCacheAtomicQuerySelfTest.class);
//        suite.addTestSuite(IgniteCacheAtomicNearEnabledQuerySelfTest.class);
//        suite.addTestSuite(IgniteCachePartitionedQueryP2PDisabledSelfTest.class);
//        suite.addTestSuite(IgniteCachePartitionedQueryEvtsDisabledSelfTest.class);
//
//        suite.addTestSuite(IgniteCacheUnionDuplicatesTest.class);
//        suite.addTestSuite(IgniteCacheJoinPartitionedAndReplicatedCollocationTest.class);
//        suite.addTestSuite(IgniteClientReconnectCacheQueriesFailoverTest.class);
//        suite.addTestSuite(IgniteErrorOnRebalanceTest.class);
//        suite.addTestSuite(CacheQueryBuildValueTest.class);
//        suite.addTestSuite(CacheOffheapBatchIndexingMultiTypeTest.class);
//
//        suite.addTestSuite(IgniteCacheQueryIndexSelfTest.class);
//        suite.addTestSuite(IgniteCacheCollocatedQuerySelfTest.class);
//        suite.addTestSuite(IgniteCacheLargeResultSelfTest.class);
//        suite.addTestSuite(GridCacheQueryInternalKeysSelfTest.class);
//        suite.addTestSuite(H2ResultSetIteratorNullifyOnEndSelfTest.class);
//        suite.addTestSuite(IgniteSqlBigIntegerKeyTest.class);
//        suite.addTestSuite(IgniteCacheOffheapEvictQueryTest.class);
//        suite.addTestSuite(IgniteCacheOffheapIndexScanTest.class);
//
//        suite.addTestSuite(GridCacheCrossCacheQuerySelfTest.class);
//        suite.addTestSuite(GridCacheQuerySerializationSelfTest.class);
//        suite.addTestSuite(IgniteBinaryObjectFieldsQuerySelfTest.class);
//        suite.addTestSuite(IgniteStableBaselineBinObjFieldsQuerySelfTest.class);
//        suite.addTestSuite(IgniteBinaryWrappedObjectFieldsQuerySelfTest.class);
//        suite.addTestSuite(IgniteCacheQueryH2IndexingLeakTest.class);
//        suite.addTestSuite(IgniteCacheQueryNoRebalanceSelfTest.class);
//        suite.addTestSuite(GridCacheQueryTransformerSelfTest.class);
//        suite.addTestSuite(CacheScanQueryFailoverTest.class);
//        suite.addTestSuite(IgniteCachePrimitiveFieldsQuerySelfTest.class);
//
//        suite.addTestSuite(IgniteCacheJoinQueryWithAffinityKeyTest.class);
//        suite.addTestSuite(IgniteCacheJoinPartitionedAndReplicatedTest.class);
//        suite.addTestSuite(IgniteCrossCachesJoinsQueryTest.class);
//
//        suite.addTestSuite(IgniteCacheMultipleIndexedTypesTest.class);
//
//        // DML.
//        suite.addTestSuite(IgniteCacheMergeSqlQuerySelfTest.class);
//        suite.addTestSuite(IgniteCacheInsertSqlQuerySelfTest.class);
//        suite.addTestSuite(IgniteCacheUpdateSqlQuerySelfTest.class);
//        suite.addTestSuite(IgniteCacheDeleteSqlQuerySelfTest.class);
//        suite.addTestSuite(IgniteSqlSkipReducerOnUpdateDmlSelfTest.class);
//        suite.addTestSuite(IgniteSqlSkipReducerOnUpdateDmlFlagSelfTest.class);
//
//        suite.addTestSuite(IgniteBinaryObjectQueryArgumentsTest.class);
//        suite.addTestSuite(IgniteBinaryObjectLocalQueryArgumentsTest.class);
//
//        suite.addTestSuite(IndexingSpiQuerySelfTest.class);
//        suite.addTestSuite(IndexingSpiQueryTxSelfTest.class);
//
//        suite.addTestSuite(IgniteCacheMultipleIndexedTypesTest.class);
//        suite.addTestSuite(IgniteSqlQueryMinMaxTest.class);
//
//        suite.addTestSuite(GridCircularQueueTest.class);
//        suite.addTestSuite(IndexingSpiQueryWithH2IndexingSelfTest.class);
//
//        // DDL.
//        suite.addTestSuite(H2DynamicIndexTransactionalReplicatedSelfTest.class);
//        suite.addTestSuite(H2DynamicIndexTransactionalPartitionedSelfTest.class);
//        suite.addTestSuite(H2DynamicIndexTransactionalPartitionedNearSelfTest.class);
//        suite.addTestSuite(H2DynamicIndexAtomicReplicatedSelfTest.class);
//        suite.addTestSuite(H2DynamicIndexAtomicPartitionedSelfTest.class);
//        suite.addTestSuite(H2DynamicIndexAtomicPartitionedNearSelfTest.class);
//        suite.addTestSuite(H2DynamicTableSelfTest.class);
//        suite.addTestSuite(H2DynamicColumnsClientBasicSelfTest.class);
//        suite.addTestSuite(H2DynamicColumnsServerBasicSelfTest.class);
//        suite.addTestSuite(H2DynamicColumnsServerCoordinatorBasicSelfTest.class);
//
//        // DML+DDL.
//        suite.addTestSuite(H2DynamicIndexingComplexClientAtomicPartitionedTest.class);
//        suite.addTestSuite(H2DynamicIndexingComplexClientAtomicPartitionedNoBackupsTest.class);
//        suite.addTestSuite(H2DynamicIndexingComplexClientAtomicReplicatedTest.class);
//        suite.addTestSuite(H2DynamicIndexingComplexClientTransactionalPartitionedTest.class);
//        suite.addTestSuite(H2DynamicIndexingComplexClientTransactionalPartitionedNoBackupsTest.class);
//        suite.addTestSuite(H2DynamicIndexingComplexClientTransactionalReplicatedTest.class);
//        suite.addTestSuite(H2DynamicIndexingComplexServerAtomicPartitionedTest.class);
//        suite.addTestSuite(H2DynamicIndexingComplexServerAtomicPartitionedNoBackupsTest.class);
//        suite.addTestSuite(H2DynamicIndexingComplexServerAtomicReplicatedTest.class);
//        suite.addTestSuite(H2DynamicIndexingComplexServerTransactionalPartitionedTest.class);
//        suite.addTestSuite(H2DynamicIndexingComplexServerTransactionalPartitionedNoBackupsTest.class);
//        suite.addTestSuite(H2DynamicIndexingComplexServerTransactionalReplicatedTest.class);
//
//        suite.addTestSuite(DdlTransactionSelfTest.class);
//
//        // Fields queries.
//        suite.addTestSuite(SqlFieldsQuerySelfTest.class);
//        suite.addTestSuite(IgniteCacheLocalFieldsQuerySelfTest.class);
//        suite.addTestSuite(IgniteCacheReplicatedFieldsQuerySelfTest.class);
//        suite.addTestSuite(IgniteCacheReplicatedFieldsQueryROSelfTest.class);
//        suite.addTestSuite(IgniteCacheReplicatedFieldsQueryP2PEnabledSelfTest.class);
//        suite.addTestSuite(IgniteCacheReplicatedFieldsQueryJoinNoPrimaryPartitionsSelfTest.class);
//        suite.addTestSuite(IgniteCachePartitionedFieldsQuerySelfTest.class);
//        suite.addTestSuite(IgniteCacheAtomicFieldsQuerySelfTest.class);
//        suite.addTestSuite(IgniteCacheAtomicNearEnabledFieldsQuerySelfTest.class);
//        suite.addTestSuite(IgniteCachePartitionedFieldsQueryP2PEnabledSelfTest.class);
//        suite.addTestSuite(IgniteCacheFieldsQueryNoDataSelfTest.class);
//        suite.addTestSuite(GridCacheQueryIndexingDisabledSelfTest.class);
//        suite.addTestSuite(GridOrderedMessageCancelSelfTest.class);
//        suite.addTestSuite(CacheQueryEvictDataLostTest.class);
//
//        // Full text queries.
//        suite.addTestSuite(GridCacheFullTextQuerySelfTest.class);
//        suite.addTestSuite(IgniteCacheFullTextQueryNodeJoiningSelfTest.class);
//
//        // Ignite cache and H2 comparison.
//        suite.addTestSuite(BaseH2CompareQueryTest.class);
//        suite.addTestSuite(H2CompareBigQueryTest.class);
//        suite.addTestSuite(H2CompareBigQueryDistributedJoinsTest.class);
//
//        // Cache query metrics.
//        suite.addTestSuite(CacheLocalQueryMetricsSelfTest.class);
//        suite.addTestSuite(CachePartitionedQueryMetricsDistributedSelfTest.class);
//        suite.addTestSuite(CachePartitionedQueryMetricsLocalSelfTest.class);
//        suite.addTestSuite(CacheReplicatedQueryMetricsDistributedSelfTest.class);
//        suite.addTestSuite(CacheReplicatedQueryMetricsLocalSelfTest.class);
//
//        // Cache query metrics.
//        suite.addTestSuite(CacheLocalQueryDetailMetricsSelfTest.class);
//        suite.addTestSuite(CachePartitionedQueryDetailMetricsDistributedSelfTest.class);
//        suite.addTestSuite(CachePartitionedQueryDetailMetricsLocalSelfTest.class);
//        suite.addTestSuite(CacheReplicatedQueryDetailMetricsDistributedSelfTest.class);
//        suite.addTestSuite(CacheReplicatedQueryDetailMetricsLocalSelfTest.class);
//
//        // Unmarshalling query test.
//        suite.addTestSuite(IgniteCacheP2pUnmarshallingQueryErrorTest.class);
//        suite.addTestSuite(IgniteCacheNoClassQuerySelfTest.class);
//
//        // Cancellation.
//        suite.addTestSuite(IgniteCacheDistributedQueryCancelSelfTest.class);
//        suite.addTestSuite(IgniteCacheLocalQueryCancelOrTimeoutSelfTest.class);
//
//        // Distributed joins.
//        suite.addTestSuite(H2CompareBigQueryDistributedJoinsTest.class);
//        suite.addTestSuite(IgniteCacheDistributedJoinCollocatedAndNotTest.class);
//        suite.addTestSuite(IgniteCacheDistributedJoinCustomAffinityMapper.class);
//        suite.addTestSuite(IgniteCacheDistributedJoinNoIndexTest.class);
//        suite.addTestSuite(IgniteCacheDistributedJoinPartitionedAndReplicatedTest.class);
//        suite.addTestSuite(IgniteCacheDistributedJoinQueryConditionsTest.class);
//        suite.addTestSuite(IgniteCacheDistributedJoinTest.class);
//        suite.addTestSuite(IgniteSqlDistributedJoinSelfTest.class);
//        suite.addTestSuite(IgniteSqlQueryParallelismTest.class);
//
//        // Other.
//        suite.addTestSuite(CacheIteratorScanQueryTest.class);
//        suite.addTestSuite(CacheQueryNewClientSelfTest.class);
//        suite.addTestSuite(CacheOffheapBatchIndexingSingleTypeTest.class);
//        suite.addTestSuite(CacheSqlQueryValueCopySelfTest.class);
//        suite.addTestSuite(IgniteCacheQueryCacheDestroySelfTest.class);
//        suite.addTestSuite(IgniteQueryDedicatedPoolTest.class);
//        suite.addTestSuite(IgniteSqlEntryCacheModeAgnosticTest.class);
//        suite.addTestSuite(QueryEntityCaseMismatchTest.class);
//        suite.addTestSuite(IgniteCacheDistributedPartitionQuerySelfTest.class);
//        suite.addTestSuite(IgniteCacheDistributedPartitionQueryNodeRestartsSelfTest.class);
//        suite.addTestSuite(IgniteCacheDistributedPartitionQueryConfigurationSelfTest.class);
//        suite.addTestSuite(IgniteSqlKeyValueFieldsTest.class);
//        suite.addTestSuite(IgniteSqlRoutingTest.class);
//        suite.addTestSuite(IgniteSqlNotNullConstraintTest.class);
//        suite.addTestSuite(LongIndexNameTest.class);
//        suite.addTestSuite(GridCacheQuerySqlFieldInlineSizeSelfTest.class);
//        suite.addTestSuite(IgniteSqlParameterizedQueryTest.class);
//        suite.addTestSuite(H2ConnectionLeaksSelfTest.class);
//        suite.addTestSuite(IgniteCheckClusterStateBeforeExecuteQueryTest.class);
//        suite.addTestSuite(OptimizedMarshallerIndexNameTest.class);
//        suite.addTestSuite(SqlSystemViewsSelfTest.class);
//
//        suite.addTestSuite(GridIndexRebuildSelfTest.class);
//
//        suite.addTestSuite(SqlTransactionCommandsWithMvccDisabledSelfTest.class);
//
//        suite.addTestSuite(IgniteSqlDefaultValueTest.class);
//        suite.addTestSuite(IgniteDecimalSelfTest.class);
//        suite.addTestSuite(IgniteSQLColumnConstraintsTest.class);
//        suite.addTestSuite(IgniteTransactionSQLColumnConstraintTest.class);
//
//        suite.addTestSuite(IgniteCachePartitionedAtomicColumnConstraintsTest.class);
//        suite.addTestSuite(IgniteCachePartitionedTransactionalColumnConstraintsTest.class);
//        suite.addTestSuite(IgniteCachePartitionedTransactionalSnapshotColumnConstraintTest.class);
//        suite.addTestSuite(IgniteCacheReplicatedAtomicColumnConstraintsTest.class);
//        suite.addTestSuite(IgniteCacheReplicatedTransactionalColumnConstraintsTest.class);
//        suite.addTestSuite(IgniteCacheReplicatedTransactionalSnapshotColumnConstraintTest.class);
//
//        // H2 Rows on-heap cache
//        suite.addTestSuite(H2RowCacheSelfTest.class);
//        suite.addTestSuite(H2RowCachePageEvictionTest.class);
//
//        // User operation SQL
//        suite.addTestSuite(SqlParserUserSelfTest.class);
//        suite.addTestSuite(SqlUserCommandSelfTest.class);
//        suite.addTestSuite(EncryptedSqlTableTest.class);
//
//        suite.addTestSuite(ThreadLocalObjectPoolSelfTest.class);
//        suite.addTestSuite(H2StatementCacheSelfTest.class);
//        suite.addTestSuite(PreparedStatementExSelfTest.class);

        // Partition loss.
        for (int i = 0; i < 250; i++)
            suite.addTestSuite(IndexingCachePartitionLossPolicySelfTest.class);

//        // GROUP_CONCAT
//        suite.addTestSuite(IgniteSqlGroupConcatCollocatedTest.class);
//        suite.addTestSuite(IgniteSqlGroupConcatNotCollocatedTest.class);
//
//        // Binary
//        suite.addTestSuite(BinarySerializationQuerySelfTest.class);
//        suite.addTestSuite(BinarySerializationQueryWithReflectiveSerializerSelfTest.class);
//        suite.addTestSuite(IgniteCacheBinaryObjectsScanSelfTest.class);
//        suite.addTestSuite(IgniteCacheBinaryObjectsScanWithEventsSelfTest.class);
//        suite.addTestSuite(BigEntryQueryTest.class);
//        suite.addTestSuite(BinaryMetadataConcurrentUpdateWithIndexesTest.class);
//
//        // Partition pruning.
//        suite.addTestSuite(InOperationExtractPartitionSelfTest.class);
//        suite.addTestSuite(AndOperationExtractPartitionSelfTest.class);
//
//        suite.addTestSuite(GridCacheDynamicLoadOnClientTest.class);
//        suite.addTestSuite(GridCacheDynamicLoadOnClientPersistentTest.class);

        return suite;
    }
}

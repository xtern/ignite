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

namespace Apache.Ignite.Core.Transactions
{
    using System;

    /// <summary>
    /// Transaction metrics, shared across all caches.
    /// </summary>
    public interface ITransactionMetrics
    {
        /// <summary>
        /// Gets the last time transaction was committed.
        /// </summary>
        DateTime CommitTime { get; }

        /// <summary>
        /// Gets the last time transaction was rolled back.
        /// </summary>
        DateTime RollbackTime { get; }

        /// <summary>
        /// Gets the total number of transaction commits.
        /// </summary>
        int TxCommits { get; }

        /// <summary>
        /// Gets the total number of transaction rollbacks.
        /// </summary>
        int TxRollbacks { get; }

        /// <summary>
        /// Gets the total number of transaction rollbacks due to timeout.
        /// </summary>
        int TxRollbacksOnTimeout { get; }

        /// <summary>
        /// Gets the total number of transaction rollbacks due to deadlock.
        /// </summary>
        int TxRollbacksOnDeadlock { get; }
    }
}
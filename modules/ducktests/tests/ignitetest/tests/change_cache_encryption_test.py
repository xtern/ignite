# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This module contains PME free switch tests.
"""

import time

from ducktape.mark.resource import cluster

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.utils import ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


class ChangeCacheEnryptionTest(IgniteTest):
    """
    Tests PME free switch scenarios.
    """
    NUM_NODES = 3

    DATA_AMOUNT = 40_000_000

    PRELOAD_TIMEOUT = 300

    CACHE_NAME = "test-cache"

    DATAGEN_THREADS_COUNT = 6

    @cluster(num_nodes=NUM_NODES + 2)
    @ignite_versions(str(DEV_BRANCH))
    def test_encrypt(self, ignite_version):
        return self._perform_load(ignite_version, True)

    @cluster(num_nodes=NUM_NODES + 2)
    @ignite_versions(str(DEV_BRANCH))
    def test_idle(self, ignite_version):
        return self._perform_load(ignite_version, False)

    def _perform_load(self, ignite_version, encrypt):
        """
        Tests TDE encryption cache key rotation latency.
        """
        data = {}

        config = IgniteConfiguration(
            cluster_state="INACTIVE",
            version=IgniteVersion(ignite_version),
            caches=[CacheConfiguration(
                name=self.CACHE_NAME,
                backups=2,
                atomicity_mode='TRANSACTIONAL',
                encryption_enabled=True)
            ],
            data_storage=DataStorageConfiguration(
                checkpoint_frequency=10 * 1000,
                default=DataRegionConfiguration(name='persistent', persistent=True)
            )
        )

        ignites = IgniteService(self.test_context, config, num_nodes=self.NUM_NODES)

        ignites.start()

        client_config = config._replace(client_mode=True,
                                        discovery_spi=from_ignite_cluster(ignites, slice(0, self.NUM_NODES - 1)))

        control_utility = ControlUtility(ignites, self.test_context)

        control_utility.activate()

        IgniteApplicationService(self.test_context, client_config,
                                 java_class_name="org.apache.ignite.internal.ducktest.tests.DataGenerationApplication",
                                 params={"cacheName": self.CACHE_NAME,
                                         "range": self.DATA_AMOUNT,
                                         "threads_count": self.DATAGEN_THREADS_COUNT},
                                 timeout_sec=self.PRELOAD_TIMEOUT).run()

        if encrypt:
            control_utility.change_cache_key(self.CACHE_NAME)

        load_generator = IgniteApplicationService(
            self.test_context,
            client_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.LoadGeneratorApplication",
            params={"cacheName": self.CACHE_NAME, "warmup": 100, "range": 1_000_000, "threads": 2})

        load_generator.start()

        time.sleep(30)

        if encrypt:
            ignites.await_event("Cache group reencryption is finished", 300, from_the_beginning=True)

        load_generator.stop()

        data["Worst latency (ms)"] = load_generator.extract_result("WORST_LATENCY")
        data["Streamed txs"] = load_generator.extract_result("STREAMED")
        data["Measure duration (ms)"] = load_generator.extract_result("AVG_OPERATION")

        return data
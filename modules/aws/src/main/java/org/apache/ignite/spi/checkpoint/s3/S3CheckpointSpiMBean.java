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

package org.apache.ignite.spi.checkpoint.s3;

import org.apache.ignite.mxbean.MXBeanDescription;
import org.apache.ignite.spi.IgniteSpiManagementMBean;

/**
 * Management bean for {@link S3CheckpointSpi}.
 */
@MXBeanDescription("MBean that provides access to S3 checkpoint SPI configuration.")
public interface S3CheckpointSpiMBean extends IgniteSpiManagementMBean {
    /**
     * Gets S3 bucket name to use.
     *
     * @return S3 bucket name to use.
     */
    @MXBeanDescription("S3 bucket name.")
    public String getBucketName();

    /**
     * @return S3 bucket endpoint.
     */
    @MXBeanDescription("S3 bucket endpoint.")
    public String getBucketEndpoint();

    /**
     * @return S3 server-side encryption algorithm.
     */
    @MXBeanDescription("S3 server-side encryption algorithm.")
    public String getSSEAlgorithm();

    /**
     * @return S3 access key.
     */
    @MXBeanDescription("S3 access key.")
    public String getAccessKey();

    /**
     * @return HTTP proxy host.
     */
    @MXBeanDescription("HTTP proxy host.")
    public String getProxyHost();

    /**
     * @return HTTP proxy port
     */
    @MXBeanDescription("HTTP proxy port.")
    public int getProxyPort();

    /**
     * @return HTTP proxy user name.
     */
    @MXBeanDescription("HTTP proxy user name.")
    public String getProxyUsername();
}

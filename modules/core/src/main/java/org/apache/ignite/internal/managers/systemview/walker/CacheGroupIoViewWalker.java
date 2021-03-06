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

package org.apache.ignite.internal.managers.systemview.walker;

import org.apache.ignite.spi.systemview.view.CacheGroupIoView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;

/**
 * Generated by {@code org.apache.ignite.codegen.SystemViewRowAttributeWalkerGenerator}.
 * {@link CacheGroupIoView} attributes walker.
 * 
 * @see CacheGroupIoView
 */
public class CacheGroupIoViewWalker implements SystemViewRowAttributeWalker<CacheGroupIoView> {
    /** {@inheritDoc} */
    @Override public void visitAll(AttributeVisitor v) {
        v.accept(0, "cacheGroupId", int.class);
        v.accept(1, "cacheGroupName", String.class);
        v.accept(2, "physicalReads", long.class);
        v.accept(3, "logicalReads", long.class);
    }

    /** {@inheritDoc} */
    @Override public void visitAll(CacheGroupIoView row, AttributeWithValueVisitor v) {
        v.acceptInt(0, "cacheGroupId", row.cacheGroupId());
        v.accept(1, "cacheGroupName", String.class, row.cacheGroupName());
        v.acceptLong(2, "physicalReads", row.physicalReads());
        v.acceptLong(3, "logicalReads", row.logicalReads());
    }

    /** {@inheritDoc} */
    @Override public int count() {
        return 4;
    }
}

/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.collect.sources;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.TableIdent;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.reference.sys.RowContextReferenceResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

@Singleton
public class SysTableRegistry {

    private final SysSchemaInfo sysSchemaInfo;
    private final SystemCollectSource systemCollectSource;

    @Inject
    public SysTableRegistry(SysSchemaInfo sysSchemaInfo,
                            SystemCollectSource systemCollectSource) {
        this.sysSchemaInfo = sysSchemaInfo;
        this.systemCollectSource = systemCollectSource;
    }

    public void registerSysTable(TableInfo tableInfo, Supplier<CompletableFuture<? extends Iterable<?>>> iterableSupplier,
                                 Map<ColumnIdent, RowCollectExpressionFactory> expressionFactories) {
        TableIdent ident = tableInfo.ident();
        sysSchemaInfo.registerSysTable(tableInfo);
        systemCollectSource.registerIterableGetter(ident.fqn(), iterableSupplier);
        RowContextReferenceResolver.INSTANCE.registerTableFactory(ident, expressionFactories);
    }

}

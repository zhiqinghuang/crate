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

package io.crate.analyze;


import io.crate.data.Row;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.tree.AlterTableRename;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.Table;

import javax.annotation.Nullable;
import java.util.List;

class AlterTableRenameAnalyzer {

    private final Schemas schemas;

    AlterTableRenameAnalyzer(Schemas schemas) {
        this.schemas = schemas;
    }

    public AlterTableRenameAnalyzedStatement analyze(AlterTableRename node, Row parameters, String defaultSchema) {
        Table table = node.table();
        DocTableInfo tableInfo = schemas.getWritableTable(TableIdent.of(table, defaultSchema));
        PartitionName partitionName = getPartitionName(table.partitionProperties(), tableInfo, parameters);
        return new AlterTableRenameAnalyzedStatement(tableInfo, partitionName, node.newName(), table.excludePartitions());
    }

    @Nullable
    private static PartitionName getPartitionName(List<Assignment> partitionsProperties,
                                                  DocTableInfo tableInfo,
                                                  Row parameters) {
        if (partitionsProperties.isEmpty()) {
            return null;
        }
        PartitionName partitionName = PartitionPropertiesAnalyzer.toPartitionName(
            tableInfo,
            partitionsProperties,
            parameters
        );
        if (tableInfo.partitions().contains(partitionName) == false) {
            throw new IllegalArgumentException("Referenced partition \"" + partitionName + "\" does not exist.");
        }
        return partitionName;
    }
}

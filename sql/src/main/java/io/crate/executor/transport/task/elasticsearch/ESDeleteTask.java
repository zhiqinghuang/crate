/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.executor.transport.task.elasticsearch;

import io.crate.Constants;
import io.crate.action.FutureActionListener;
import io.crate.analyze.where.DocKeys;
import io.crate.concurrent.CompletableFutures;
import io.crate.data.BatchConsumer;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowsBatchIterator;
import io.crate.exceptions.Exceptions;
import io.crate.exceptions.SQLExceptions;
import io.crate.executor.Executor;
import io.crate.executor.JobTask;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.node.dml.ESDelete;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.index.engine.VersionConflictEngineException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ESDeleteTask extends JobTask {

    private final TransportDeleteAction deleteAction;
    private final ESDelete esDelete;

    public ESDeleteTask(ESDelete esDelete, TransportDeleteAction deleteAction) {
        super(esDelete.jobId());
        this.esDelete = esDelete;
        this.deleteAction = deleteAction;
    }

    @Override
    public void execute(final BatchConsumer consumer, Row parameters) {
        List<CompletableFuture<Long>> futures = createAndSendRequests();
        CompletableFutures.allAsList(futures).whenComplete((r, t) -> {
            if (t == null) {
                consumer.accept(RowsBatchIterator.newInstance(new Row1(sumRowCount(r))), null);
            } else {
                consumer.accept(null, t);
            }
        });
    }

    private List<CompletableFuture<Long>> createAndSendRequests() {
        List<CompletableFuture<Long>> futures = new ArrayList<>(esDelete.docKeys().size());
        for (DocKeys.DocKey docKey : esDelete.docKeys()) {
            DeleteRequest deleteRequest = deleteRequestFromDocKey(esDelete.tableInfo(), docKey);
            FutureActionListener<DeleteResponse, Long> listener = new FutureActionListener<>(r -> {
                if (r.isFound()) {
                    return 1L;
                }
                return 0L;
            });
            deleteAction.execute(deleteRequest, listener);
            futures.add(listener.exceptionally(ESDeleteTask::treatVersionConflictAsNoMatch));
        }
        return futures;
    }

    private static long treatVersionConflictAsNoMatch(Throwable t) {
        t = SQLExceptions.unwrap(t);
        if (t instanceof VersionConflictEngineException) {
            return 0L;
        }
        Exceptions.rethrowUnchecked(t);
        return 0L;
    }

    private static long sumRowCount(List<Long> r) {
        long rowCount = 0;
        for (Long count : r) {
            rowCount += count;
        }
        return rowCount;
    }

    private static DeleteRequest deleteRequestFromDocKey(DocTableInfo table, DocKeys.DocKey docKey) {
        DeleteRequest deleteRequest = new DeleteRequest(
            ESGetTask.indexName(table, docKey.partitionValues().orNull()),
            Constants.DEFAULT_MAPPING_TYPE,
            docKey.id()
        );
        deleteRequest.routing(docKey.routing());
        if (docKey.version().isPresent()) {
            deleteRequest.version(docKey.version().get());
        }
        return deleteRequest;
    }

    @Override
    public final List<CompletableFuture<Long>> executeBulk() {
        List<CompletableFuture<Long>> results = createAndSendRequests();
        swallowExceptionsToErrorRowCount(results);

        List<CompletableFuture<Long>> bulkResults = new ArrayList<>(esDelete.getBulkSize());
        Map<Integer, Integer> itemToBulkIdx = esDelete.getItemToBulkIdx();
        CompletableFutures.allAsList(results).whenComplete((rowCounts, t) -> {
            if (t == null) {
                long[] resultRowCounts = new long[esDelete.getBulkSize()];
                Arrays.fill(resultRowCounts, 0L);
                for (int i = 0; i < rowCounts.size(); i++) {
                    Long rowCount = rowCounts.get(i);
                    Integer resultIdx = itemToBulkIdx.get(i);
                    resultRowCounts[resultIdx] += rowCount;
                }

                for (int i = 0; i < bulkResults.size(); i++) {
                    bulkResults.get(i).complete(resultRowCounts[i]);
                }
            } else {
                for (CompletableFuture<Long> bulkResult : bulkResults) {
                    bulkResult.complete(Executor.ROWCOUNT_ERROR);
                }
            }
        });
        return bulkResults;
    }

    private static void swallowExceptionsToErrorRowCount(List<CompletableFuture<Long>> results) {
        for (int i = 0; i < results.size(); i++) {
            results.set(i, results.get(i).exceptionally(t -> Executor.ROWCOUNT_ERROR));
        }
    }
}

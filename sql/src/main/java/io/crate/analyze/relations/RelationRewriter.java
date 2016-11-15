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

package io.crate.analyze.relations;

import com.google.common.base.Optional;
import io.crate.analyze.*;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.collections.Lists2;
import io.crate.metadata.ReplaceMode;
import io.crate.metadata.ReplacingSymbolVisitor;
import io.crate.operation.operator.AndOperator;
import io.crate.planner.Limits;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Rewrite relation by trying to collapse parent/childs into one relation
 * <pre>
 *     select sum(x) from (select x) --> select sum(x)
 * </pre>
 *
 * Execution order of a SELECT:
 *
 *  1. where            pushdown if no limit/orderBy on child
 *                      can be merged into having or, if no aggregations on child, into where
 *  2. aggregations     pushdown if no limit/orderBy/having on child
 *  3. having           pushdown if no limit/orderBy/ on child
 *  4. order by         pushdown if no limit on child or if equal ordering
 *  5. limit            pushdown if no nothing else left on parent
 *
 */
public class RelationRewriter {

    private final static Visitor VISITOR = new Visitor();
    private final static FieldReplacer FIELD_REPLACER = new FieldReplacer();

    public RelationRewriter() {
    }

    public static AnalyzedRelation rewrite(AnalyzedRelation relation) {
        QueriedRelation queriedRelation = VISITOR.process(relation, null);
        if (queriedRelation == null) {
            return relation;
        }
        return queriedRelation;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static class MergeQS {

        private final QuerySpec querySpec;
        private final FieldResolver fieldResolver;

        MergeQS(QuerySpec querySpec, FieldResolver fieldResolver) {
            this.querySpec = querySpec;
            this.fieldResolver = fieldResolver;
        }

        /**
         * Try to merge an order by clause into the child querySpec.
         *
         * This may actually affect the ordering of the execution slightly as the ORDER BY may be moved BEFORE a limit:
         *
         * <pre>
         *  select * from (select * from t1 limit 10 offset 5) as tt order by x
         *  ->
         *  select * from t1 order by x limit 10 offset 5
         *
         *
         *  select * from (select * from t1 limit 10 offset 5) as tt order by x limit 2
         *  ->
         *  select * from t1 order by x limit least(10, 2) offset 5
         *
         * </pre>
         */
        boolean tryMerge(OrderBy orderBy) {
            Optional<OrderBy> currentOrderBy = querySpec.orderBy();
            if (querySpec.limit().isPresent()) {
                if (currentOrderBy.isPresent()) {
                    OrderBy copyAndReplace = orderBy.copyAndReplace(i -> FIELD_REPLACER.process(i, fieldResolver));
                    return copyAndReplace.equals(currentOrderBy.get());
                }
            }
            orderBy.replace(i -> FIELD_REPLACER.process(i, fieldResolver));
            querySpec.orderBy(orderBy);
            return true;
        }

        boolean tryMergeHaving(Optional<HavingClause> having) {
            if (!having.isPresent()) {
                return true;
            }
            if (querySpec.limit().isPresent() || querySpec.orderBy().isPresent()) {
                return false;
            }
            assert !querySpec.having().isPresent() : "Cannot merge having";

            HavingClause parentHavingClause = having.get();
            if (parentHavingClause.hasQuery()) {
                Symbol parentQuery = FIELD_REPLACER.replace(parentHavingClause.query(), fieldResolver);
                querySpec.having(new HavingClause(parentQuery));
            } else {
                querySpec.having(parentHavingClause);
            }
            return true;
        }

        boolean tryMerge(WhereClause where) {
            if (where.equals(WhereClause.MATCH_ALL)) {
                return true;
            }
            if (querySpec.limit().isPresent() || querySpec.orderBy().isPresent()) {
                return false;
            }
            // TODO: move QueryClause merge logic into QueryClause
            if (querySpec.hasAggregates() || querySpec.groupBy().isPresent()) {
                Optional<HavingClause> having = querySpec.having();
                if (having.isPresent()) {
                    HavingClause havingClause = having.get();
                    if (havingClause.noMatch()) {
                        return true; // keep existing having without adding where because NoMatch & something is still NoMatch
                    }
                    if (where.noMatch()) {
                        querySpec.having(new HavingClause(Literal.BOOLEAN_FALSE));
                        return true;
                    }
                    if (havingClause.hasQuery()) {
                        querySpec.having(new HavingClause(AndOperator.join(
                            havingClause.query(),
                            FIELD_REPLACER.replace(where.query(), fieldResolver))));
                        return true;
                    }
                } else {
                    // TODO: if the whereClause involves only group keys this could be pushed down as whereClause
                    // but not sure if this is the right place to do that optimization?
                    if (where.noMatch()) {
                        querySpec.having(new HavingClause(Literal.BOOLEAN_FALSE));
                    } else {
                        querySpec.having(new HavingClause(FIELD_REPLACER.replace(where.query(), fieldResolver)));
                    }
                    return true;
                }
            }
            WhereClause currentWhere = querySpec.where();
            if (currentWhere.noMatch()) {
                return true;
            }
            if (where.noMatch()) {
                querySpec.where(where);
            }
            assert where.partitions() == null || where.partitions().isEmpty() : "Cannot merge WhereClause with partitions";
            assert !where.docKeys().isPresent() : "Cannot merge WhereClause with docKeys";
            querySpec.where(new WhereClause(AndOperator.join(
                currentWhere.query(),
                FIELD_REPLACER.replace(where.query(), fieldResolver))));
            return true;
        }

        boolean tryMergeAggregations(Optional<List<Symbol>> groupKeys, List<Symbol> outputs, boolean hasAggregations) {
            if (!groupKeys.isPresent() && !hasAggregations) {
                return true;
            }
            if (querySpec.limit().isPresent() || querySpec.orderBy().isPresent() || querySpec.having().isPresent()) {
                return false;
            }
            Optional<List<Symbol>> currentGroupKeys = querySpec.groupBy();
            if (groupKeys.isPresent()) {
                if (!currentGroupKeys.isPresent()) {
                    List<Symbol> groupKeySymbols = groupKeys.get();
                    Lists2.replaceItems(groupKeySymbols, i -> FIELD_REPLACER.replace(i, fieldResolver));
                    querySpec.groupBy(groupKeySymbols);
                    mergeOutputs(outputs);
                    return true;
                }
                return false;
            }

            return false;
        }

        boolean tryMergeLimitAndOffset(Optional<Symbol> limit, Optional<Symbol> offset) {
            querySpec.limit(Limits.mergeMin(querySpec.limit(), limit));
            querySpec.offset(Limits.mergeAdd(querySpec.offset(), offset));
            return true;
        }

        MergeQS mergeOutputs(List<Symbol> outputs) {
            Lists2.replaceItems(outputs, i -> FIELD_REPLACER.replace(i, fieldResolver));
            querySpec.outputs(outputs);
            return this;
        }
    }

    private static class FieldReplacer extends ReplacingSymbolVisitor<FieldResolver> {

        FieldReplacer() {
            super(ReplaceMode.MUTATE);
        }

        @Override
        public Symbol visitField(Field field, FieldResolver context) {
            return context.resolveField(field);
        }

        public Symbol replace(Symbol symbol, FieldResolver fieldResolver) {
            return process(symbol, fieldResolver);
        }
    }

    private static class Visitor extends AnalyzedRelationVisitor<Void, QueriedRelation> {

        @Override
        protected QueriedRelation visitAnalyzedRelation(AnalyzedRelation relation, Void context) {
            return null;
        }

        @Override
        public QueriedRelation visitQueriedSelectRelation(QueriedSelectRelation relation, Void context) {
            QuerySpec qs = relation.querySpec();
            QueriedRelation subRelation = process(relation.subRelation(), context);
            MergeQS mergeQS = new MergeQS(subRelation.querySpec(), new QuerySpecFieldResolver(subRelation));

            if (mergeQS.tryMerge(qs.where())) {
                qs.where(WhereClause.MATCH_ALL);

                Optional<OrderBy> orderByOptional = qs.orderBy();
                Optional<List<Symbol>> groupBy = qs.groupBy();
                if (groupBy.isPresent() || qs.hasAggregates()) {
                    if (mergeQS.tryMergeAggregations(groupBy, qs.outputs(), qs.hasAggregates())) {
                        qs.groupBy(null);
                        qs.hasAggregates(false);
                        boolean mergedHaving = mergeQS.tryMergeHaving(qs.having());
                        assert mergedHaving : "if aggregations can be merged, having merge must be possible as well";
                        qs.having(null);

                        if (orderByOptional.isPresent()) {
                            boolean mergedOrderBy = mergeQS.tryMerge(orderByOptional.get());
                            assert mergedOrderBy : "if aggregations can be merged, order by merge must be possible as well";
                            qs.orderBy(null);
                        }
                        if (mergeQS.tryMergeLimitAndOffset(qs.limit(), qs.offset())) {
                            return subRelation;
                        } else {
                            throw new AssertionError("If aggregations can be merged, limit & offset must work as well");
                        }
                    }
                } else {
                    if (!orderByOptional.isPresent() || mergeQS.tryMerge(orderByOptional.get())) {
                        qs.orderBy(null);
                        if (mergeQS.tryMergeLimitAndOffset(qs.limit(), qs.offset())) {
                            mergeQS.mergeOutputs(qs.outputs());
                            return subRelation;
                        }
                    }
                }
            }
            return relation;
        }

        @Override
        public QueriedRelation visitQueriedDocTable(QueriedDocTable table, Void context) {
            return table;
        }

        @Override
        public QueriedRelation visitMultiSourceSelect(MultiSourceSelect mss, Void context) {
            return mss;
        }
    }

    private static class QuerySpecFieldResolver implements FieldResolver {

        private final QueriedRelation relation;

        QuerySpecFieldResolver(QueriedRelation relation) {
            this.relation = relation;
        }

        @Nullable
        @Override
        public Symbol resolveField(Field field) {
            //assert field.relation() == relation : "field must belong to this relation in order to resolve it";
            return relation.querySpec().outputs().get(field.index());
        }
    }
}

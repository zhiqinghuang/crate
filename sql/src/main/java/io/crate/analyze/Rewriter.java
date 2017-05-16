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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.crate.analyze.relations.*;
import io.crate.analyze.symbol.*;
import io.crate.operation.operator.AndOperator;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;

public class Rewriter {

    /**
     * Rewrite an Outer join to an inner join if possible.
     * <p>
     * Conditions on OUTER tables are not pushed down when a MultiSourceSelect is initially created because
     * the whereClause needs to be checked AFTER the join
     * (because if the join generates NULL rows the whereClause could become TRUE on those NULL rows)
     * <p>
     * See the following two examples where <code>t2</code> is the OUTER table:
     * <p>
     * <pre>
     *     select * from t1
     *          left join t2 on t1.t2_id = t2.id
     *     where
     *          coalesce(t2.x, 20) > 10   # becomes TRUE for null rows
     * </pre>
     * <p>
     * <p>
     * <p>
     * But if we know that the whereClause cannot possible become TRUE then we can push it down
     * (re-writing this into an inner join)
     * <p>
     * This is possible because all rows that are generated by the left-join would be removed again by the whereClause anyway.
     * <p>
     * <pre>
     *     select * from t1
     *          left join t2 on t1.t2_id = t2.id
     *     where
     *          t2.x > 10   # if t2.x is NULL this is always FALSE
     * </pre>
     */
    public static void tryRewriteOuterToInnerJoin(EvaluatingNormalizer normalizer, MultiSourceSelect mss) {
        if (mss.sources().size() > 2) {
            return;
        }
        WhereClause where = mss.querySpec().where();
        if (!where.hasQuery()) {
            return;
        }
        Iterator<Map.Entry<QualifiedName, AnalyzedRelation>> it = mss.sources().entrySet().iterator();
        Map.Entry<QualifiedName, AnalyzedRelation> left = it.next();
        Map.Entry<QualifiedName, AnalyzedRelation> right = it.next();
        QualifiedName leftName = left.getKey();
        QualifiedName rightName = right.getKey();
        JoinPair joinPair = JoinPairs.ofRelationsWithMergedConditions(
            leftName,
            rightName,
            mss.joinPairs(),
            false
        );
        assert leftName.equals(joinPair.left()) : "This JoinPair has a different left Qualified name: " + joinPair.left();
        assert rightName.equals(joinPair.right()) : "This JoinPair has a different left Qualified name: " + joinPair.right();

        JoinType joinType = joinPair.joinType();
        if (!joinType.isOuter()) {
            return;
        }
        tryRewrite(normalizer, mss, where, left, right, joinPair, joinType);
    }

    private static void tryRewrite(EvaluatingNormalizer normalizer,
                                   MultiSourceSelect mss,
                                   WhereClause where,
                                   Map.Entry<QualifiedName, AnalyzedRelation> left,
                                   Map.Entry<QualifiedName, AnalyzedRelation> right,
                                   JoinPair joinPair,
                                   JoinType joinType) {
        final Map<QualifiedName, QueriedRelation> outerRelations = groupOuterRelationQSByName(left, right, joinType);
        Map<Set<QualifiedName>, Symbol> splitQueries = QuerySplitter.split(where.query());
        for (QualifiedName outerRelation : outerRelations.keySet()) {
            Symbol outerRelationQuery = splitQueries.remove(Sets.newHashSet(outerRelation));
            if (outerRelationQuery == null) {
                continue;
            }
            if (couldMatchWithNullValues(normalizer, outerRelationQuery, outerRelation)) {
                splitQueries.put(Sets.newHashSet(outerRelation), outerRelationQuery);
            } else {
                QueriedRelation outerSubRelation = outerRelations.get(outerRelation);
                applyOuterJoinRewrite(
                    joinPair,
                    mss.querySpec(),
                    outerSubRelation,
                    splitQueries,
                    outerRelationQuery
                );
            }
        }
    }

    private static boolean couldMatchWithNullValues(EvaluatingNormalizer normalizer, Symbol query, QualifiedName relationName) {
        Symbol symbol = FieldReplacer.replaceFields(query, fieldToNullLiteralIfRelationMatches(relationName));
        Symbol normalized = normalizer.normalize(symbol, null);
        return WhereClause.canMatch(normalized);
    }

    private static Function<Field, Symbol> fieldToNullLiteralIfRelationMatches(QualifiedName relationName) {
        return field -> {
            if (field.relation().getQualifiedName().equals(relationName)) {
                return Literal.NULL;
            }
            return field;
        };
    }

    private static Map<QualifiedName, QueriedRelation> groupOuterRelationQSByName(Map.Entry<QualifiedName, AnalyzedRelation> left,
                                                                                  Map.Entry<QualifiedName, AnalyzedRelation> right,
                                                                                  JoinType joinType) {
        switch (joinType) {
            case LEFT:
                return Collections.singletonMap(right.getKey(), (QueriedRelation) right.getValue());
            case RIGHT:
                return Collections.singletonMap(left.getKey(), (QueriedRelation) left.getValue());
            case FULL:
                return ImmutableMap.of(
                    left.getKey(), (QueriedRelation) left.getValue(),
                    right.getKey(), (QueriedRelation) right.getValue()
                );
        }
        throw new AssertionError("Invalid joinType for outer-join -> inner-join rewrite: " + joinType);
    }

    private static void applyOuterJoinRewrite(JoinPair joinPair,
                                              QuerySpec multiSourceQuerySpec,
                                              QueriedRelation outerSubRelation,
                                              Map<Set<QualifiedName>, Symbol> splitQueries,
                                              Symbol outerRelationQuery) {
        CollectFieldsToRemoveFromOutputs collectFieldsToRemoveFromOutputs =
            new CollectFieldsToRemoveFromOutputs(outerSubRelation, multiSourceQuerySpec.outputs(), joinPair.condition());
        QuerySpec qs = outerSubRelation.querySpec();
        Symbol query = FieldReplacer.replaceFields(outerRelationQuery, collectFieldsToRemoveFromOutputs);

        if (Aggregations.containsAggregation(query)) {
            if (qs.having().isPresent()) {
                qs.having().get().add(query);
            } else {
                qs.having(new HavingClause(query));
            }
        } else {
            qs.where(qs.where().add(query));
        }
        if (splitQueries.isEmpty()) { // All queries where successfully pushed down
            joinPair.joinType(JoinType.INNER);
            multiSourceQuerySpec.where(WhereClause.MATCH_ALL);
        } else { // Query only for one relation was pushed down
            if (joinPair.left().equals(outerSubRelation.getQualifiedName())) {
                joinPair.joinType(JoinType.LEFT);
            } else {
                joinPair.joinType(JoinType.RIGHT);
            }
            multiSourceQuerySpec.where(new WhereClause(AndOperator.join(splitQueries.values())));
        }
        for (Field fieldToRemove : collectFieldsToRemoveFromOutputs.fieldsToNotCollect()) {
            multiSourceQuerySpec.outputs().remove(fieldToRemove);
            QueriedRelation relation = (QueriedRelation) fieldToRemove.relation();

            int index = fieldToRemove.index();
            relation.querySpec().outputs().remove(index);
            relation.fields().remove(fieldToRemove);
        }
    }

    /**
     * Collect fields which are being pushed down and otherwise not required.
     */
    private static class CollectFieldsToRemoveFromOutputs implements Function<Field, Symbol> {
        private final QueriedRelation outerRelation;
        private final List<Symbol> mssOutputSymbols;
        private final Symbol joinCondition;
        private final Set<Field> fieldsToNotCollect;

        CollectFieldsToRemoveFromOutputs(QueriedRelation outerRelation,
                                         List<Symbol> mssOutputSymbols,
                                         Symbol joinCondition) {
            this.outerRelation = outerRelation;
            this.mssOutputSymbols = mssOutputSymbols;
            this.joinCondition = joinCondition;
            this.fieldsToNotCollect = new HashSet<>();
        }

        @Nullable
        @Override
        public Symbol apply(@Nullable Field input) {
            if (input == null) {
                return null;
            }
            if (!input.relation().equals(outerRelation)) {
                return input;
            }


            // if the column was only added to the outerSpec outputs because of the whereClause
            // it's possible to not collect it as long is it isn't used somewhere else
            if (!mssOutputSymbols.contains(input) &&
                !SymbolVisitors.any(symbol -> Objects.equals(input, symbol), joinCondition)) {
                fieldsToNotCollect.add(input);
            }
            return outerRelation.querySpec().outputs().get(input.index());
        }

        Collection<Field> fieldsToNotCollect() {
            return fieldsToNotCollect;
        }
    }
}

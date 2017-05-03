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

package io.crate.planner.fetch;

import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.*;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.sql.tree.QualifiedName;
import org.elasticsearch.common.collect.Tuple;

import java.util.*;
import java.util.function.Function;

class MultiSourceFetchPushDown {

    private final MultiSourceSelect statement;

    private List<Symbol> remainingOutputs;
    private Map<TableIdent, FetchSource> fetchSources;

    MultiSourceFetchPushDown(MultiSourceSelect statement) {
        this.statement = statement;
        this.fetchSources = new HashMap<>(statement.sources().size());
    }

    Map<TableIdent, FetchSource> fetchSources() {
        return fetchSources;
    }

    List<Symbol> remainingOutputs() {
        return remainingOutputs;
    }

    void process() {
        remainingOutputs = statement.querySpec().outputs();

        HashMap<Symbol, FetchReference> fetchRefByOriginalSymbol = new HashMap<>();
        ArrayList<Symbol> mssOutputs = new ArrayList<>(
            statement.sources().size() + statement.requiredForQuery().size());

        for (Map.Entry<QualifiedName, AnalyzedRelation> entry : statement.sources().entrySet()) {
            QueriedRelation relation = (QueriedRelation) entry.getValue();
            if (!(relation instanceof QueriedDocTable)) {
                mssOutputs.addAll(relation.fields());
                continue;
            }

            DocTableRelation rel = ((QueriedDocTable) relation).tableRelation();
            DocTableInfo tableInfo = rel.tableInfo();
            FetchFields canBeFetched = filterByRelation(statement.canBeFetched(), relation, rel);
            if (!canBeFetched.isEmpty()) {

                Field fetchIdColumn = rel.getField(DocSysColumns.FETCHID);
                assert fetchIdColumn != null: "_fetchId must be accessible";
                mssOutputs.add(fetchIdColumn);
                InputColumn fetchIdInput = new InputColumn(mssOutputs.size() - 1, fetchIdColumn.valueType());

                ArrayList<Symbol> qtOutputs = new ArrayList<>(
                    relation.querySpec().outputs().size() - canBeFetched.size() + 1);
                qtOutputs.add(fetchIdColumn);

                for (int i = 0; i < relation.querySpec().outputs().size(); i++) {
                    Symbol output = relation.querySpec().outputs().get(i);
                    if (!canBeFetched.contains(output)) {
                        qtOutputs.add(output);
                        mssOutputs.add(relation.fields().get(i));
                    }
                }
                for (Tuple<Field, Reference> parentAndChild : canBeFetched.parentAndChildren()) {
                    Field parent = parentAndChild.v1();
                    Reference child = parentAndChild.v2();
                    FetchReference fr = new FetchReference(
                        fetchIdInput, DocReferences.toSourceLookup(child));
                    allocateFetchedReference(fr, tableInfo.partitionedByColumns());
                    fetchRefByOriginalSymbol.put(parent, fr);
                }
                QuerySpec querySpec = relation.querySpec().copyAndReplace(Function.identity());
                querySpec.outputs(qtOutputs);
                QueriedDocTable newRelation = new QueriedDocTable(rel, querySpec);
                entry.setValue(newRelation);
                mssOutputs.set(fetchIdInput.index(), newRelation.getField(DocSysColumns.FETCHID, Operation.READ));
            } else {
                mssOutputs.addAll(relation.fields());
            }
        }

        statement.querySpec().outputs(mssOutputs);
        MappingSymbolVisitor.inPlace().processInplace(remainingOutputs, fetchRefByOriginalSymbol);
        if (statement.querySpec().orderBy().isPresent()) {
            MappingSymbolVisitor.inPlace().processInplace(statement.querySpec().orderBy().get().orderBySymbols(), fetchRefByOriginalSymbol);
        }
    }

    private static FetchFields filterByRelation(Set<Field> fields, AnalyzedRelation rel, DocTableRelation tableRelation) {
        FetchFields fetchFields = new FetchFields(tableRelation);
        for (Field field : fields) {
            if (field.relation().equals(rel)) {
                fetchFields.add(field);
            }
        }
        return fetchFields;
    }

    private void allocateFetchedReference(FetchReference fr, List<Reference> partitionedByColumns) {
        FetchSource fs = fetchSources.get(fr.ref().ident().tableIdent());
        if (fs == null) {
            fs = new FetchSource(partitionedByColumns);
            fetchSources.put(fr.ref().ident().tableIdent(), fs);
        }
        fs.fetchIdCols().add(fr.fetchId());
        if (fr.ref().granularity() == RowGranularity.DOC) {
            fs.references().add(fr.ref());
        }
    }

    private final static class FetchFields {

        private final DocTableRelation tableRelation;
        private Set<Field> canBeFetchedParent = new HashSet<>();
        private Set<Reference> canBeFetchedChild = new HashSet<>();

        FetchFields(DocTableRelation tableRelation) {
            this.tableRelation = tableRelation;
        }

        void add(Field field) {
            canBeFetchedParent.add(field);
            canBeFetchedChild.add(tableRelation.resolveField(tableRelation.getField(field.path())));
        }

        boolean isEmpty() {
            return canBeFetchedChild.isEmpty();
        }

        int size() {
            return canBeFetchedChild.size();
        }

        boolean contains(Symbol output) {
            return canBeFetchedChild.contains(output);
        }

        Iterable<Tuple<Field, Reference>> parentAndChildren() {
            return () -> new Iterator<Tuple<Field, Reference>>() {

                private final Iterator<Reference> children = canBeFetchedChild.iterator();
                private final Iterator<Field> parents = canBeFetchedParent.iterator();

                @Override
                public boolean hasNext() {
                    return children.hasNext();
                }

                @Override
                public Tuple<Field, Reference> next() {
                    if (!children.hasNext()) {
                        throw new NoSuchElementException("Iterator is exhausted");
                    }
                    Reference child = children.next();
                    Field parent = parents.next();

                    return new Tuple<>(parent, child);
                }
            };
        }
    }
}

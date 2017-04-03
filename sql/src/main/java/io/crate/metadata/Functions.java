/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.metadata;

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import io.crate.types.DataType;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Functions {

    private final Map<String, FunctionResolver> functionResolvers;
    private final Map<String, Map<String, FunctionResolver>> schemaFunctionResolvers;

    @Inject
    public Functions(Map<FunctionIdent, FunctionImplementation> functionImplementations,
                     Map<String, FunctionResolver> functionResolvers) {
        this.functionResolvers = Maps.newHashMap(functionResolvers);
        this.functionResolvers.putAll(generateFunctionResolvers(functionImplementations));
        schemaFunctionResolvers = new ConcurrentHashMap<>();
    }

    private Map<String, FunctionResolver> generateFunctionResolvers(Map<FunctionIdent, FunctionImplementation> functionImplementations) {
        Multimap<String, Tuple<FunctionIdent, FunctionImplementation>> signatures = getSignatures(functionImplementations);
        return signatures.keys().stream()
            .distinct()
            .collect(Collectors.toMap(name -> name, name -> new GeneratedFunctionResolver(signatures.get(name))));
    }

    private Multimap<String, Tuple<FunctionIdent, FunctionImplementation>> getSignatures(
        Map<FunctionIdent, FunctionImplementation> functionImplementations) {
        Multimap<String, Tuple<FunctionIdent, FunctionImplementation>> signatureMap = ArrayListMultimap.create();
        for (Map.Entry<FunctionIdent, FunctionImplementation> entry : functionImplementations.entrySet()) {
            signatureMap.put(entry.getKey().name(), new Tuple<>(entry.getKey(), entry.getValue()));
        }
        return signatureMap;
    }

    public void registerSchemaFunctionResolvers(String schema, Map<FunctionIdent, FunctionImplementation> functions) {
        schemaFunctionResolvers.put(schema, generateFunctionResolvers(functions));
    }

    public void deregisterSchemaFunctions() {
        schemaFunctionResolvers.clear();
    }

    /**
     * Returns the built-in function implementation for the given function name and arguments.
     *
     * @param name           The function name.
     * @param argumentsTypes The function argument types.
     * @return a function implementation or null if it was not found.
     */
    @Nullable
    public FunctionImplementation getBuiltin(String name, List<DataType> argumentsTypes) throws IllegalArgumentException {
        return resolveFunctionForArgumentTypes(argumentsTypes, functionResolvers.get(name));
    }

    /**
     * Returns the function implementation for the given function schema, name and arguments.
     * <p>
     * The lookup order:
     * <p>
     * x.foo -> in the `x` schema
     * foo -> the session schema -> the default schema
     *
     * @param schema         The schema name.
     * @param name           The function name.
     * @param argumentsTypes The function argument types.
     * @return a function implementation or null if it was not found.
     */
    @Nullable
    public FunctionImplementation getUserDefined(@Nullable String schema, String name, List<DataType> argumentsTypes)
        throws IllegalArgumentException {
        Map<String, FunctionResolver> functionResolvers
            = schemaFunctionResolvers.get(schema == null ? Schemas.DEFAULT_SCHEMA_NAME : schema);
        if (functionResolvers == null) {
            return null;
        }
        return resolveFunctionForArgumentTypes(argumentsTypes, functionResolvers.get(name));
    }

    /**
     * Returns the function implementation for the given function ident by
     * looking up first in the built-in and then in user defined function.
     *
     * @param ident The function ident.
     * @return a function implementation or null if it was not found.
     */
    public FunctionImplementation getQualified(FunctionIdent ident) {
        FunctionImplementation impl = getBuiltin(ident.name(), ident.argumentTypes());
        if (impl != null) {
            return impl;
        }
        return getUserDefined(ident.schema(), ident.name(), ident.argumentTypes());
    }

    public FunctionImplementation getQualifiedSafe(FunctionIdent ident) {
        FunctionImplementation impl = null;
        String exceptionMessage = null;
        try {
            impl = getQualified(ident);
        } catch (IllegalArgumentException e) {
            if (e.getMessage() != null && !e.getMessage().isEmpty()) {
                exceptionMessage = e.getMessage();
            }
        }
        if (impl == null) {
            if (exceptionMessage == null) {
                exceptionMessage = String.format(Locale.ENGLISH, "unknown function: %s(%s)", ident.name(),
                    Joiner.on(", ").join(ident.argumentTypes()));
            }
            throw new UnsupportedOperationException(exceptionMessage);
        }
        return impl;
    }

    public FunctionImplementation getBuiltinSafe(String name, List<DataType> argumentTypes) {
        FunctionImplementation impl = null;
        String exceptionMessage = null;
        try {
            impl = getBuiltin(name, argumentTypes);
        } catch (IllegalArgumentException e) {
            if (e.getMessage() != null && !e.getMessage().isEmpty()) {
                exceptionMessage = e.getMessage();
            }
        }
        throwIfNotFound(name, argumentTypes, impl, exceptionMessage);
        return impl;
    }

    private void throwIfNotFound(String name,
                                 List<DataType> argumentTypes,
                                 @Nullable FunctionImplementation impl,
                                 @Nullable String exceptionMessage) {
        if (impl == null) {
            throw new UnsupportedOperationException(
                exceptionMessage == null ?
                    String.format(Locale.ENGLISH, "unknown function: %s(%s)", name, Joiner.on(", ").join(argumentTypes)) :
                    exceptionMessage
            );
        }
    }

    private FunctionImplementation resolveFunctionForArgumentTypes(List<DataType> types, FunctionResolver resolver) {
        if (resolver != null) {
            List<DataType> signature = resolver.getSignature(types);
            if (signature != null) {
                return resolver.getForTypes(signature);
            }
        }
        return null;
    }

    private static class GeneratedFunctionResolver implements FunctionResolver {

        private final List<Signature.SignatureOperator> signatures;
        private final Map<List<DataType>, FunctionImplementation> functions;

        GeneratedFunctionResolver(Collection<Tuple<FunctionIdent, FunctionImplementation>> functionTuples) {
            signatures = new ArrayList<>(functionTuples.size());
            functions = new HashMap<>(functionTuples.size());
            for (Tuple<FunctionIdent, FunctionImplementation> functionTuple : functionTuples) {
                List<DataType> argumentTypes = functionTuple.v1().argumentTypes();
                signatures.add(Signature.of(argumentTypes));
                functions.put(argumentTypes, functionTuple.v2());
            }
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            return functions.get(dataTypes);
        }

        @Nullable
        @Override
        public List<DataType> getSignature(List<DataType> dataTypes) {
            for (Signature.SignatureOperator signature : signatures) {
                List<DataType> sig = signature.apply(dataTypes);
                if (sig != null) {
                    return sig;
                }
            }
            return null;
        }
    }
}

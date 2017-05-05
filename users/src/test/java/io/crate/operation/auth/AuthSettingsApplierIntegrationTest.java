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

package io.crate.operation.auth;

import com.google.common.collect.ImmutableMap;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.testing.UseJdbc;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.junit.After;
import org.junit.Test;

import java.util.Map;


@UseJdbc(value = 1)
@ClusterScope(scope = Scope.TEST, numClientNodes = 0, numDataNodes = 0)
public class AuthSettingsApplierIntegrationTest extends SQLTransportIntegrationTest {

    @After
    private void cleanUpTransientSettings() {
        Settings.Builder builder = Settings.builder();
        for (String setting : authSettings().getAsMap().keySet()) {
            builder.put(setting, (String) null);
        }
        internalCluster().client().admin().cluster()
            .prepareUpdateSettings()
            .setTransientSettings(builder)
            .get();
    }

    private Settings authSettings() {
        return Settings.builder()
            .put("auth.host_based.config",
                "a", new String[]{"name"}, new String[]{"foo"})
            .put("auth.host_based.config",
                "b", new String[]{"name"}, new String[]{"bar"})
            .build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("auth.host_based.enabled", true)
            .build();
    }

    private static void assertSettingsApplied(ImmutableMap<String, Map<String, String>> hbaConf) throws Exception {
        for (String node : internalCluster().getNodeNames()) {
            AuthenticationProvider provider = internalCluster().getInstance(AuthenticationProvider.class, node);
            assertEquals(hbaConf, ((AuthenticationService) provider.authService()).hbaConf());
        }
    }

    private static ImmutableMap<String, Map<String, String>> convertSettingsToMap(Settings settings) {
        ImmutableMap.Builder<String, Map<String, String>> mapBuilder = ImmutableMap.builder();
        for (Map.Entry<String, Settings> entry : settings.getByPrefix("auth.host_based.config.").getAsGroups().entrySet()) {
            mapBuilder.put(entry.getKey(), entry.getValue().getAsMap());
        }
        return mapBuilder.build();
    }

    @Test
    public void testClusterSettingsAppliedFromLastNode() throws Exception {
        // start 1st node
        internalCluster().startNode(Settings.EMPTY);
        ensureStableCluster(1);

        // start 2nd node
        final Settings s2 = Settings.builder()
            .put("auth.host_based.config",
                "a", new String[]{"user"}, new String[]{"foo"})
            .put("auth.host_based.config",
                "b", new String[]{"user"}, new String[]{"bar"})
            .build();
        internalCluster().startNode(s2);
        ensureStableCluster(2);
        assertSettingsApplied(convertSettingsToMap(s2));

        // start 3rd node
        final Settings s3 = Settings.builder()
            .put("auth.host_based.config",
                "a", new String[]{"user"}, new String[]{"baz"})
            .put("auth.host_based.config",
                "c", new String[]{"user"}, new String[]{"foobar"})
            .build();

        internalCluster().startNode(s3);
        ensureStableCluster(3);
        assertSettingsApplied(convertSettingsToMap(s3));

        // start 4th node -- settings must not be updated
        internalCluster().startNode(Settings.EMPTY);
        ensureStableCluster(4);
        assertSettingsApplied(convertSettingsToMap(s3));
    }
}

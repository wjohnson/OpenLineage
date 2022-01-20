/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openlineage.proxy;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.openlineage.proxy.api.models.ConsoleConfig;
import io.openlineage.proxy.api.models.ConsoleLineageStream;
import io.openlineage.proxy.api.models.KafkaConfig;
import io.openlineage.proxy.api.models.KafkaLineageStream;
import io.openlineage.proxy.api.models.LineageStream;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * A factory for creating {@link LineageStream} instances. A {@code LineageStream} must define a
 * {@link ProxyStreamConfig} defining the set of parameters needed to construct a new {@code
 * LineageStream} instance. For example, {@link KafkaConfig} defines the parameters for constructing
 * a new {@link KafkaLineageStream} instance when invoking {@link ProxyStreamFactory#build()}.
 * Below, we define a list of supported {@code LineageStream}s. Note, when defining your own {@code
 * ProxyStreamConfig}, the {@code type} parameter <b>must</b> be specified.
 *
 * <ul>
 *   <li>A default {@link ConsoleLineageStream} stream
 *   <li>A {@link KafkaLineageStream} stream
 * </ul>
 */
@Slf4j
public final class ProxyStreamFactory {
  private static final String DEFAULT_PROXY_LINEAGE_SOURCE = "openLineageProxyBackend";
  private static final List<ProxyStreamConfig> DEFAULT_STREAMS =
      Lists.newArrayList(new ConsoleConfig());

  @Getter @Setter private String source = DEFAULT_PROXY_LINEAGE_SOURCE;
  @Getter @Setter private List<ProxyStreamConfig> streams = DEFAULT_STREAMS;

  public ImmutableSet<LineageStream> build() {
    final ImmutableSet.Builder lineageStreams = ImmutableSet.builder();
    for (final ProxyStreamConfig config : streams) {
      if (config instanceof ConsoleConfig) {
        lineageStreams.add(new ConsoleLineageStream());
      } else if (config instanceof KafkaConfig) {
        final KafkaConfig kafkaConfig = (KafkaConfig) config;
        if (!kafkaConfig.hasLocalServerId()) {
          // Set the local server ID to the lineage source when not specified
          kafkaConfig.setLocalServerId(source);
        }
        kafkaConfig.getProperties().put("bootstrap.servers", kafkaConfig.getBootstrapServerUrl());
        kafkaConfig.getProperties().put("server.id", kafkaConfig.getLocalServerId());
        lineageStreams.add(new KafkaLineageStream((KafkaConfig) config));
      }
    }
    return lineageStreams.build();
  }
}

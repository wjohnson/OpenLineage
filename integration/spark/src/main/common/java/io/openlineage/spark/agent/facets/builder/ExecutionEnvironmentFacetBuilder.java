package io.openlineage.spark.agent.facets.builder;

import io.openlineage.spark.agent.facets.EnvironmentFacet;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.HashMap;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;

@Slf4j
public class ExecutionEnvironmentFacetBuilder extends CustomFacetBuilder<Object, EnvironmentFacet> {
  private final OpenLineageContext openLineageContext;

  public ExecutionEnvironmentFacetBuilder(OpenLineageContext openLineageContext) {
    this.openLineageContext = openLineageContext;
  }

  @Override
  public boolean isDefinedAt(Object x) {
    return (x instanceof SparkListenerSQLExecutionEnd
        || x instanceof SparkListenerSQLExecutionStart);
  }

  @Override
  protected void build(Object x, BiConsumer<String, ? super EnvironmentFacet> consumer) {
    HashMap<String, Object> executionEnvironment = new HashMap<>();
    if (x instanceof SparkListenerSQLExecutionStart) {
      SparkListenerSQLExecutionStart event = (SparkListenerSQLExecutionStart) x;
      executionEnvironment.put("execution-id", String.valueOf(event.executionId()));
    } else {
      SparkListenerSQLExecutionEnd event = (SparkListenerSQLExecutionEnd) x;
      executionEnvironment.put("execution-id", String.valueOf(event.executionId()));
    }

    consumer.accept("execution-environment", new EnvironmentFacet(executionEnvironment));
  }
}

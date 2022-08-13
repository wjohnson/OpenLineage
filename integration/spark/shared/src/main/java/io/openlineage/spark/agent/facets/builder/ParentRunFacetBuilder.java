package io.openlineage.spark.agent.facets.builder;

import io.openlineage.client.OpenLineage.ParentRunFacet;
import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.spark.agent.util.SparkConfUtils;
import io.openlineage.spark.api.AbstractRunFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class ParentRunFacetBuilder extends AbstractRunFacetBuilder<SparkListenerEvent> {
  private static final Map<Long, UUID> executionIdToRunIdMap = new ConcurrentHashMap<>();

  public ParentRunFacetBuilder(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  protected void build(SparkListenerEvent event, BiConsumer<String, ? super RunFacet> consumer) {
    log.info("Testing parentrunfacetbuilder");
    Optional<SparkSession> ss = context.getSparkSession();
    log.info("ss is present: {}", ss.isPresent());
    log.info("ss: {}", ss);
    log.info("I'm here!");
    Optional<ParentRunFacet> pFacet =
        buildParentFacet(
            ss.get()); // ss.get() because can't pass in an optional to buildParentFacet
    log.info("I'm not here!");
    log.info("parent run facet: {}", pFacet);
    log.info("parent run facet is present: {}", pFacet.isPresent());

    // context
    //     .getSparkSession()
    //     .ifPresent(
    //         sess -> {
    //           Optional<ParentRunFacet> parentFacet = buildParentFacet(sess);
    //           parentFacet.ifPresent(facet -> consumer.accept("parent", facet));
    //         });
  }

  private Optional<ParentRunFacet> buildParentFacet(SparkSession sess) {
    // This property appears to be unique to Azure Databricks' implementation
    // of a connector to Azure Synapse SQL Pools
    // String sqlExecutionParent =
    //     SparkConfUtils.findSparkConfigKey(sess.sparkContext().conf(), "sql.execution.parent",
    // null);

    // // Better to do an IF and then call method or call method with IF inside it?
    // String parentRunId = executionParentToParentRun(sqlExecutionParent);
    String parentRunId = null;
    if (parentRunId == null) {
      log.info("here 1");
      log.info("check sess.SparkContext.conf: {}", sess.sparkContext().conf());
      // the spark conf is null, which causes a null pointer exception in findSparkConfigKey for the test testSqlEventWithJobEventEmitsOnce
      parentRunId =
          SparkConfUtils.findSparkConfigKey(
              sess.sparkContext().conf(), "openlineage.parentRunId", null);
    }
    log.info("here 2");
    
    String parentJobName =
        SparkConfUtils.findSparkConfigKey(
            sess.sparkContext().conf(), "openlineage.parentJobName", null);
    log.info("here 3");
    String namespace =
        SparkConfUtils.findSparkConfigKey(
            sess.sparkContext().conf(), "openlineage.namespace", null);
    log.info("here 4");
    if (parentRunId != null && parentJobName != null && namespace != null) {
      log.info("here 5");
      return Optional.of(
          context
              .getOpenLineage()
              .newParentRunFacetBuilder()
              .run(
                  context
                      .getOpenLineage()
                      .newParentRunFacetRunBuilder()
                      .runId(UUID.fromString(parentRunId))
                      .build())
              .job(
                  context
                      .getOpenLineage()
                      .newParentRunFacetJobBuilder()
                      .namespace(namespace)
                      .name(parentJobName)
                      .build())
              .build());
    }
    log.info("here 6");
    return Optional.empty();
  }

  private String executionParentToParentRun(String sqlExecutionParent) {
    String output = null;
    // Test to see if the sqlExecutionParent was present, if so extract the stored runid
    if (sqlExecutionParent != null) {
      if (executionIdToRunIdMap.containsKey(Long.parseLong(sqlExecutionParent))) {
        output = executionIdToRunIdMap.get(Long.parseLong(sqlExecutionParent)).toString();
      } else {
        output = null;
      }
    }
    return output;
  }
}

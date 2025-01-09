/*
 *
 * title           :collector/zookeeper_module.go
 * description     :Submodule Collector for the Cluster ZooKeeper metrics
 * author          :Enes Erdoğan
 * date            :2025/01/09
 * version         :1.0
 *
 */
package collector

/* ======================================================================
 * Dependencies and libraries
 * ====================================================================== */
import (
    // Go Default libraries
    "context"
    "strings"

    // Own libraries
    jp "keedio/cloudera_exporter/json_parser"
    log "keedio/cloudera_exporter/logger"

    // Go Prometheus libraries
    "github.com/prometheus/client_golang/prometheus"
)

/* ======================================================================
 * Data Structs
 * ====================================================================== */
// None (following the style of hdfs_module.go)

/* ======================================================================
 * Constants with the ZooKeeper module TSquery sentences
 * ====================================================================== */
const ZK_SCRAPER_NAME = "zookeeper"

// --- Base Metric Queries ---
// Each query is now a single line with proper escaping of quotes.
const (
    // The number of alerts (events per second)
    ZK_ALERTS_RATE = 
    "SELECT LAST(alerts_rate) WHERE category=\"SERVICE\" AND serviceName=\"ZOOKEEPER\""

    // Duration of the last/current canary job (ms)
    ZK_CANARY_DURATION = 
    "SELECT LAST(canary_duration) WHERE category=\"SERVICE\" AND serviceName=\"ZOOKEEPER\""

    // The current epoch (epoch per second)
    ZK_CURRENT_EPOCH_RATE = 
    "SELECT LAST(current_epoch_rate) WHERE category=\"SERVICE\" AND serviceName=\"ZOOKEEPER\""

    // The current ZooKeeper XID
    ZK_CURRENT_XID = 
    "SELECT LAST(current_xid) WHERE category=\"SERVICE\" AND serviceName=\"ZOOKEEPER\""

    // The number of critical events (events per second)
    ZK_EVENTS_CRITICAL_RATE =
    "SELECT LAST(events_critical_rate) WHERE category=\"SERVICE\" AND serviceName=\"ZOOKEEPER\""

    // The number of important events (events per second)
    ZK_EVENTS_IMPORTANT_RATE = 
    "SELECT LAST(events_important_rate) WHERE category=\"SERVICE\" AND serviceName=\"ZOOKEEPER\""

    // The number of informational events (events per second)
    ZK_EVENTS_INFORMATIONAL_RATE =
    "SELECT LAST(events_informational_rate) WHERE category=\"SERVICE\" AND serviceName=\"ZOOKEEPER\""

    // Percentage of Time with Bad Health
    ZK_HEALTH_BAD_RATE =
    "SELECT LAST(health_bad_rate) WHERE category=\"SERVICE\" AND serviceName=\"ZOOKEEPER\""

    // Percentage of Time with Concerning Health
    ZK_HEALTH_CONCERNING_RATE =
    "SELECT LAST(health_concerning_rate) WHERE category=\"SERVICE\" AND serviceName=\"ZOOKEEPER\""

    // Percentage of Time with Disabled Health
    ZK_HEALTH_DISABLED_RATE =
    "SELECT LAST(health_disabled_rate) WHERE category=\"SERVICE\" AND serviceName=\"ZOOKEEPER\""

    // Percentage of Time with Good Health
    ZK_HEALTH_GOOD_RATE =
    "SELECT LAST(health_good_rate) WHERE category=\"SERVICE\" AND serviceName=\"ZOOKEEPER\""

    // Percentage of Time with Unknown Health
    ZK_HEALTH_UNKNOWN_RATE =
    "SELECT LAST(health_unknown_rate) WHERE category=\"SERVICE\" AND serviceName=\"ZOOKEEPER\""
)

// --- Aggregate Metric Queries (examples) ---
// If you want aggregates across all clusters or totals, you can add them here:
const (
    // e.g. alerts_rate aggregated across clusters
    ZK_ALERTS_RATE_ACROSS_CLUSTERS = 
    "SELECT LAST(alerts_rate_across_clusters)"

    // e.g. total alerts_rate aggregated across clusters
    ZK_TOTAL_ALERTS_RATE_ACROSS_CLUSTERS =
    "SELECT LAST(total_alerts_rate_across_clusters)"
)

/* ======================================================================
 * Global variables (Prometheus descriptors)
 * ====================================================================== */
var (
    // Base metrics
    zkAlertsRate = createZKMetricStruct("alerts_rate",
        "Number of ZooKeeper alerts (events per second)",
    )
    zkCanaryDuration = createZKMetricStruct("canary_duration_ms",
        "Duration of the last or currently running canary job (ms)",
    )
    zkCurrentEpochRate = createZKMetricStruct("current_epoch_rate",
        "The current epoch (epoch per second)",
    )
    zkCurrentXID = createZKMetricStruct("current_xid",
        "The current ZooKeeper XID",
    )
    zkEventsCriticalRate = createZKMetricStruct("events_critical_rate",
        "The number of critical events (events per second)",
    )
    zkEventsImportantRate = createZKMetricStruct("events_important_rate",
        "The number of important events (events per second)",
    )
    zkEventsInformationalRate = createZKMetricStruct("events_informational_rate",
        "The number of informational events (events per second)",
    )
    zkHealthBadRate = createZKMetricStruct("health_bad_rate",
        "Percentage of Time with Bad Health (s/s)",
    )
    zkHealthConcerningRate = createZKMetricStruct("health_concerning_rate",
        "Percentage of Time with Concerning Health (s/s)",
    )
    zkHealthDisabledRate = createZKMetricStruct("health_disabled_rate",
        "Percentage of Time with Disabled Health (s/s)",
    )
    zkHealthGoodRate = createZKMetricStruct("health_good_rate",
        "Percentage of Time with Good Health (s/s)",
    )
    zkHealthUnknownRate = createZKMetricStruct("health_unknown_rate",
        "Percentage of Time with Unknown Health (s/s)",
    )

    // Aggregate metrics (examples)
    zkAlertsRateAcrossClusters = createZKMetricStruct("alerts_rate_across_servers",
        "Alerts rate aggregated across all clusters",
    )
    zkTotalAlertsRateAcrossClusters = createZKMetricStruct("total_alerts_rate_across_servers",
        "Total alerts rate aggregated across all clusters",
    )
)

// This array ties each query to its corresponding Prometheus descriptor.
// Add or remove items here based on your needs.
var zkQueryVariableRelationship = []relation{
    // Base metrics
    {ZK_ALERTS_RATE,                *zkAlertsRate},
    {ZK_CANARY_DURATION,            *zkCanaryDuration},
    {ZK_CURRENT_EPOCH_RATE,         *zkCurrentEpochRate},
    {ZK_CURRENT_XID,                *zkCurrentXID},
    {ZK_EVENTS_CRITICAL_RATE,       *zkEventsCriticalRate},
    {ZK_EVENTS_IMPORTANT_RATE,      *zkEventsImportantRate},
    {ZK_EVENTS_INFORMATIONAL_RATE,  *zkEventsInformationalRate},
    {ZK_HEALTH_BAD_RATE,            *zkHealthBadRate},
    {ZK_HEALTH_CONCERNING_RATE,     *zkHealthConcerningRate},
    {ZK_HEALTH_DISABLED_RATE,       *zkHealthDisabledRate},
    {ZK_HEALTH_GOOD_RATE,           *zkHealthGoodRate},
    {ZK_HEALTH_UNKNOWN_RATE,        *zkHealthUnknownRate},

    // Example aggregator queries
    {ZK_ALERTS_RATE_ACROSS_CLUSTERS,        *zkAlertsRateAcrossClusters},
    {ZK_TOTAL_ALERTS_RATE_ACROSS_CLUSTERS,  *zkTotalAlertsRateAcrossClusters},
}

/* ======================================================================
 * Functions
 * ====================================================================== */

// createZKMetricStruct is analogous to create_hdfs_metric_struct in hdfs_module.go
func createZKMetricStruct(metricName string, description string) *prometheus.Desc {
    // If description is empty, auto-generate something readable
    if len(description) == 0 {
        description = strings.ReplaceAll(strings.ToUpper(metricName), "_", " ")
    }

    // Return a Prometheus descriptor
    return prometheus.NewDesc(
        prometheus.BuildFQName(namespace, ZK_SCRAPER_NAME, metricName),
        description,
        []string{"cluster", "entityName"}, // Same label pattern as HDFS
        nil,
    )
}

// createZKMetric is analogous to create_hdfs_metric in hdfs_module.go
func createZKMetric(
    ctx context.Context,
    config Collector_connection_data,
    query string,
    metricStruct prometheus.Desc,
    ch chan<- prometheus.Metric,
) bool {

    // 1. Perform the timeseries query
    jsonParsed, err := make_and_parse_timeseries_query(ctx, config, query)
    if err != nil {
        return false
    }

    // 2. Number of timeSeries in the response
    numTsSeries, err := jp.Get_timeseries_num(jsonParsed)
    if err != nil {
        return false
    }

    // 3. Extract metadata for each TimeSeries
    for tsIndex := 0; tsIndex < numTsSeries; tsIndex++ {
        clusterName := jp.Get_timeseries_query_cluster(jsonParsed, tsIndex)
        entityName := jp.Get_timeseries_query_entity_name(jsonParsed, tsIndex)

        // 4. Grab the last data point’s value
        value, err := jp.Get_timeseries_query_value(jsonParsed, tsIndex)
        if err != nil {
            // Skip if no valid data
            continue
        }

        // 5. Emit to Prometheus
        ch <- prometheus.MustNewConstMetric(
            &metricStruct,
            prometheus.GaugeValue,
            value,
            clusterName,
            entityName,
        )
    }

    return true
}

/* ======================================================================
 * Scrape "Class"
 * ====================================================================== */
type ScrapeZookeeperMetrics struct{}

// Name returns the Scraper name (must be unique).
func (ScrapeZookeeperMetrics) Name() string {
    return ZK_SCRAPER_NAME
}

// Help describes the role of this Scraper.
func (ScrapeZookeeperMetrics) Help() string {
    return "Collects ZooKeeper metrics from Cloudera Manager"
}

// Version is an arbitrary float for the scraper version.
func (ScrapeZookeeperMetrics) Version() float64 {
    return 1.0
}

// Scrape runs the queries defined in zkQueryVariableRelationship
// and emits metrics to the Prometheus channel.
func (ScrapeZookeeperMetrics) Scrape(
    ctx context.Context,
    config *Collector_connection_data,
    ch chan<- prometheus.Metric,
) error {
    log.Debug_msg("Executing ZooKeeper Metrics Scraper")

    successQueries := 0
    errorQueries := 0

    // Loop over each (QUERY, PROM_DESC) relation
    for i := range zkQueryVariableRelationship {
        rel := zkQueryVariableRelationship[i]
        if createZKMetric(ctx, *config, rel.Query, rel.Metric_struct, ch) {
            successQueries++
        } else {
            errorQueries++
        }
    }

    log.Debug_msg(
        "ZK Scraper: %d queries run, %d successful, %d errors",
        successQueries+errorQueries,
        successQueries,
        errorQueries,
    )
    return nil
}

// Ensure ScrapeZookeeperMetrics implements the Scraper interface
var _ Scraper = ScrapeZookeeperMetrics{}

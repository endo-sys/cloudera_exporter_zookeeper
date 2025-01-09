/*
 *
 * title           :collector/zookeeper_module.go
 * description     :Submodule Collector for the Cluster Zookeeper metrics
 * author          :Your Name
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
// None (same pattern as HDFS)

/* ======================================================================
 * Constants with the Zookeeper module TSquery sentences
 * ====================================================================== */
const ZK_SCRAPER_NAME = "zookeeper"

// Example queries (placeholder). Adjust them for your actual Cloudera Manager queries:
const (
    // Possible ZooKeeper queries. Replace with real queries from CM:
    ZK_ALERTS_RATE                = "SELECT LAST(alerts_rate) WHERE category=SERVICE and serviceType=ZOOKEEPER"
    ZK_CANARY_DURATION            = "SELECT LAST(canary_duration) WHERE category=SERVICE and serviceType=ZOOKEEPER"
    ZK_CURRENT_EPOCH_RATE         = "SELECT LAST(current_epoch_rate) WHERE category=SERVICE and serviceType=ZOOKEEPER"
    ZK_CURRENT_XID                = "SELECT LAST(current_xid) WHERE category=SERVICE and serviceType=ZOOKEEPER"
    // etc... add more as needed
)

/* ======================================================================
 * Global variables
 * ====================================================================== */
// Prometheus data Descriptors for the metrics to export
var (
    zk_alerts_rate = create_zk_metric_struct(
        "alerts_rate",
        "Number of ZooKeeper alerts (events per second)",
    )
    zk_canary_duration = create_zk_metric_struct(
        "canary_duration_ms",
        "Duration of the last or currently running canary job in milliseconds",
    )
    zk_current_epoch_rate = create_zk_metric_struct(
        "current_epoch_rate",
        "The current ZooKeeper epoch (epochs per second)",
    )
    zk_current_xid = create_zk_metric_struct(
        "current_xid",
        "The current ZooKeeper XID",
    )
    // Add more descriptors as needed ...
)

// This array ties each query to its corresponding Prometheus descriptor
var zk_query_variable_relationship = []relation{
    {ZK_ALERTS_RATE,         *zk_alerts_rate},
    {ZK_CANARY_DURATION,     *zk_canary_duration},
    {ZK_CURRENT_EPOCH_RATE,  *zk_current_epoch_rate},
    {ZK_CURRENT_XID,         *zk_current_xid},
    // Add more {query, descriptor} pairs as needed...
}

/* ======================================================================
 * Functions
 * ====================================================================== */

// create_zk_metric_struct is analogous to create_hdfs_metric_struct,
// but uses the "zookeeper" scraper name.
func create_zk_metric_struct(metric_name string, description string) *prometheus.Desc {
    // If description is empty, auto-generate
    if len(description) == 0 {
        description = strings.Replace(strings.ToUpper(metric_name), "_", " ", -1)
    }

    // Return a Prometheus descriptor
    return prometheus.NewDesc(
        prometheus.BuildFQName(namespace, ZK_SCRAPER_NAME, metric_name),
        description,
        []string{"cluster", "entityName"}, // same label pattern as HDFS
        nil,
    )
}

// create_zk_metric is analogous to create_hdfs_metric, specialized for ZK:
func create_zk_metric(
    ctx context.Context,
    config Collector_connection_data,
    query string,
    metric_struct prometheus.Desc,
    ch chan<- prometheus.Metric,
) bool {

    // Perform the timeseries query
    json_parsed, err := make_and_parse_timeseries_query(ctx, config, query)
    if err != nil {
        return false
    }

    // Get the number of timeSeries in the response
    num_ts_series, err := jp.Get_timeseries_num(json_parsed)
    if err != nil {
        return false
    }

    // Extract metadata for each TimeSeries
    for ts_index := 0; ts_index < num_ts_series; ts_index++ {
        cluster_name := jp.Get_timeseries_query_cluster(json_parsed, ts_index)
        entity_name := jp.Get_timeseries_query_entity_name(json_parsed, ts_index)

        // Get the last data point’s value
        value, err := jp.Get_timeseries_query_value(json_parsed, ts_index)
        if err != nil {
            continue
        }

        // Push to Prometheus
        ch <- prometheus.MustNewConstMetric(
            &metric_struct,
            prometheus.GaugeValue,
            value,
            cluster_name, // label
            entity_name,  // label
        )
    }
    return true
}

/* ======================================================================
 * Scrape "Class" 
 * ====================================================================== */
type ScrapeZookeeperMetrics struct{}

// Name of the Scraper. Must be unique.
func (ScrapeZookeeperMetrics) Name() string {
    return ZK_SCRAPER_NAME
}

// Help describes the role of the Scraper.
func (ScrapeZookeeperMetrics) Help() string {
    return "ZooKeeper Metrics"
}

// Version of the scraper (arbitrary float).
func (ScrapeZookeeperMetrics) Version() float64 {
    return 1.0
}

// Scrape function that runs all queries and collects metrics.
func (ScrapeZookeeperMetrics) Scrape(
    ctx context.Context,
    config *Collector_connection_data,
    ch chan<- prometheus.Metric,
) error {
    log.Debug_msg("Executing ZooKeeper Metrics Scraper")

    // Counters
    success_queries := 0
    error_queries := 0

    // Loop over each (QUERY, PROM_DESC) relation and collect
    for i := 0; i < len(zk_query_variable_relationship); i++ {
        relationElem := zk_query_variable_relationship[i]
        if create_zk_metric(ctx, *config, relationElem.Query, relationElem.Metric_struct, ch) {
            success_queries++
        } else {
            error_queries++
        }
    }

    log.Debug_msg(
        "ZK Module executed %d queries. %d success and %d errors",
        success_queries+error_queries,
        success_queries,
        error_queries,
    )

    return nil
}

// Confirm that ScrapeZookeeperMetrics implements the Scraper interface
var _ Scraper = ScrapeZookeeperMetrics{}

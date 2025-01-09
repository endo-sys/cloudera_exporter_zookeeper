package main

import (
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "time"

    "github.com/prometheus/client_golang/prometheus"
)

// ZookeeperCollector implements the prometheus.Collector interface
type ZookeeperCollector struct {
    // Basic metrics for ZooKeeper
    alertsRate                *prometheus.Desc
    canaryDuration            *prometheus.Desc
    currentEpochRate          *prometheus.Desc
    currentXid                *prometheus.Desc
    eventsCriticalRate        *prometheus.Desc
    eventsImportantRate       *prometheus.Desc
    eventsInformationalRate   *prometheus.Desc
    healthBadRate             *prometheus.Desc
    healthConcerningRate      *prometheus.Desc
    healthDisabledRate        *prometheus.Desc
    healthGoodRate            *prometheus.Desc
    healthUnknownRate         *prometheus.Desc

    // Example “across_clusters” metric
    alertsRateAcrossClusters  *prometheus.Desc
    // Example “total_” metric
    totalAlertsRateAcrossClusters *prometheus.Desc

    // Cloudera Manager connection details
    cmHost     string
    cmPort     string
    apiVersion string
    username   string
    password   string
}

// NewZookeeperCollector returns a new ZookeeperCollector
func NewZookeeperCollector(cmHost, cmPort, apiVersion, username, password string) *ZookeeperCollector {
    // Common label dimensions you might want: clusterName, serviceName, roleName, etc.
    // For simplicity, we'll just define a "cluster" label in some metrics below.
    return &ZookeeperCollector{
        alertsRate: prometheus.NewDesc(
            "zookeeper_alerts_rate",
            "Number of ZooKeeper alerts (events per second)",
            []string{"cluster"}, // labels
            nil,                 // no constant labels
        ),
        canaryDuration: prometheus.NewDesc(
            "zookeeper_canary_duration_ms",
            "Duration of the last or currently running ZooKeeper canary job (milliseconds)",
            []string{"cluster"},
            nil,
        ),
        currentEpochRate: prometheus.NewDesc(
            "zookeeper_current_epoch_rate",
            "The current epoch (epoch per second)",
            []string{"cluster"},
            nil,
        ),
        currentXid: prometheus.NewDesc(
            "zookeeper_current_xid",
            "The current ZooKeeper XID",
            []string{"cluster"},
            nil,
        ),
        eventsCriticalRate: prometheus.NewDesc(
            "zookeeper_events_critical_rate",
            "Number of critical events (events per second)",
            []string{"cluster"},
            nil,
        ),
        eventsImportantRate: prometheus.NewDesc(
            "zookeeper_events_important_rate",
            "Number of important events (events per second)",
            []string{"cluster"},
            nil,
        ),
        eventsInformationalRate: prometheus.NewDesc(
            "zookeeper_events_informational_rate",
            "Number of informational events (events per second)",
            []string{"cluster"},
            nil,
        ),
        healthBadRate: prometheus.NewDesc(
            "zookeeper_health_bad_rate",
            "Percentage of time with Bad Health (seconds per second)",
            []string{"cluster"},
            nil,
        ),
        healthConcerningRate: prometheus.NewDesc(
            "zookeeper_health_concerning_rate",
            "Percentage of time with Concerning Health (seconds per second)",
            []string{"cluster"},
            nil,
        ),
        healthDisabledRate: prometheus.NewDesc(
            "zookeeper_health_disabled_rate",
            "Percentage of time with Disabled Health (seconds per second)",
            []string{"cluster"},
            nil,
        ),
        healthGoodRate: prometheus.NewDesc(
            "zookeeper_health_good_rate",
            "Percentage of time with Good Health (seconds per second)",
            []string{"cluster"},
            nil,
        ),
        healthUnknownRate: prometheus.NewDesc(
            "zookeeper_health_unknown_rate",
            "Percentage of time with Unknown Health (seconds per second)",
            []string{"cluster"},
            nil,
        ),

        // Example aggregator metrics
        alertsRateAcrossClusters: prometheus.NewDesc(
            "zookeeper_alerts_rate_across_clusters",
            "Alerts rate aggregated across all clusters",
            nil, // no label (aggregated)
            nil,
        ),
        totalAlertsRateAcrossClusters: prometheus.NewDesc(
            "zookeeper_total_alerts_rate_across_clusters",
            "Total alerts rate aggregated across all clusters",
            nil,
            nil,
        ),

        // Store Cloudera Manager connection info
        cmHost:     cmHost,
        cmPort:     cmPort,
        apiVersion: apiVersion,
        username:   username,
        password:   password,
    }
}

// Describe sends the descriptors of each Zookeeper metric we define to Prometheus.
func (zc *ZookeeperCollector) Describe(ch chan<- *prometheus.Desc) {
    ch <- zc.alertsRate
    ch <- zc.canaryDuration
    ch <- zc.currentEpochRate
    ch <- zc.currentXid
    ch <- zc.eventsCriticalRate
    ch <- zc.eventsImportantRate
    ch <- zc.eventsInformationalRate
    ch <- zc.healthBadRate
    ch <- zc.healthConcerningRate
    ch <- zc.healthDisabledRate
    ch <- zc.healthGoodRate
    ch <- zc.healthUnknownRate

    ch <- zc.alertsRateAcrossClusters
    ch <- zc.totalAlertsRateAcrossClusters
}

// Collect is called by the Prometheus registry when collecting metrics.
func (zc *ZookeeperCollector) Collect(ch chan<- prometheus.Metric) {
    // 1) Query the Cloudera Manager timeseries API for each metric
    // 2) Parse the JSON responses
    // 3) Create the appropriate Prometheus metrics

    // For demonstration, we show the fetch for a few metrics, but you can replicate
    // the logic for each metric name (alerts_rate, canary_duration, etc.)
    clusterName := "DemoCluster" // This might come from your config or environment

    // --------------------------------------------------------------------------------
    // Example 1: Fetch "alerts_rate" at the cluster scope
    // --------------------------------------------------------------------------------
    alertsRateValue, err := zc.fetchMetric("alerts_rate", clusterName)
    if err != nil {
        log.Printf("Error fetching alerts_rate: %v\n", err)
    } else {
        ch <- prometheus.MustNewConstMetric(
            zc.alertsRate,
            prometheus.GaugeValue,
            alertsRateValue,
            clusterName,
        )
    }

    // --------------------------------------------------------------------------------
    // Example 2: "alerts_rate_across_clusters" (aggregate)
    // --------------------------------------------------------------------------------
    // If you want an aggregator metric, the name might be "alerts_rate_across_clusters"
    // per Cloudera Manager docs. You can just fetch that directly.
    aggregatorValue, err := zc.fetchMetric("alerts_rate_across_clusters", "")
    if err != nil {
        log.Printf("Error fetching alerts_rate_across_clusters: %v\n", err)
    } else {
        ch <- prometheus.MustNewConstMetric(
            zc.alertsRateAcrossClusters,
            prometheus.GaugeValue,
            aggregatorValue,
        )
    }

    // --------------------------------------------------------------------------------
    // Example 3: "total_alerts_rate_across_clusters"
    // --------------------------------------------------------------------------------
    totalAggregatorValue, err := zc.fetchMetric("total_alerts_rate_across_clusters", "")
    if err != nil {
        log.Printf("Error fetching total_alerts_rate_across_clusters: %v\n", err)
    } else {
        ch <- prometheus.MustNewConstMetric(
            zc.totalAlertsRateAcrossClusters,
            prometheus.GaugeValue,
            totalAggregatorValue,
        )
    }

    // --------------------------------------------------------------------------------
    // Example 4: Additional metrics
    // --------------------------------------------------------------------------------
    // For canary_duration, current_xid, etc., replicate the same approach
    canaryValue, err := zc.fetchMetric("canary_duration", clusterName)
    if err == nil {
        ch <- prometheus.MustNewConstMetric(
            zc.canaryDuration,
            prometheus.GaugeValue,
            canaryValue,
            clusterName,
        )
    }
    // ... similarly for current_epoch_rate, current_xid, events_critical_rate, etc. ...
}

// fetchMetric is a helper that queries the CM timeseries API for a single metricName
func (zc *ZookeeperCollector) fetchMetric(metricName, clusterName string) (float64, error) {
    // Build the Cloudera Manager timeseries endpoint
    // For reference:
    //   /api/vXX/timeseries?query=<metric-name>[clusterName=xxx]
    // In practice, you may need to URL-encode the query or handle multiple filters
    var url string
    if clusterName != "" {
        // Example query filtering by cluster:
        // e.g., "SELECT alerts_rate WHERE category=CLUSTER AND clusterName=DemoCluster"
        url = fmt.Sprintf(
            "http://%s:%s/api/%s/timeseries?query=%s%%5BclusterName=%s%%5D",
            zc.cmHost, zc.cmPort, zc.apiVersion, metricName, clusterName,
        )
    } else {
        // aggregator metric does not require a cluster filter
        url = fmt.Sprintf(
            "http://%s:%s/api/%s/timeseries?query=%s",
            zc.cmHost, zc.cmPort, zc.apiVersion, metricName,
        )
    }

    req, err := http.NewRequest("GET", url, nil)
    if err != nil {
        return 0, err
    }
    req.SetBasicAuth(zc.username, zc.password)

    client := &http.Client{Timeout: 10 * time.Second}
    resp, err := client.Do(req)
    if err != nil {
        return 0, err
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return 0, fmt.Errorf("non-200 response: %d", resp.StatusCode)
    }

    var tsResp timeSeriesAPIResponse
    if err := json.NewDecoder(resp.Body).Decode(&tsResp); err != nil {
        return 0, err
    }

    // We’ll sum up the most recent value from each timeSeries item (if multiple).
    // You may want a different approach: average, min, or max.
    var sum float64
    var count int

    for _, item := range tsResp.Items {
        for _, ts := range item.TimeSeries {
            dataLen := len(ts.Data)
            if dataLen == 0 {
                continue
            }
            // We'll take the last datapoint (could be the first or a different aggregator strategy)
            lastPoint := ts.Data[dataLen-1]
            sum += lastPoint.Value
            count++
        }
    }
    if count == 0 {
        return 0, nil
    }

    // Return the aggregated sum, or an average if you prefer
    return sum, nil
}

// --------------------------------------------------------------------------------
// timeSeriesAPIResponse is a minimal struct that matches the Cloudera Manager
// timeseries JSON response. Adjust the fields as needed for your environment.
// --------------------------------------------------------------------------------
type timeSeriesAPIResponse struct {
    Items []struct {
        TimeSeries []struct {
            Metadata struct {
                MetricName string `json:"metricName"`
            } `json:"metadata"`
            Data []struct {
                Timestamp string  `json:"timestamp"`
                Value     float64 `json:"value"`
            } `json:"data"`
        } `json:"timeSeries"`
    } `json:"items"`
}

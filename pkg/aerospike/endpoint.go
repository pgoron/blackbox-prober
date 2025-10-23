package aerospike

import (
	"crypto/tls"
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	as "github.com/aerospike/aerospike-client-go/v7"
)

var clusterStats = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: ASSuffix + "_aerospike_client_cluster_stats",
	Help: "Cluster aggregated metrics from the go aerospike client",
}, []string{"cluster", "probe_endpoint", "namespace", "name"})

type AerospikeClusterInfo struct {
	ClusterName string
	Config      AerospikeClientConfig
}

type AerospikeNamespacedEndpoint struct {
	ClusterInfo *AerospikeClusterInfo
	Namespace   string
	Logger      log.Logger
}
type AerospikeNamespacedClusterEndpoint struct {
	AerospikeNamespacedEndpoint
	Client *as.Client
}

func (e *AerospikeNamespacedClusterEndpoint) GetHash() string {
	return fmt.Sprintf("%s/%s/ns:%s", e.ClusterInfo.ClusterName, e.ClusterInfo.ClusterName, e.Namespace)
}

func (e *AerospikeNamespacedClusterEndpoint) GetName() string {
	return e.ClusterInfo.ClusterName
}

func (e *AerospikeNamespacedClusterEndpoint) IsCluster() bool {
	return true
}

func (e *AerospikeNamespacedClusterEndpoint) setMetricFromASStats(stats map[string]interface{}, key string) {
	val, ok := stats[key]
	if !ok {
		return
	}

	value, ok := val.(float64)
	if !ok {
		return
	}
	clusterStats.WithLabelValues(e.ClusterInfo.ClusterName, e.GetName(), e.Namespace, key).Set(value)
}

func (e *AerospikeNamespacedClusterEndpoint) refreshMetrics() {
	stats, err := e.Client.Stats()
	cluster_stats := stats["cluster-aggregated-stats"].(map[string]interface{})
	if err != nil {
		level.Error(e.Logger).Log("msg", "Failed to pull metrics from aerospike client", "err", err)
		return
	}
	e.setMetricFromASStats(cluster_stats, "open-connections")
	e.setMetricFromASStats(cluster_stats, "closed-connections")
	e.setMetricFromASStats(cluster_stats, "connections-attempts")
	e.setMetricFromASStats(cluster_stats, "connections-successful")
	e.setMetricFromASStats(cluster_stats, "connections-failed")
	e.setMetricFromASStats(cluster_stats, "connections-pool-empty")
	e.setMetricFromASStats(cluster_stats, "node-added-count")
	e.setMetricFromASStats(cluster_stats, "node-removed-count")
	e.setMetricFromASStats(cluster_stats, "partition-map-updates")
	e.setMetricFromASStats(cluster_stats, "tends-total")
	e.setMetricFromASStats(cluster_stats, "tends-successful")
	e.setMetricFromASStats(cluster_stats, "tends-failed")
}

func (e *AerospikeNamespacedClusterEndpoint) Connect() error {
	clientPolicy := as.NewClientPolicy()
	clientPolicy.ConnectionQueueSize = e.ClusterInfo.Config.genericConfig.ConnectionQueueSize
	clientPolicy.OpeningConnectionThreshold = e.ClusterInfo.Config.genericConfig.OpeningConnectionThreshold
	clientPolicy.MinConnectionsPerNode = e.ClusterInfo.Config.genericConfig.MinConnectionsPerNode
	clientPolicy.TendInterval = e.ClusterInfo.Config.genericConfig.TendInterval

	if e.ClusterInfo.Config.tlsEnabled {
		// Setup TLS Config
		tlsConfig := &tls.Config{
			InsecureSkipVerify:       e.ClusterInfo.Config.genericConfig.TLSSkipVerify,
			PreferServerCipherSuites: true,
		}
		clientPolicy.TlsConfig = tlsConfig
	}

	if e.ClusterInfo.Config.authEnabled {
		if e.ClusterInfo.Config.genericConfig.AuthExternal {
			clientPolicy.AuthMode = as.AuthModeExternal
		} else {
			clientPolicy.AuthMode = as.AuthModeInternal
		}

		clientPolicy.User = e.ClusterInfo.Config.username
		clientPolicy.Password = e.ClusterInfo.Config.password
	}

	client, err := as.NewClientWithPolicyAndHost(clientPolicy, &e.ClusterInfo.Config.host)
	if err != nil {
		return err
	}
	e.Client = client
	e.Refresh()
	return nil
}

func (e *AerospikeNamespacedClusterEndpoint) Refresh() error {
	e.refreshMetrics()
	return nil
}

func (e *AerospikeNamespacedClusterEndpoint) Close() error {
	if e != nil && e.Client != nil {
		e.Client.Close()
	}
	return nil
}

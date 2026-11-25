package cluster_test

// WI-054. Every refusal has a test, because every one of them is a mistake that would
// otherwise show up as data rather than as an error.

import (
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/niqbal/workflow-engine/internal/cluster"
)

// goodKey is 32 bytes, base64, which is the shortest a cluster will take.
var goodKey = base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))

func env(pairs ...string) func(string) string {
	m := map[string]string{}
	for i := 0; i+1 < len(pairs); i += 2 {
		m[pairs[i]] = pairs[i+1]
	}
	return func(k string) string { return m[k] }
}

const twoNodeYAML = `
node: node-a
listen: {api: ":7233", internal: ":7234"}
advertise: laptop.local:7234
peers:
  node-a: laptop.local:7234
  node-b: mini.local:7234
store: {kind: postgres, url_env: WFE_PG_URL}
cluster_key_env: WFE_CLUSTER_KEY
num_shards: 64
max_clock_skew: 1s
`

func goodEnv() func(string) string {
	return env("WFE_PG_URL", "postgres://db/wfe", "WFE_CLUSTER_KEY", goodKey)
}

func TestParseAWorkingTwoNodeConfig(t *testing.T) {
	c, err := cluster.Parse([]byte(twoNodeYAML), goodEnv())
	if err != nil {
		t.Fatal(err)
	}
	if c.Node != "node-a" || c.NumShards != 64 || c.MaxClockSkew != time.Second {
		t.Fatalf("config %+v", c)
	}
	if c.Listen.API != ":7233" || c.Listen.Internal != ":7234" {
		t.Fatalf("listeners %+v", c.Listen)
	}
	if !c.Clustered() {
		t.Error("two peers is not clustered")
	}
	if got := string(c.StoreURL); got != "postgres://db/wfe" {
		t.Errorf("store URL %q, want the value from the environment", got)
	}
	if len(c.ClusterKey) != 32 {
		t.Errorf("cluster key is %d bytes", len(c.ClusterKey))
	}
	if got := c.TransportPeers(); len(got) != 2 || got["node-b"] != "mini.local:7234" {
		t.Errorf("transport peers %v", got)
	}
	if got := c.PeerNames(); strings.Join(got, ",") != "node-a,node-b" {
		t.Errorf("peer names %v, want them sorted", got)
	}
	// Defaults fill in what the file leaves out.
	if c.LeaseDuration != cluster.DefaultLease {
		t.Errorf("lease %s, want the default %s", c.LeaseDuration, cluster.DefaultLease)
	}
	// A config must not print its secrets.
	if s := c.String(); strings.Contains(s, "postgres://db/wfe") || strings.Contains(s, goodKey) {
		t.Errorf("String leaks a secret: %s", s)
	}
}

func TestLoadReadsAFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wfe.yaml")
	if err := os.WriteFile(path, []byte(twoNodeYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.Load(path, goodEnv()); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.Load(filepath.Join(t.TempDir(), "absent.yaml"), goodEnv()); err == nil {
		t.Fatal("a missing file was accepted")
	} else if !errors.Is(err, cluster.ErrConfig) {
		t.Errorf("missing file: %v, want an ErrConfig", err)
	}
}

// TestEveryRefusal is the acceptance criterion: each refusal, with the message a reader needs.
func TestEveryRefusal(t *testing.T) {
	cases := []struct {
		name   string
		yaml   string
		getenv func(string) string
		// want lists fragments the message must contain: what was wrong, and why it matters.
		want []string
	}{
		{
			name: "node missing from peers",
			yaml: `
node: node-c
listen: {api: ":7233", internal: ":7234"}
peers: {node-a: a:7234, node-b: b:7234}
store: {kind: postgres, url_env: WFE_PG_URL}
cluster_key_env: WFE_CLUSTER_KEY
`,
			want: []string{"node-c", "not in peers", "node-a, node-b", "serves nothing"},
		},
		{
			name: "memory store with peers",
			yaml: `
node: node-a
listen: {api: ":7233", internal: ":7234"}
peers: {node-a: a:7234, node-b: b:7234}
store: {kind: memory}
cluster_key_env: WFE_CLUSTER_KEY
`,
			want: []string{"memory", "2 peers", "share nothing"},
		},
		{
			name: "missing cluster key with peers",
			yaml: `
node: node-a
listen: {api: ":7233", internal: ":7234"}
peers: {node-a: a:7234, node-b: b:7234}
store: {kind: postgres, url_env: WFE_PG_URL}
`,
			want: []string{"cluster_key_env is required", "every node must have the same one"},
		},
		{
			name: "short cluster key",
			yaml: twoNodeYAML,
			getenv: env("WFE_PG_URL", "postgres://db/wfe",
				"WFE_CLUSTER_KEY", base64.StdEncoding.EncodeToString([]byte("too short"))),
			want: []string{"WFE_CLUSTER_KEY", "9 bytes", "at least 32"},
		},
		{
			name:   "cluster key not base64",
			yaml:   twoNodeYAML,
			getenv: env("WFE_PG_URL", "postgres://db/wfe", "WFE_CLUSTER_KEY", "not base64!!"),
			want:   []string{"WFE_CLUSTER_KEY", "base64"},
		},
		{
			name:   "cluster key unset",
			yaml:   twoNodeYAML,
			getenv: env("WFE_PG_URL", "postgres://db/wfe"),
			want:   []string{"WFE_CLUSTER_KEY", "empty or unset"},
		},
		{
			name:   "store url unset",
			yaml:   twoNodeYAML,
			getenv: env("WFE_CLUSTER_KEY", goodKey),
			want:   []string{"WFE_PG_URL", "empty or unset"},
		},
		{
			name: "postgres without url_env",
			yaml: `
node: node-a
peers: {node-a: a:7234}
store: {kind: postgres}
`,
			want: []string{"store.url_env is required"},
		},
		{
			name: "unknown store kind",
			yaml: `
node: node-a
peers: {node-a: a:7234}
store: {kind: sqlite}
`,
			want: []string{"sqlite", "memory, postgres"},
		},
		{
			name: "no peers at all",
			yaml: `
node: node-a
store: {kind: memory}
`,
			want: []string{"peers is required", "including this one"},
		},
		{
			name: "peer with no address",
			yaml: `
node: node-a
peers: {node-a: a:7234, node-b: ""}
store: {kind: postgres, url_env: WFE_PG_URL}
cluster_key_env: WFE_CLUSTER_KEY
`,
			want: []string{"node-b", "no address"},
		},
		{
			name: "one port for both listeners",
			yaml: `
node: node-a
listen: {api: ":7233", internal: ":7233"}
peers: {node-a: a:7233, node-b: b:7233}
store: {kind: postgres, url_env: WFE_PG_URL}
cluster_key_env: WFE_CLUSTER_KEY
`,
			want: []string{":7233", "its own port", "firewalled"},
		},
		{
			name: "no node name",
			yaml: `
peers: {node-a: a:7234}
store: {kind: memory}
`,
			want: []string{"node is required"},
		},
		{
			name: "zero shards",
			yaml: `
node: node-a
peers: {node-a: a:7234}
store: {kind: memory}
num_shards: -1
`,
			want: []string{"num_shards", "at least 1"},
		},
		{
			name: "lease shorter than the clock skew",
			yaml: `
node: node-a
peers: {node-a: a:7234}
store: {kind: memory}
lease_duration: 500ms
max_clock_skew: 2s
`,
			want: []string{"lease_duration", "max_clock_skew", "expire while its owner thinks it holds it"},
		},
		{
			name: "misspelled field",
			yaml: `
node: node-a
peers: {node-a: a:7234}
store: {kind: memory}
num_shardz: 64
`,
			want: []string{"num_shardz"},
		},
		{
			name: "not yaml at all",
			yaml: "node: [this is not a node name",
			want: []string{"cluster config"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			getenv := tc.getenv
			if getenv == nil {
				getenv = goodEnv()
			}
			_, err := cluster.Parse([]byte(tc.yaml), getenv)
			if err == nil {
				t.Fatal("the config was accepted")
			}
			if !errors.Is(err, cluster.ErrConfig) {
				t.Errorf("%v does not wrap ErrConfig", err)
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("message %q does not contain %q", err, want)
				}
			}
		})
	}
}

func TestASingleNodeNeedsNoKeyOrInternalListener(t *testing.T) {
	c, err := cluster.Parse([]byte(`
node: only
peers: {only: localhost:7234}
store: {kind: memory}
`), env())
	if err != nil {
		t.Fatal(err)
	}
	if c.Clustered() {
		t.Error("one peer is clustered")
	}
	if len(c.ClusterKey) != 0 {
		t.Error("a single node was given a cluster key it did not configure")
	}
	if c.Listen.API != cluster.DefaultAPIListen {
		t.Errorf("api listener %q", c.Listen.API)
	}
	if c.NumShards != cluster.DefaultNumShards {
		t.Errorf("num_shards %d, want the default", c.NumShards)
	}
}

func TestAdvertiseDefaultsToThePeerEntry(t *testing.T) {
	c, err := cluster.Parse([]byte(twoNodeYAML), goodEnv())
	if err != nil {
		t.Fatal(err)
	}
	if c.Advertise != "laptop.local:7234" {
		t.Fatalf("advertise %q", c.Advertise)
	}
	// Without an explicit advertise, the node's own peers entry is what peers dial.
	c, err = cluster.Parse([]byte(`
node: node-a
listen: {api: ":7233", internal: ":7234"}
peers:
  node-a: laptop.local:7234
  node-b: mini.local:7234
store: {kind: postgres, url_env: WFE_PG_URL}
cluster_key_env: WFE_CLUSTER_KEY
`), goodEnv())
	if err != nil {
		t.Fatal(err)
	}
	if c.Advertise != "laptop.local:7234" {
		t.Fatalf("advertise %q, want the peers entry", c.Advertise)
	}
}

func TestRawBase64KeyIsAccepted(t *testing.T) {
	raw := base64.RawStdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	c, err := cluster.Parse([]byte(twoNodeYAML),
		env("WFE_PG_URL", "postgres://db/wfe", "WFE_CLUSTER_KEY", raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.ClusterKey) != 32 {
		t.Fatalf("key is %d bytes", len(c.ClusterKey))
	}
}

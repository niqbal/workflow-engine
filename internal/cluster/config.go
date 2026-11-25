// Package cluster reads wfe.yaml: what a node is called, who its peers are, where the store
// is, and the key the cluster shares (WI-054, SPEC §13.2, docs/MULTI-MACHINE.md).
//
// Loading a config is mostly refusing one. Every check here is a mistake that would otherwise
// show up as data, not as an error: a node missing from the peer list cannot be reached by
// anyone and quietly serves nothing; two nodes with different shard counts route by different
// mappings and write to each other's shards; a cluster on an in-memory store has as many
// unrelated engines as it has nodes; and a cluster key that differs between nodes makes every
// task token one node issues invalid at the next. The one check that cannot be done from the
// file — the shard count the cluster was created with — belongs to the store, which is why
// it is EnsureCluster's job and not this package's.
//
// Secrets never live in the file. It names the environment variables that hold them.
package cluster

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/niqbal/workflow-engine/internal/transport"
)

// StoreKind is where a node keeps its state.
type StoreKind string

const (
	// StoreMemory is the in-memory store: one process, nothing survives it, and no cluster.
	StoreMemory StoreKind = "memory"
	// StorePostgres is the shared store a cluster needs.
	StorePostgres StoreKind = "postgres"
)

// MinClusterKeyBytes is the shortest key a cluster will accept. The key signs task tokens and
// authenticates peer calls, so it is a secret, not an identifier.
const MinClusterKeyBytes = 32

// Listen is a node's two listeners: the public API and the node-to-node endpoint, on
// separate ports so the internal one can be firewalled to the peers.
type Listen struct {
	API      string `yaml:"api"`
	Internal string `yaml:"internal"`
}

// Store says where the cluster's state lives.
type Store struct {
	Kind StoreKind `yaml:"kind"`
	// URLEnv is the environment variable holding the connection URL. The URL itself is
	// never in the file.
	URLEnv string `yaml:"url_env"`
}

// Config is a parsed wfe.yaml.
type Config struct {
	Node      string            `yaml:"node"`
	Listen    Listen            `yaml:"listen"`
	Advertise string            `yaml:"advertise"`
	Peers     map[string]string `yaml:"peers"`
	Store     Store             `yaml:"store"`
	// ClusterKeyEnv names the environment variable holding the cluster key, base64, at
	// least MinClusterKeyBytes once decoded.
	ClusterKeyEnv string        `yaml:"cluster_key_env"`
	NumShards     int           `yaml:"num_shards"`
	MaxClockSkew  time.Duration `yaml:"max_clock_skew"`
	LeaseDuration time.Duration `yaml:"lease_duration"`

	// Resolved values, filled by Load.
	StoreURL   []byte `yaml:"-"` // never printed; see String
	ClusterKey []byte `yaml:"-"`
}

// Defaults applied when the file leaves a field out.
const (
	DefaultNumShards    = 64
	DefaultMaxClockSkew = time.Second
	DefaultLease        = 30 * time.Second
	DefaultAPIListen    = ":7233"
	DefaultInternal     = ":7234"
)

// ErrConfig is every refusal from this package, so a caller can tell a bad config from a
// store or network failure.
var ErrConfig = errors.New("cluster config")

func configErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrConfig, fmt.Sprintf(format, args...))
}

// Load reads and checks a config file. getenv supplies the environment; nil means the
// process's own, which is what the server uses and what a test replaces.
func Load(path string, getenv func(string) string) (Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("%w: %v", ErrConfig, err)
	}
	return Parse(raw, getenv)
}

// Parse checks an already-read config. Errors name the field and what was wrong with it.
func Parse(raw []byte, getenv func(string) string) (Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	var c Config
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true) // a misspelled field is a refusal, not a silent default
	if err := dec.Decode(&c); err != nil {
		return Config{}, configErr("%v", err)
	}
	c.applyDefaults()
	if err := c.check(getenv); err != nil {
		return Config{}, err
	}
	return c, nil
}

func (c *Config) applyDefaults() {
	if c.NumShards == 0 {
		c.NumShards = DefaultNumShards
	}
	if c.MaxClockSkew == 0 {
		c.MaxClockSkew = DefaultMaxClockSkew
	}
	if c.LeaseDuration == 0 {
		c.LeaseDuration = DefaultLease
	}
	if c.Listen.API == "" {
		c.Listen.API = DefaultAPIListen
	}
	if c.Store.Kind == "" {
		c.Store.Kind = StorePostgres
	}
	if len(c.Peers) > 1 && c.Listen.Internal == "" {
		c.Listen.Internal = DefaultInternal
	}
	if c.Advertise == "" {
		c.Advertise = c.Peers[c.Node]
	}
}

func (c *Config) check(getenv func(string) string) error {
	if c.Node == "" {
		return configErr("node is required")
	}
	if c.NumShards < 1 {
		return configErr("num_shards must be at least 1, got %d", c.NumShards)
	}
	if c.MaxClockSkew < 0 {
		return configErr("max_clock_skew cannot be negative")
	}
	if c.LeaseDuration <= c.MaxClockSkew {
		return configErr("lease_duration (%s) must be longer than max_clock_skew (%s): a lease "+
			"shorter than the clocks' disagreement would expire while its owner thinks it holds it",
			c.LeaseDuration, c.MaxClockSkew)
	}
	if len(c.Peers) == 0 {
		return configErr("peers is required: list every node, including this one")
	}
	if _, ok := c.Peers[c.Node]; !ok {
		return configErr("node %q is not in peers (%s): a node its peers cannot reach serves nothing",
			c.Node, strings.Join(c.PeerNames(), ", "))
	}
	for _, name := range c.PeerNames() {
		if strings.TrimSpace(c.Peers[name]) == "" {
			return configErr("peer %q has no address", name)
		}
	}
	if c.Advertise == "" {
		return configErr("advertise is required when peers does not give this node an address")
	}

	switch c.Store.Kind {
	case StoreMemory:
		if len(c.Peers) > 1 {
			return configErr("store.kind is memory with %d peers: nodes on an in-memory store "+
				"share nothing, so each would run its own engine under one cluster's name",
				len(c.Peers))
		}
	case StorePostgres:
		if c.Store.URLEnv == "" {
			return configErr("store.url_env is required for a postgres store")
		}
		url := getenv(c.Store.URLEnv)
		if strings.TrimSpace(url) == "" {
			return configErr("store.url_env names %s, which is empty or unset", c.Store.URLEnv)
		}
		c.StoreURL = []byte(url)
	default:
		return configErr("store.kind %q is not one of memory, postgres", c.Store.Kind)
	}

	if len(c.Peers) > 1 {
		if c.Listen.Internal == "" {
			return configErr("listen.internal is required with more than one peer")
		}
		if c.Listen.Internal == c.Listen.API {
			return configErr("listen.api and listen.internal are both %q: the internal endpoint "+
				"needs its own port so it can be firewalled to the peers", c.Listen.API)
		}
		if c.ClusterKeyEnv == "" {
			return configErr("cluster_key_env is required with more than one peer: the key signs " +
				"task tokens and peer calls, and every node must have the same one")
		}
	}
	if c.ClusterKeyEnv != "" {
		key, err := decodeKey(getenv(c.ClusterKeyEnv))
		if err != nil {
			return configErr("cluster_key_env names %s, which %v", c.ClusterKeyEnv, err)
		}
		c.ClusterKey = key
	}
	return nil
}

// decodeKey reads a base64 cluster key and insists it is long enough to be a secret.
func decodeKey(v string) ([]byte, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil, errors.New("is empty or unset")
	}
	key, err := base64.StdEncoding.DecodeString(v)
	if err != nil {
		if raw, rawErr := base64.RawStdEncoding.DecodeString(v); rawErr == nil {
			key = raw
		} else {
			return nil, fmt.Errorf("is not valid base64: %v", err)
		}
	}
	if len(key) < MinClusterKeyBytes {
		return nil, fmt.Errorf("decodes to %d bytes; a cluster key needs at least %d",
			len(key), MinClusterKeyBytes)
	}
	return key, nil
}

// PeerNames lists the configured nodes in a stable order.
func (c Config) PeerNames() []string {
	out := make([]string, 0, len(c.Peers))
	for name := range c.Peers {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// TransportPeers is the peer map the HTTP transport takes.
func (c Config) TransportPeers() map[transport.NodeID]string {
	out := make(map[transport.NodeID]string, len(c.Peers))
	for name, addr := range c.Peers {
		out[transport.NodeID(name)] = addr
	}
	return out
}

// Clustered reports whether this node has peers to talk to.
func (c Config) Clustered() bool { return len(c.Peers) > 1 }

// String describes the config without its secrets, for a start-up line.
func (c Config) String() string {
	return fmt.Sprintf("node=%s api=%s internal=%s advertise=%s peers=%d store=%s shards=%d lease=%s skew=%s",
		c.Node, c.Listen.API, c.Listen.Internal, c.Advertise, len(c.Peers), c.Store.Kind,
		c.NumShards, c.LeaseDuration, c.MaxClockSkew)
}

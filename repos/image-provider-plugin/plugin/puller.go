// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plugin

import (
	"context"
	"crypto/sha256"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ai-on-gke/image-provider-plugin/client"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

const (
	// defaultPullerTTL is how long a registry session (the ping and bearer
	// token of a repository) is reused. Request credentials such as GCE
	// access tokens rotate within an hour, and a cached session keeps
	// refreshing its bearer token with the credentials it started with.
	defaultPullerTTL = 10 * time.Minute
	// defaultMaxPullers bounds the number of cached registry sessions.
	defaultMaxPullers = 64
)

// pullerCache shares go-containerregistry Pullers between registry calls
// that use the same credentials and transport. A Puller pings the registry
// and exchanges the credentials for a bearer token once per repository and
// reuses the token, so preparing many images from one repository costs one
// token exchange instead of one or more per image. Each remote.Image or
// remote.Layer call otherwise pings and fetches a token, which at thousands
// of cold images a minute exhausts registry request quotas.
type pullerCache struct {
	ttl time.Duration
	max int
	now func() time.Time

	mu      sync.Mutex
	entries map[pullerKey]*pullerEntry
}

type pullerKey struct {
	creds     [sha256.Size]byte
	transport string
}

type pullerEntry struct {
	puller  *remote.Puller
	created time.Time
}

// newPullerCache returns a pullerCache that reuses a Puller for ttl and
// keeps at most max of them.
func newPullerCache(ttl time.Duration, max int) *pullerCache {
	return &pullerCache{
		ttl:     ttl,
		max:     max,
		now:     time.Now,
		entries: make(map[pullerKey]*pullerEntry),
	}
}

// options returns the remote options for a registry call authenticated by
// authConfig, then keychain (see RemoteOptions), over transport, or
// remote.DefaultTransport when transport is nil. Calls share a Puller only
// when they pass the same credentials and transportName.
//
// Call done with the error of the remote.Image, remote.Layer or similar call
// that used the options. A Puller keeps the first error of its ping and
// token exchange, so done drops the Puller on any error, and the next call
// starts a new session.
func (c *pullerCache) options(ctx context.Context, keychain authn.Keychain, authConfig *client.AuthConfig, transportName string, transport http.RoundTripper) ([]remote.Option, func(error), error) {
	opts := RemoteOptions(ctx, keychain, authConfig)
	if transport != nil {
		opts = append(opts, remote.WithTransport(transport))
	}
	key := pullerKey{creds: credentialDigest(authConfig), transport: transportName}
	puller, err := c.get(key, opts)
	if err != nil {
		return nil, nil, err
	}
	done := func(err error) {
		if err != nil {
			c.drop(key, puller)
		}
	}
	return append(opts, remote.Reuse(puller)), done, nil
}

func (c *pullerCache) get(key pullerKey, opts []remote.Option) (*remote.Puller, error) {
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[key]; ok {
		if now.Sub(e.created) < c.ttl {
			return e.puller, nil
		}
		delete(c.entries, key)
	}
	// The ping and token exchange of a session run with the context of the
	// call that starts it. Later requests use the context of their call.
	p, err := remote.NewPuller(opts...)
	if err != nil {
		return nil, err
	}
	if len(c.entries) >= c.max {
		c.evictOldestLocked()
	}
	c.entries[key] = &pullerEntry{puller: p, created: now}
	return p, nil
}

func (c *pullerCache) drop(key pullerKey, p *remote.Puller) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[key]; ok && e.puller == p {
		delete(c.entries, key)
	}
}

func (c *pullerCache) evictOldestLocked() {
	var oldest pullerKey
	var oldestAt time.Time
	first := true
	for k, e := range c.entries {
		if first || e.created.Before(oldestAt) {
			oldest, oldestAt, first = k, e.created, false
		}
	}
	if !first {
		delete(c.entries, oldest)
	}
}

// credentialDigest identifies the credentials of authConfig. Requests
// without credentials, which fall back to the keychain, share one digest.
func credentialDigest(a *client.AuthConfig) [sha256.Size]byte {
	if a == nil {
		return sha256.Sum256(nil)
	}
	return sha256.Sum256([]byte(strings.Join([]string{
		a.ServerAddress,
		a.Username,
		a.Password,
		a.Auth,
		a.IdentityToken,
		a.RegistryToken,
	}, "\x00")))
}

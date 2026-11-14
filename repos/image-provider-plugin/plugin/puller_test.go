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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ai-on-gke/image-provider-plugin/client"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// tokenRegistry serves an in-memory registry that wants a bearer token from
// its /token endpoint, and counts the token requests.
type tokenRegistry struct {
	srv    *httptest.Server
	tokens atomic.Int64
	// failTokens is how many token requests fail first. They fail with
	// HTTP 403, since go-containerregistry retries a 429.
	failTokens atomic.Int64
}

func newTokenRegistry(t *testing.T) *tokenRegistry {
	t.Helper()
	r := &tokenRegistry{}
	reg := registry.New()
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/token" {
			r.tokens.Add(1)
			if r.failTokens.Add(-1) >= 0 {
				http.Error(w, "denied", http.StatusForbidden)
				return
			}
			_, _ = w.Write([]byte(`{"token":"t"}`))
			return
		}
		if req.Header.Get("Authorization") != "Bearer t" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+r.srv.URL+`/token",service="test"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		reg.ServeHTTP(w, req)
	}))
	t.Cleanup(r.srv.Close)
	return r
}

// push writes n random images to the registry and returns their refs.
func (r *tokenRegistry) push(t *testing.T, n int) []string {
	t.Helper()
	repo, err := name.NewRepository(strings.TrimPrefix(r.srv.URL, "http://") + "/sandbox")
	if err != nil {
		t.Fatal(err)
	}
	var refs []string
	for range n {
		img, err := random.Image(64, 1)
		if err != nil {
			t.Fatal(err)
		}
		d, err := img.Digest()
		if err != nil {
			t.Fatal(err)
		}
		ref := repo.Digest(d.String())
		if err := remote.Write(ref, img); err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref.String())
	}
	return refs
}

func TestDefaultImageResolverReusesToken(t *testing.T) {
	alice := &client.AuthConfig{Username: "_token", Password: "alice"}
	bob := &client.AuthConfig{Username: "_token", Password: "bob"}
	for _, tc := range []struct {
		name string
		// auths holds the credentials of each resolve, in order.
		auths      []*client.AuthConfig
		failTokens int64
		// expireAfter advances the clock past the session TTL after this
		// many resolves. Zero keeps the clock.
		expireAfter int
		wantErrs    int
		wantTokens  int64
	}{
		{
			name:       "one token for all images",
			auths:      []*client.AuthConfig{alice, alice, alice},
			wantTokens: 1,
		},
		{
			name:       "one token per credential",
			auths:      []*client.AuthConfig{alice, bob, alice, bob},
			wantTokens: 2,
		},
		{
			name:       "new session after a failed token exchange",
			auths:      []*client.AuthConfig{alice, alice, alice},
			failTokens: 1,
			wantErrs:   1,
			wantTokens: 2,
		},
		{
			name:        "new session after the TTL",
			auths:       []*client.AuthConfig{alice, alice, alice},
			expireAfter: 2,
			wantTokens:  2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := newTokenRegistry(t)
			refs := reg.push(t, len(tc.auths))
			reg.tokens.Store(0)
			reg.failTokens.Store(tc.failTokens)
			d, _ := newTestDriver(t)
			now := time.Now()
			d.pullers.now = func() time.Time { return now }

			errs := 0
			for i, auth := range tc.auths {
				if tc.expireAfter > 0 && i == tc.expireAfter {
					now = now.Add(defaultPullerTTL)
				}
				digest, _, _, layers, err := d.defaultImageResolver(t.Context(), refs[i], auth)
				if err != nil {
					errs++
					continue
				}
				if want := refs[i][strings.LastIndex(refs[i], "@")+1:]; digest != want || len(layers) != 1 {
					t.Errorf("resolve %d = (%s, %d layers), want (%s, 1 layer)", i, digest, len(layers), want)
				}
			}
			if errs != tc.wantErrs {
				t.Errorf("failed resolves = %d, want %d", errs, tc.wantErrs)
			}
			if got := reg.tokens.Load(); got != tc.wantTokens {
				t.Errorf("token requests = %d, want %d", got, tc.wantTokens)
			}
		})
	}
}

func TestPullerCacheEvictsOldest(t *testing.T) {
	c := newPullerCache(time.Hour, 2)
	now := time.Now()
	c.now = func() time.Time { return now }
	for i, pw := range []string{"a", "b", "c"} {
		now = now.Add(time.Duration(i) * time.Second)
		if _, _, err := c.options(t.Context(), nil, &client.AuthConfig{Username: "u", Password: pw}, "", nil); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(c.entries); got != 2 {
		t.Fatalf("cached sessions = %d, want 2", got)
	}
	for _, k := range []string{"b", "c"} {
		if _, ok := c.entries[pullerKey{creds: credentialDigest(&client.AuthConfig{Username: "u", Password: k})}]; !ok {
			t.Errorf("session for %q was evicted, want the oldest (a) evicted", k)
		}
	}
}

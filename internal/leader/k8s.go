package leader

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// k8sClient talks to the in-cluster Kubernetes API using the pod's service
// account token. It patches the pod's own labels and manages a coordination.k8s.io
// Lease — no client-go dependency.
type k8sClient struct {
	podName   string
	namespace string
	client    *http.Client
	apiURL    string
	token     string
}

func newK8sClient(podName, namespace string) (*k8sClient, error) {
	token, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return nil, fmt.Errorf("read service account token: %w", err)
	}

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if ca, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"); err == nil {
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(ca)
		tlsConfig.RootCAs = pool
	}

	return &k8sClient{
		podName:   podName,
		namespace: namespace,
		token:     string(token),
		apiURL:    "https://kubernetes.default.svc",
		client: &http.Client{
			Timeout:   5 * time.Second,
			Transport: &http.Transport{TLSClientConfig: tlsConfig},
		},
	}, nil
}

// setLeaderLabel sets or removes the postkeys/role label on this pod.
func (c *k8sClient) setLeaderLabel(ctx context.Context, leader bool) error {
	var value interface{} = "leader"
	if !leader {
		value = nil // null removes the label
	}

	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"labels": map[string]interface{}{
				"postkeys/role": value,
			},
		},
	}

	body, err := json.Marshal(patch)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s", c.apiURL, c.namespace, c.podName)
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/merge-patch+json")
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) //nolint:errcheck

	if resp.StatusCode >= 300 {
		return fmt.Errorf("kubernetes API returned %d", resp.StatusCode)
	}
	return nil
}

// lease is a minimal subset of coordination.k8s.io/v1 Lease.
// Pointer fields let us distinguish "absent" from "zero" when sending PUT bodies.
type lease struct {
	Kind       string        `json:"kind,omitempty"`
	APIVersion string        `json:"apiVersion,omitempty"`
	Metadata   leaseMetadata `json:"metadata"`
	Spec       leaseSpec     `json:"spec"`
}

type leaseMetadata struct {
	Name            string `json:"name"`
	Namespace       string `json:"namespace,omitempty"`
	ResourceVersion string `json:"resourceVersion,omitempty"`
}

type leaseSpec struct {
	HolderIdentity       *string `json:"holderIdentity"`
	LeaseDurationSeconds *int32  `json:"leaseDurationSeconds"`
	AcquireTime          *string `json:"acquireTime,omitempty"`
	RenewTime            *string `json:"renewTime,omitempty"`
	LeaseTransitions     *int32  `json:"leaseTransitions,omitempty"`
}

// microTime formats t as a Kubernetes MicroTime (RFC3339 with microseconds, UTC).
func microTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000Z")
}

func strPtr(s string) *string { return &s }
func int32Ptr(i int32) *int32 { return &i }

func (c *k8sClient) leaseURL(name string) string {
	if name == "" {
		return fmt.Sprintf("%s/apis/coordination.k8s.io/v1/namespaces/%s/leases", c.apiURL, c.namespace)
	}
	return fmt.Sprintf("%s/apis/coordination.k8s.io/v1/namespaces/%s/leases/%s", c.apiURL, c.namespace, name)
}

func (c *k8sClient) doLease(ctx context.Context, method, url string, body []byte) (*lease, int, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return nil, 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var l lease
		if err := json.Unmarshal(respBody, &l); err != nil {
			return nil, resp.StatusCode, fmt.Errorf("decode lease: %w", err)
		}
		return &l, resp.StatusCode, nil
	}
	return nil, resp.StatusCode, fmt.Errorf("kubernetes API returned %d: %s", resp.StatusCode, string(respBody))
}

// getLease fetches the Lease by name. Returns (nil, 404, nil) if it doesn't exist.
func (c *k8sClient) getLease(ctx context.Context, name string) (*lease, int, error) {
	l, status, err := c.doLease(ctx, http.MethodGet, c.leaseURL(name), nil)
	if status == http.StatusNotFound {
		return nil, status, nil
	}
	return l, status, err
}

// createLease POSTs a new Lease with this pod as the holder.
func (c *k8sClient) createLease(ctx context.Context, name string, durationSeconds int32, now time.Time) (*lease, int, error) {
	l := lease{
		Kind:       "Lease",
		APIVersion: "coordination.k8s.io/v1",
		Metadata: leaseMetadata{
			Name:      name,
			Namespace: c.namespace,
		},
		Spec: leaseSpec{
			HolderIdentity:       strPtr(c.podName),
			LeaseDurationSeconds: int32Ptr(durationSeconds),
			AcquireTime:          strPtr(microTime(now)),
			RenewTime:            strPtr(microTime(now)),
			LeaseTransitions:     int32Ptr(0),
		},
	}
	body, err := json.Marshal(l)
	if err != nil {
		return nil, 0, err
	}
	return c.doLease(ctx, http.MethodPost, c.leaseURL(""), body)
}

// updateLease PUTs the Lease. Requires l.Metadata.ResourceVersion to be set for
// optimistic concurrency; a 409 response means the caller must re-read and retry.
func (c *k8sClient) updateLease(ctx context.Context, l *lease) (*lease, int, error) {
	l.Kind = "Lease"
	l.APIVersion = "coordination.k8s.io/v1"
	body, err := json.Marshal(l)
	if err != nil {
		return nil, 0, err
	}
	return c.doLease(ctx, http.MethodPut, c.leaseURL(l.Metadata.Name), body)
}

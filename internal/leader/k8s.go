package leader

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

// k8sLabeler patches the pod's own labels via the Kubernetes API.
// Uses the in-cluster service account token — no client-go dependency needed.
type k8sLabeler struct {
	podName   string
	namespace string
	client    *http.Client
	apiURL    string
	token     string
}

func newK8sLabeler(podName, namespace string) (*k8sLabeler, error) {
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

	return &k8sLabeler{
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
func (l *k8sLabeler) setLeaderLabel(leader bool) error {
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

	url := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s", l.apiURL, l.namespace, l.podName)
	req, err := http.NewRequest(http.MethodPatch, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/merge-patch+json")
	req.Header.Set("Authorization", "Bearer "+l.token)

	resp, err := l.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("kubernetes API returned %d", resp.StatusCode)
	}
	return nil
}

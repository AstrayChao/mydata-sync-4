package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	neturl "net/url"
	"os"
	"strings"
	"time"

	monitor "mydata-sync-4/proto"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

var cstZone = time.FixedZone("CST", 8*3600)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	targets := loadTargets("t.csv")
	slog.Info("开始任务", "count", len(targets))
	results := runChecks(targets)
	resultSet := &monitor.ResultSet{
		Results: results,
	}
	if len(resultSet.Results) > 0 {
		pushToWebhook(resultSet)
	}
}

func runChecks(targets []*monitor.CheckResult) []*monitor.CheckResult {
	g, ctx := errgroup.WithContext(context.Background())
	g.SetLimit(50)

	httpClient := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DisableKeepAlives: true,
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
			TLSNextProto:      map[string]func(string, *tls.Conn) http.RoundTripper{},
		},
	}

	for _, t := range targets {
		target := t
		g.Go(func() error {
			checkOneTarget(ctx, httpClient, target)
			return nil
		})
	}

	_ = g.Wait()
	return targets
}

func checkOneTarget(ctx context.Context, httpClient *http.Client, t *monitor.CheckResult) {
	t.CreatedAt = time.Now().In(cstZone).Format(time.DateTime)
	t.StatusCode = 0
	t.ResponseTimeMs = 0
	t.ErrorReason = ""
	t.IsAccessible = false

	normalizedURL, parsedURL, err := normalizeURL(t.GetUrl())
	if err != nil {
		t.ErrorReason = fmt.Sprintf("URL构建失败: %v", err)
		return
	}
	t.Url = normalizedURL

	start := time.Now()
	defer func() {
		t.ResponseTimeMs = time.Since(start).Milliseconds()
	}()

	switch strings.ToLower(parsedURL.Scheme) {
	case "http", "https":
		statusCode, ok, errReason := checkHTTP(ctx, httpClient, normalizedURL)
		// 关键：HTTP 状态码原样上报（404/500/...）
		t.StatusCode = statusCode
		t.IsAccessible = ok
		t.ErrorReason = errReason
	case "ftp", "ftps":
		err := checkFTPConnectivity(ctx, parsedURL, 10*time.Second)
		if err != nil {
			// 非 HTTP 错误不映射，status_code 保持 0
			t.ErrorReason = err.Error()
			t.IsAccessible = false
			t.StatusCode = 0
		} else {
			// 连通即成功，给 200（仅作为成功标记）
			t.IsAccessible = true
			t.StatusCode = 200
			t.ErrorReason = ""
		}
	default:
		t.ErrorReason = "unsupported protocol"
		t.StatusCode = 0
		t.IsAccessible = false
	}
}

func checkHTTP(ctx context.Context, client *http.Client, url string) (int32, bool, string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, false, fmt.Sprintf("URL构建失败: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		// 不映射错误码，status_code=0，细节放 error_reason
		return 0, false, err.Error()
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	code := int32(resp.StatusCode)
	if resp.StatusCode > 0 && resp.StatusCode < 400 {
		return code, true, ""
	}
	return code, false, fmt.Sprintf("HTTP异常: %d %s", resp.StatusCode, http.StatusText(resp.StatusCode))
}

func checkFTPConnectivity(ctx context.Context, parsedURL *neturl.URL, timeout time.Duration) error {
	host := parsedURL.Hostname()
	if host == "" {
		return fmt.Errorf("ftp host is empty")
	}

	port := parsedURL.Port()
	if port == "" {
		if strings.EqualFold(parsedURL.Scheme, "ftps") {
			port = "990"
		} else {
			port = "21"
		}
	}

	addr := net.JoinHostPort(host, port)
	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(timeout))

	if strings.EqualFold(parsedURL.Scheme, "ftps") {
		tlsConn := tls.Client(conn, &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         host,
		})
		if err := tlsConn.Handshake(); err != nil {
			return err
		}
		_ = tlsConn.Close()
	}

	return nil
}

func normalizeURL(raw string) (string, *neturl.URL, error) {
	u := strings.TrimSpace(raw)
	if u == "" {
		return "", nil, fmt.Errorf("empty url")
	}
	if !strings.Contains(u, "://") {
		u = "http://" + u
	}
	parsed, err := neturl.Parse(u)
	if err != nil {
		return u, nil, err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return u, nil, fmt.Errorf("invalid url (missing scheme/host)")
	}
	return u, parsed, nil
}
func pushToWebhook(data *monitor.ResultSet) {
	pbBytes, _ := proto.Marshal(data)
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	gw.Write(pbBytes)
	gw.Close()

	url := strings.TrimSpace(os.Getenv("PROD_WEBHOOK_URL"))
	if url == "" {
		slog.Warn("跳过推送: PROD_WEBHOOK_URL 未设置")
		url = "https://datatrusted.cn/web-monitor-hook/harvest"
		return
	}

	se := os.Getenv("PROD_WEBHOOK_SECRET")
	if se == "" {
		slog.Warn("跳过推送: PROD_WEBHOOK_SECRET 未设置")
		se = "eW6uTAv2bh8zMhdcvbEBKcWhZdWrsm3MwkEMa6sc3tXjQ4huXNAmxe23"
		// return
	}

	req, err := http.NewRequest("POST", url, &buf)
	if err != nil {
		// 如果 URL 格式非法，这里会捕获，而不是 Panic
		slog.Error("创建请求失败 (检查 URL 格式)", "err", err, "url", url)
		return
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("X-Webhook-Secret", se)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		slog.Error("Webhook发送失败", "err", err)
	} else {
		defer resp.Body.Close()
		slog.Info("Webhook推送成功", "count", len(data.Results))
	}
}
func normalizeType(t string) (string, bool) {
	v := strings.ToLower(strings.TrimSpace(t))
	return v, v == "platform" || v == "dataset"
}

func normalizeURLType(t string) string {
	v := strings.ToLower(strings.TrimSpace(t))
	if v == "database_url" || v == "download_url" {
		return v
	}
	return "unknown"
}

func loadTargets(path string) []*monitor.CheckResult {
	f, err := os.Open(path)
	if err != nil {
		os.Exit(1)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.Read() // skip header
	var list []*monitor.CheckResult
	for {
		row, _ := reader.Read()
		if row == nil {
			break
		}
		if len(row) >= 3 {
			id := strings.TrimSpace(row[0])
			typ, ok := normalizeType(row[1])
			url := strings.TrimSpace(row[2])
			urlType := "unknown"
			if len(row) >= 4 {
				urlType = normalizeURLType(row[3])
			}
			if id == "" || url == "" || !ok {
				continue
			}
			list = append(list, &monitor.CheckResult{
				Id:      id,
				Type:    typ,
				Url:     url,
				UrlType: urlType,
			})
		}
	}
	return list
}

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
	monitor "mydata-sync-4/proto"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

// 北京时间时区
var CstZone = time.FixedZone("CST", 8*3600)

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
	g.SetLimit(50) // 并发限制

	// 禁用 KeepAlive 以获得更准确的单次连接耗时 (可选，视需求而定)
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DisableKeepAlives: true,
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
			TLSNextProto:      map[string]func(authority string, c *tls.Conn) http.RoundTripper{},
		},
	}

	for _, t := range targets {
		g.Go(func() error {
			// 1. 设置时间戳
			t.CreatedAt = time.Now().In(CstZone).Format(time.DateTime)
			safeUrl := strings.TrimSpace(t.Url)
			req, err := http.NewRequestWithContext(ctx, "GET", safeUrl, nil)
			if err != nil {
				t.IsAccessible = false
				t.ErrorReason = fmt.Sprintf("URL构建失败: %v", err)
				return nil
			}

			start := time.Now()

			resp, err := client.Do(req)

			t.ResponseTimeMs = time.Since(start).Milliseconds()

			if err != nil {
				t.IsAccessible = false
				t.ErrorReason = err.Error()
			} else {
				t.StatusCode = int32(resp.StatusCode)

				if resp.StatusCode > 0 && resp.StatusCode < 400 {
					t.IsAccessible = true
					t.ErrorReason = ""
				} else {
					t.IsAccessible = false
					t.ErrorReason = fmt.Sprintf("HTTP异常: %d %s", resp.StatusCode, http.StatusText(resp.StatusCode))
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
			return nil
		})
	}
	g.Wait()
	return targets
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
		return
	}

	se := os.Getenv("PROD_WEBHOOK_SECRET")
	if se == "" {
		slog.Warn("跳过推送: PROD_WEBHOOK_SECRET 未设置")
		return
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
			list = append(list, &monitor.CheckResult{
				Id: row[0], Type: row[1], Url: row[2],
			})
		}
	}
	return list
}

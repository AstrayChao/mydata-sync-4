package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/sha256"
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

	// 1. 读取
	targets := loadTargets("t.csv")
	slog.Info("开始任务", "count", len(targets))

	// 2. 检测
	results := runChecks(targets)

	// 3. 分流
	platformSet := &monitor.ResultSet{}
	datasetSet := &monitor.ResultSet{}

	for _, r := range results {
		if r.Type == "platform" {
			platformSet.Results = append(platformSet.Results, r)
		} else {
			datasetSet.Results = append(datasetSet.Results, r)
		}
	}

	// 4. 发送 Platform
	if len(platformSet.Results) > 0 {
		pushToWebhook(platformSet)
	}
	if len(datasetSet.Results) > 0 {
		// 生成带时间戳的文件名: dataset_20251125_183000.bin
		timestamp := time.Now().In(CstZone).Format("20060102_150405")
		fileName := fmt.Sprintf("dataset_%s.bin", timestamp)

		saveProtoFile(fileName, datasetSet)
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
			TLSNextProto: map[string]func(authority string, c *tls.Conn) http.RoundTripper{},
		},
	}

	for _, t := range targets {
		t := t
		g.Go(func() error {
			// 1. 设置时间戳
			t.CreatedAt = time.Now().In(CstZone).Format(time.DateTime)

			req, err := http.NewRequestWithContext(ctx, "GET", safeUrl, nil)
			if err != nil {
				t.IsAccessible = false
				t.ErrorReason = fmt.Sprintf("URL构建失败: %v", err)
				return nil
			}

			// 2. 计时开始
			start := time.Now()

			resp, err := client.Do(req)

			// 3. 计时结束 (毫秒)
			t.ResponseTimeMs = time.Since(start).Milliseconds()

			if err != nil {
				t.IsAccessible = false
				t.ErrorReason = err.Error()
			} else {
				t.IsAccessible = true
				t.StatusCode = int32(resp.StatusCode)
				// 必须读掉 Body 才能准确结束连接
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

// aesEncrypt 使用 AES-CBC 加密数据
func aesEncrypt(data []byte, key string) ([]byte, error) {
	// 使用 SHA256 生成 32 字节的密钥
	keyHash := sha256.Sum256([]byte(key))

	// 创建 AES cipher
	block, err := aes.NewCipher(keyHash[:])
	if err != nil {
		return nil, fmt.Errorf("创建 AES cipher 失败: %v", err)
	}

	// 使用 MD5 作为 IV（初始化向量）
	iv := md5.Sum(keyHash[:])

	// 填充数据到块大小的倍数
	blockSize := block.BlockSize()
	padding := blockSize - len(data)%blockSize
	paddedData := append(data, bytes.Repeat([]byte{byte(padding)}, padding)...)

	// 创建加密模式
	mode := cipher.NewCBCEncrypter(block, iv[:])

	// 加密数据
	encrypted := make([]byte, len(paddedData))
	mode.CryptBlocks(encrypted, paddedData)

	return encrypted, nil
}

func saveProtoFile(name string, data *monitor.ResultSet) {
	// 序列化 protobuf 数据
	b, _ := proto.Marshal(data)

	// 从环境变量获取加密密钥
	encryptionKey := os.Getenv("ENCRYPTION_KEY")
	if encryptionKey == "" {
		slog.Warn("未设置 ENCRYPTION_KEY，将保存未加密的文件")
		os.WriteFile(name, b, 0644)
		slog.Info("保存文件成功", "file", name, "count", len(data.Results))
		return
	}

	// 使用 AES 加密数据
	encryptedData, err := aesEncrypt(b, encryptionKey)
	if err != nil {
		slog.Error("加密数据失败", "err", err)
		// 如果加密失败，保存未加密的数据
		os.WriteFile(name, b, 0644)
		slog.Info("保存文件成功（未加密）", "file", name, "count", len(data.Results))
		return
	}

	// 保存加密后的数据
	os.WriteFile(name, encryptedData, 0644)
	slog.Info("保存加密文件成功", "file", name, "count", len(data.Results))
}

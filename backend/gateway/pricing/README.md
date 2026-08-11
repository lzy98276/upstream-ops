# Model Pricing Data

与参考项目 sub2api 对齐的模型单价数据。

## 来源

- 运行时主表：LiteLLM `model_prices_and_context_window.json`，默认
  `https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json`
- 本地缓存：`pricing.dataDir/litellm_model_prices_and_context_window.json`
- 管理端自定义源：`/gateway/price-sources`，必须返回 LiteLLM 同格式；优先级更高的源覆盖默认目录
- 内置回退：本目录 `default_prices.json`
- 硬编码回退：`pricing.go` 中 `seedFallbackPrices` / `fallbackForModel`，对齐
  `tmp/sub2api/backend/internal/service/billing_service.go`（如 `grok-4.5` 不在 LiteLLM 表中）

## 解析顺序

1. DB 价格覆盖（`model_price_overrides`）
2. 管理端自定义 LiteLLM 源（按优先级）
3. LiteLLM 官方目录与离线回退 JSON
4. 硬编码家族回退（Grok 等）

## DeepSeek

官方 `litellm_provider=deepseek` 条目（含 `deepseek/…` 前缀）与常见裸名别名
（`deepseek-v3`、`deepseek-r1`、`deepseek-v4-flash` 等）已写入本表。
无精确表项时，`pricing.go` 的 DeepSeek 家族前缀回退会落到对应主型号。

## 更新

服务启动时读取本地缓存；之后每 `pricing.hashCheckIntervalMinutes`
分钟按间隔检查 LiteLLM 和自定义源，下载成功后热更新内存价目，无需重启。
远程同步关闭或失败时继续使用缓存和内置表。

可在配置文件中调整：

```yaml
pricing:
  enabled: true
  remoteURL: https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json
  # LiteLLM 没有配套 hash 文件，留空即可
  hashURL: ""
  dataDir: ./data
  hashCheckIntervalMinutes: 10
```

手工更新内置回退表时：

```bash
# 从 LiteLLM 主表同步（仅用于更新内置离线回退）
curl -L https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json \
  -o backend/gateway/pricing/default_prices.json
```

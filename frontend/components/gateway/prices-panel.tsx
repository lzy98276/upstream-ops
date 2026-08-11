import { useCallback, useEffect, useState } from "react"
import { Loader2, Pencil, Plus, RefreshCw, Trash2 } from "lucide-react"
import { toast } from "sonner"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { apiFetch } from "@/lib/api"
import type { ModelPriceOverride, ModelPriceSource } from "@/lib/api-types"
import {
  parsePriceTiers,
  perTokenToMTok,
  type PriceFormState,
  type PriceTierFormState,
} from "./gateway-utils"

type PricesPanelProps = {
  busy: boolean
  prices: ModelPriceOverride[]
  priceDialogOpen: boolean
  editingPrice: ModelPriceOverride | null
  priceForm: PriceFormState
  setPriceForm: (form: PriceFormState) => void
  onPriceDialogOpenChange: (open: boolean) => void
  onCreatePrice: () => void
  onSavePrice: () => void
  onOpenDefaults: () => void
  onEditPrice: (p: ModelPriceOverride) => void
  onDeletePrice: (p: ModelPriceOverride) => void
}

type PriceSourceForm = {
  name: string
  url: string
  priority: string
  enabled: boolean
}

const emptyPriceSourceForm = (): PriceSourceForm => ({
  name: "",
  url: "",
  priority: "100",
  enabled: true,
})

function PriceSummary({ price }: { price: ModelPriceOverride }) {
  switch (price.billing_mode || "token") {
    case "per_request": {
      const tiers = parsePriceTiers(price.pricing_tiers_json)
      return (
        <div className="space-y-0.5 text-xs tabular-nums">
          <div>${price.per_request_price ?? 0} / 次</div>
          {tiers.length > 0 ? (
            <div className="text-muted-foreground">{tiers.length} 个分层价格</div>
          ) : null}
        </div>
      )
    }
    case "image":
      return (
        <div className="text-xs tabular-nums">
          1K ${price.image_price_1k ?? 0} · 2K ${price.image_price_2k ?? 0} · 4K ${price.image_price_4k ?? 0}
        </div>
      )
    case "video":
      return (
        <div className="text-xs tabular-nums">
          480p ${price.video_price_480p ?? 0} · 720p ${price.video_price_720p ?? 0} · 1080p ${price.video_price_1080p ?? 0}
        </div>
      )
    default:
      return (
        <div className="space-y-0.5 text-xs tabular-nums">
          <div>
            输入 ${perTokenToMTok(price.input_price_per_token)} · 输出 ${perTokenToMTok(price.output_price_per_token)} / MTok
          </div>
          <div className="text-muted-foreground">
            缓存写入 ${perTokenToMTok(price.cache_creation_price_per_token)} · 读取 ${perTokenToMTok(price.cache_read_price_per_token)}
          </div>
        </div>
      )
  }
}

function PricingTierRow({
  tier,
  index,
  onChange,
  onRemove,
}: {
  tier: PriceTierFormState
  index: number
  onChange: (tier: PriceTierFormState) => void
  onRemove: () => void
}) {
  return (
    <div className="rounded-md border bg-background p-3">
      <div className="flex items-center justify-between gap-2">
        <Badge variant="secondary">档位 {index + 1}</Badge>
        <Button
          size="icon"
          type="button"
          variant="ghost"
          className="size-7 text-destructive hover:text-destructive"
          title="删除档位"
          onClick={onRemove}
        >
          <Trash2 className="size-3.5" />
          <span className="sr-only">删除档位</span>
        </Button>
      </div>
      <div className="mt-3 grid gap-3 sm:grid-cols-2">
        <div className="space-y-1.5">
          <Label>尺寸标签（可选）</Label>
          <Input
            placeholder="例如 2K / HD"
            value={tier.tier_label}
            onChange={(e) => onChange({ ...tier, tier_label: e.target.value })}
          />
        </div>
        <div className="space-y-1.5">
          <Label>单价（$/次）</Label>
          <Input
            type="number"
            min="0"
            step="0.000001"
            value={tier.per_request_price}
            onChange={(e) => onChange({ ...tier, per_request_price: e.target.value })}
          />
        </div>
        <div className="space-y-1.5">
          <Label>最小 Token</Label>
          <Input
            type="number"
            min="0"
            step="1"
            value={tier.min_tokens}
            onChange={(e) => onChange({ ...tier, min_tokens: e.target.value })}
          />
        </div>
        <div className="space-y-1.5">
          <Label>最大 Token（留空不限）</Label>
          <Input
            type="number"
            min="0"
            step="1"
            value={tier.max_tokens}
            onChange={(e) => onChange({ ...tier, max_tokens: e.target.value })}
          />
        </div>
      </div>
    </div>
  )
}

export function PricesPanel({
  busy,
  prices,
  priceDialogOpen,
  editingPrice,
  priceForm,
  setPriceForm,
  onPriceDialogOpenChange,
  onCreatePrice,
  onSavePrice,
  onOpenDefaults,
  onEditPrice,
  onDeletePrice,
}: PricesPanelProps) {
  const billingMode = priceForm.billing_mode
  const [sources, setSources] = useState<ModelPriceSource[]>([])
  const [sourcesLoading, setSourcesLoading] = useState(false)
  const [sourceBusy, setSourceBusy] = useState(false)
  const [sourceDialogOpen, setSourceDialogOpen] = useState(false)
  const [editingSource, setEditingSource] = useState<ModelPriceSource | null>(null)
  const [sourceForm, setSourceForm] = useState<PriceSourceForm>(emptyPriceSourceForm)

  const loadSources = useCallback(async () => {
    setSourcesLoading(true)
    try {
      const res = await apiFetch<{ items: ModelPriceSource[] }>("/gateway/price-sources")
      setSources(res.items ?? [])
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "加载价格源失败")
    } finally {
      setSourcesLoading(false)
    }
  }, [])

  useEffect(() => {
    void loadSources()
  }, [loadSources])

  function openCreateSource() {
    setEditingSource(null)
    setSourceForm(emptyPriceSourceForm())
    setSourceDialogOpen(true)
  }

  function openEditSource(source: ModelPriceSource) {
    setEditingSource(source)
    setSourceForm({
      name: source.name,
      url: source.url,
      priority: String(source.priority),
      enabled: source.enabled,
    })
    setSourceDialogOpen(true)
  }

  async function saveSource() {
    const name = sourceForm.name.trim()
    const url = sourceForm.url.trim()
    const priority = Number(sourceForm.priority)
    if (!name || !url) {
      toast.error("请填写价格源名称和 URL")
      return
    }
    if (!Number.isInteger(priority)) {
      toast.error("优先级必须是整数")
      return
    }
    setSourceBusy(true)
    try {
      const body = JSON.stringify({ name, url, priority, enabled: sourceForm.enabled })
      if (editingSource) {
        await apiFetch(`/gateway/price-sources/${editingSource.id}`, { method: "PUT", body })
        toast.success("已更新价格源")
      } else {
        await apiFetch("/gateway/price-sources", { method: "POST", body })
        toast.success("已创建价格源")
      }
      setSourceDialogOpen(false)
      await loadSources()
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "保存价格源失败")
    } finally {
      setSourceBusy(false)
    }
  }

  async function syncSource(source: ModelPriceSource) {
    setSourceBusy(true)
    try {
      await apiFetch(`/gateway/price-sources/${source.id}/sync`, { method: "POST" })
      toast.success("价格源已同步")
      await loadSources()
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "同步价格源失败")
      await loadSources()
    } finally {
      setSourceBusy(false)
    }
  }

  async function deleteSource(source: ModelPriceSource) {
    setSourceBusy(true)
    try {
      await apiFetch(`/gateway/price-sources/${source.id}`, { method: "DELETE" })
      toast.success("已删除价格源")
      await loadSources()
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "删除价格源失败")
    } finally {
      setSourceBusy(false)
    }
  }

  return (
    <div className="space-y-4">
      <Card className="overflow-hidden border-border shadow-none">
        <CardContent className="space-y-4 p-4 sm:p-5">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <p className="text-sm leading-6 text-muted-foreground">
              官方默认目录来自 LiteLLM。自定义源使用相同 JSON 格式，优先级更高的源覆盖较低优先级。
            </p>
            <div className="flex shrink-0 gap-2">
              <Button
                size="icon"
                variant="outline"
                title="刷新价格源"
                onClick={() => void loadSources()}
                disabled={sourcesLoading || sourceBusy}
              >
                <RefreshCw className={sourcesLoading ? "size-4 animate-spin" : "size-4"} />
                <span className="sr-only">刷新价格源</span>
              </Button>
              <Button onClick={openCreateSource} disabled={sourceBusy}>
                <Plus className="size-4" /> 新建价格源
              </Button>
            </div>
          </div>

          <div className="overflow-x-auto rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>名称</TableHead>
                  <TableHead>地址</TableHead>
                  <TableHead>优先级</TableHead>
                  <TableHead>同步状态</TableHead>
                  <TableHead className="w-[132px] text-right">操作</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {sources.map((source) => (
                  <TableRow key={source.id}>
                    <TableCell className="font-medium">{source.name}</TableCell>
                    <TableCell className="max-w-80 truncate font-mono text-xs" title={source.url}>
                      {source.url}
                    </TableCell>
                    <TableCell className="tabular-nums">{source.priority}</TableCell>
                    <TableCell>
                      <div className="space-y-1">
                        <Badge variant={source.enabled ? "default" : "secondary"}>
                          {source.enabled ? `${source.model_count} 个模型` : "禁用"}
                        </Badge>
                        {source.last_error ? (
                          <p className="max-w-64 truncate text-xs text-destructive" title={source.last_error}>
                            {source.last_error}
                          </p>
                        ) : null}
                      </div>
                    </TableCell>
                    <TableCell className="text-right">
                      <div className="flex justify-end gap-1">
                        <Button
                          size="icon"
                          variant="ghost"
                          className="size-8"
                          title="同步价格源"
                          onClick={() => void syncSource(source)}
                          disabled={sourceBusy || !source.enabled}
                        >
                          {sourceBusy ? <Loader2 className="size-4 animate-spin" /> : <RefreshCw className="size-4" />}
                          <span className="sr-only">同步价格源</span>
                        </Button>
                        <Button
                          size="icon"
                          variant="ghost"
                          className="size-8"
                          title="编辑价格源"
                          onClick={() => openEditSource(source)}
                          disabled={sourceBusy}
                        >
                          <Pencil className="size-4" />
                          <span className="sr-only">编辑价格源</span>
                        </Button>
                        <Button
                          size="icon"
                          variant="ghost"
                          className="size-8 text-destructive hover:text-destructive"
                          title="删除价格源"
                          onClick={() => void deleteSource(source)}
                          disabled={sourceBusy}
                        >
                          <Trash2 className="size-4" />
                          <span className="sr-only">删除价格源</span>
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
                {sources.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={5} className="h-20 text-center text-muted-foreground">
                      未添加自定义价格源，当前只使用 LiteLLM 官方目录。
                    </TableCell>
                  </TableRow>
                ) : null}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>

      <Card className="overflow-hidden border-border shadow-none">
        <CardContent className="space-y-4 p-4 sm:p-5">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <p className="text-sm leading-6 text-muted-foreground">
              已启用的覆盖价优先于系统内置价。Token 按 <strong>$/MTok</strong> 输入，图片按 $/张，视频按 $/秒。
            </p>
            <div className="flex shrink-0 gap-2">
              <Button variant="outline" onClick={onOpenDefaults} disabled={busy}>
                查看系统默认价
              </Button>
              <Button onClick={onCreatePrice} disabled={busy}>
                <Plus className="size-4" /> 新建价格覆盖
              </Button>
            </div>
          </div>

          <div className="overflow-x-auto rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>模型</TableHead>
                  <TableHead>计费方式</TableHead>
                  <TableHead>价格</TableHead>
                  <TableHead>状态</TableHead>
                  <TableHead className="w-[92px] text-right">操作</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {prices.map((price) => (
                  <TableRow key={price.id}>
                    <TableCell className="max-w-72 break-all text-xs font-medium">
                      {price.model_name}
                    </TableCell>
                    <TableCell>
                      <Badge variant="outline">
                        {price.billing_mode === "per_request"
                          ? "按请求"
                          : price.billing_mode === "image"
                            ? "图片"
                            : price.billing_mode === "video"
                              ? "视频"
                              : "Token"}
                      </Badge>
                    </TableCell>
                    <TableCell className="min-w-64">
                      <PriceSummary price={price} />
                    </TableCell>
                    <TableCell>
                      <Badge variant={price.enabled ? "default" : "secondary"}>
                        {price.enabled ? "启用" : "禁用"}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-right">
                      <div className="flex justify-end gap-1">
                        <Button
                          size="icon"
                          variant="ghost"
                          className="size-8"
                          title="编辑价格覆盖"
                          onClick={() => onEditPrice(price)}
                          disabled={busy}
                        >
                          <Pencil className="size-4" />
                          <span className="sr-only">编辑价格覆盖</span>
                        </Button>
                        <Button
                          size="icon"
                          variant="ghost"
                          className="size-8 text-destructive hover:text-destructive"
                          title="删除价格覆盖"
                          onClick={() => onDeletePrice(price)}
                          disabled={busy}
                        >
                          <Trash2 className="size-4" />
                          <span className="sr-only">删除价格覆盖</span>
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
                {prices.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={5} className="h-28 text-center text-muted-foreground">
                      暂无价格覆盖，计费使用系统内置默认价
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>

      <Dialog open={priceDialogOpen} onOpenChange={onPriceDialogOpenChange}>
        <DialogContent className="flex max-h-[min(90dvh,760px)] w-[calc(100vw-1.5rem)] min-w-0 flex-col gap-3 overflow-hidden sm:max-w-2xl">
          <DialogHeader className="shrink-0">
            <DialogTitle>{editingPrice ? "编辑价格覆盖" : "新建价格覆盖"}</DialogTitle>
            <DialogDescription>
              已启用的覆盖价优先于系统内置价；未覆盖的模型继续使用默认价。
            </DialogDescription>
          </DialogHeader>

          <div className="min-h-0 flex-1 space-y-4 overflow-y-auto overscroll-contain pr-0.5">
            <div className="grid gap-3 sm:grid-cols-2">
              <div className="space-y-1.5">
                <Label>模型名</Label>
                <Input
                  placeholder="claude-sonnet-4"
                  value={priceForm.model_name}
                  onChange={(e) => setPriceForm({ ...priceForm, model_name: e.target.value })}
                />
              </div>
              <div className="space-y-1.5">
                <Label>计费方式</Label>
                <Select
                  value={billingMode}
                  onValueChange={(value) =>
                    setPriceForm({
                      ...priceForm,
                      billing_mode: value as PriceFormState["billing_mode"],
                    })
                  }
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="token">Token</SelectItem>
                    <SelectItem value="per_request">按请求</SelectItem>
                    <SelectItem value="image">图片</SelectItem>
                    <SelectItem value="video">视频</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>

            {billingMode === "token" ? (
              <div className="space-y-3 rounded-md border bg-muted/20 p-3">
                <div className="text-sm font-medium">Token 价格</div>
                <div className="grid gap-3 sm:grid-cols-2">
                  {[
                    ["input_mtok", "输入 $/MTok"],
                    ["output_mtok", "输出 $/MTok"],
                    ["cache_create_mtok", "缓存写入 $/MTok"],
                    ["cache_read_mtok", "缓存读取 $/MTok"],
                  ].map(([field, label]) => (
                    <div className="space-y-1.5" key={field}>
                      <Label>{label}</Label>
                      <Input
                        type="number"
                        min="0"
                        step="0.01"
                        value={priceForm[field as keyof Pick<PriceFormState, "input_mtok" | "output_mtok" | "cache_create_mtok" | "cache_read_mtok">]}
                        onChange={(e) => setPriceForm({ ...priceForm, [field]: e.target.value })}
                      />
                    </div>
                  ))}
                </div>
              </div>
            ) : null}

            {billingMode === "per_request" ? (
              <div className="space-y-3 rounded-md border bg-muted/20 p-3">
                <div className="space-y-1.5">
                  <Label>默认按次价格（$）</Label>
                  <Input
                    type="number"
                    min="0"
                    step="0.000001"
                    value={priceForm.per_request_price}
                    onChange={(e) => setPriceForm({ ...priceForm, per_request_price: e.target.value })}
                  />
                </div>
                <div className="border-t pt-3">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div>
                      <Label>分层价格</Label>
                      <p className="text-xs text-muted-foreground">尺寸标签优先匹配；留空时按 Token 区间匹配，最大 Token 留空表示不限。</p>
                    </div>
                    <Button
                      size="sm"
                      type="button"
                      variant="outline"
                      onClick={() =>
                        setPriceForm({
                          ...priceForm,
                          pricing_tiers: [
                            ...priceForm.pricing_tiers,
                            {
                              tier_label: "",
                              min_tokens: "0",
                              max_tokens: "",
                              per_request_price: "0",
                            },
                          ],
                        })
                      }
                    >
                      <Plus className="size-3.5" /> 新增档位
                    </Button>
                  </div>
                  {priceForm.pricing_tiers.length > 0 ? (
                    <div className="mt-3 space-y-2">
                      {priceForm.pricing_tiers.map((tier, index) => (
                        <PricingTierRow
                          key={index}
                          tier={tier}
                          index={index}
                          onChange={(next) => {
                            const tiers = [...priceForm.pricing_tiers]
                            tiers[index] = next
                            setPriceForm({ ...priceForm, pricing_tiers: tiers })
                          }}
                          onRemove={() =>
                            setPriceForm({
                              ...priceForm,
                              pricing_tiers: priceForm.pricing_tiers.filter((_, tierIndex) => tierIndex !== index),
                            })
                          }
                        />
                      ))}
                    </div>
                  ) : (
                    <div className="mt-3 rounded-md border border-dashed px-3 py-4 text-center text-xs text-muted-foreground">
                      未设置分层时，所有请求使用默认按次价格。
                    </div>
                  )}
                </div>
              </div>
            ) : null}

            {billingMode === "image" ? (
              <div className="space-y-3 rounded-md border bg-muted/20 p-3">
                <div className="text-sm font-medium">图片价格（$/张）</div>
                <div className="grid gap-3 sm:grid-cols-3">
                  {[
                    ["image_price_1k", "1K"],
                    ["image_price_2k", "2K"],
                    ["image_price_4k", "4K"],
                  ].map(([field, label]) => (
                    <div className="space-y-1.5" key={field}>
                      <Label>{label}</Label>
                      <Input
                        type="number"
                        min="0"
                        step="0.000001"
                        value={priceForm[field as keyof Pick<PriceFormState, "image_price_1k" | "image_price_2k" | "image_price_4k">]}
                        onChange={(e) => setPriceForm({ ...priceForm, [field]: e.target.value })}
                      />
                    </div>
                  ))}
                </div>
              </div>
            ) : null}

            {billingMode === "video" ? (
              <div className="space-y-3 rounded-md border bg-muted/20 p-3">
                <div className="text-sm font-medium">视频价格（$/秒）</div>
                <div className="grid gap-3 sm:grid-cols-3">
                  {[
                    ["video_price_480p", "480p"],
                    ["video_price_720p", "720p"],
                    ["video_price_1080p", "1080p"],
                  ].map(([field, label]) => (
                    <div className="space-y-1.5" key={field}>
                      <Label>{label}</Label>
                      <Input
                        type="number"
                        min="0"
                        step="0.000001"
                        value={priceForm[field as keyof Pick<PriceFormState, "video_price_480p" | "video_price_720p" | "video_price_1080p">]}
                        onChange={(e) => setPriceForm({ ...priceForm, [field]: e.target.value })}
                      />
                    </div>
                  ))}
                </div>
              </div>
            ) : null}

            <div className="flex items-center justify-between gap-3 rounded-md border bg-muted/20 p-3">
              <div>
                <Label>启用覆盖</Label>
                <p className="text-xs text-muted-foreground">关闭后保留配置，但计费回退到系统默认价。</p>
              </div>
              <Switch
                checked={priceForm.enabled}
                onCheckedChange={(enabled) => setPriceForm({ ...priceForm, enabled })}
              />
            </div>
          </div>

          <DialogFooter className="shrink-0">
            <Button variant="outline" onClick={() => onPriceDialogOpenChange(false)} disabled={busy}>
              取消
            </Button>
            <Button onClick={onSavePrice} disabled={busy}>
              保存价格覆盖
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={sourceDialogOpen} onOpenChange={setSourceDialogOpen}>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>{editingSource ? "编辑价格源" : "新建价格源"}</DialogTitle>
            <DialogDescription>
              URL 必须返回 LiteLLM `model_prices_and_context_window.json` 格式的 JSON。
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3">
            <div className="space-y-1.5">
              <Label>名称</Label>
              <Input
                value={sourceForm.name}
                onChange={(e) => setSourceForm({ ...sourceForm, name: e.target.value })}
                placeholder="例如 internal-prices"
              />
            </div>
            <div className="space-y-1.5">
              <Label>JSON URL</Label>
              <Input
                value={sourceForm.url}
                onChange={(e) => setSourceForm({ ...sourceForm, url: e.target.value })}
                placeholder="https://example.com/model_prices_and_context_window.json"
              />
            </div>
            <div className="grid gap-3 sm:grid-cols-2">
              <div className="space-y-1.5">
                <Label>优先级</Label>
                <Input
                  type="number"
                  step="1"
                  value={sourceForm.priority}
                  onChange={(e) => setSourceForm({ ...sourceForm, priority: e.target.value })}
                />
              </div>
              <div className="flex items-end gap-2 pb-2">
                <Switch
                  checked={sourceForm.enabled}
                  onCheckedChange={(enabled) => setSourceForm({ ...sourceForm, enabled })}
                />
                <Label>启用</Label>
              </div>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setSourceDialogOpen(false)} disabled={sourceBusy}>
              取消
            </Button>
            <Button onClick={() => void saveSource()} disabled={sourceBusy}>
              {sourceBusy ? <Loader2 className="size-4 animate-spin" /> : null}
              保存价格源
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}

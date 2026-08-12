import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { toast } from "sonner";
import {
  Check,
  CheckCircle2,
  ChevronsUpDown,
  ChevronDown,
  ChevronUp,
  GripVertical,
  HelpCircle,
  ListTree,
  MoreHorizontal,
  PencilLine,
  Plus,
  Play,
  RefreshCw,
  Trash2,
  XCircle,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator,
} from "@/components/ui/command";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useConfirm } from "@/components/ui/confirm-dialog";
import { apiFetch } from "@/lib/api";
import { formatRatio, relativeTime } from "@/lib/format";
import { useChannels } from "@/lib/queries";
import { cn } from "@/lib/utils";
import type {
  GatewayGroup,
  GatewayRoute,
  NewAPIAutoGroups,
  NewAPIGroup,
  RateSnapshot,
  UpstreamSyncLog,
  UpstreamSyncLogPage,
  UpstreamSyncRateConvertMode,
  UpstreamSyncGroup,
  UpstreamSyncAccount,
  UpstreamSyncTarget,
  UpstreamSyncTargetGroup,
  UpstreamSyncTargetProxy,
} from "@/lib/api-types";

interface TargetForm {
  id?: number;
  name: string;
  base_url: string;
  admin_api_key: string;
  enabled: boolean;
}

type NewAPITargetForm = TargetForm;

interface NewAPIGroupForm {
  name: string;
  ratio: string;
  description: string;
}

interface SyncGroupForm {
  id?: number;
  sync_mode: "account" | "gateway_rate";
  display_name: string;
  name_template: string;
  target_id: number;
  target_group_ids: number[];
  platform: string;
  model_limits_mode: string;
  model_limits: string;
  pool_mode_enabled: boolean;
  pool_mode_retry_count: number;
  pool_mode_retry_status_codes: string;
  custom_error_codes_enabled: boolean;
  custom_error_codes: string;
  rate_sort_direction: "asc" | "desc";
  accounts: SyncAccountForm[];
  gateway_rate_sync: SyncAccountForm | null;
  enabled: boolean;
}

interface SyncAccountForm {
  id?: number;
  source_kind: "channel" | "gateway_group";
  source_channel_id: number;
  source_group_id: string;
  source_group_name: string;
  gateway_group_id: string;
  gateway_rate_mode: "max" | "min";
  gateway_rate_min: string;
  gateway_rate_max: string;
  proxy_id: string;
  concurrency: number;
  weight: number;
  rate_convert_mode: UpstreamSyncRateConvertMode;
  rate_convert_value: string;
  enabled: boolean;
  test_enabled: boolean;
  test_model: string;
}

const emptyTargetForm: TargetForm = {
  name: "",
  base_url: "",
  admin_api_key: "",
  enabled: true,
};

const emptyNewAPITargetForm: NewAPITargetForm = {
  ...emptyTargetForm,
};

const emptyNewAPIGroupForm: NewAPIGroupForm = {
  name: "",
  ratio: "1",
  description: "",
};

const emptySyncGroupForm: SyncGroupForm = {
  sync_mode: "account",
  display_name: "",
  name_template: "sync-{同步分组ID}",
  target_id: 0,
  target_group_ids: [],
  platform: "openai",
  model_limits_mode: "sync_upstream",
  model_limits: "",
  pool_mode_enabled: false,
  pool_mode_retry_count: 10,
  pool_mode_retry_status_codes: "",
  custom_error_codes_enabled: false,
  custom_error_codes: "",
  rate_sort_direction: "asc",
  accounts: [],
  gateway_rate_sync: null,
  enabled: true,
};

const emptySyncAccountForm: SyncAccountForm = {
  source_kind: "channel",
  source_channel_id: 0,
  source_group_id: "",
  source_group_name: "",
  gateway_group_id: "",
  gateway_rate_mode: "max",
  gateway_rate_min: "0",
  gateway_rate_max: "0",
  proxy_id: "",
  concurrency: 10,
  weight: 1,
  rate_convert_mode: "raw",
  rate_convert_value: "1",
  enabled: true,
  test_enabled: false,
  test_model: "",
};

const emptyGatewayRateSyncForm: SyncAccountForm = {
  ...emptySyncAccountForm,
  source_kind: "gateway_group",
  rate_convert_mode: "multiply",
  rate_convert_value: "1",
};

const syncPlatformOptions = [
  { value: "anthropic", label: "Anthropic" },
  { value: "openai", label: "OpenAI" },
  { value: "gemini", label: "Gemini" },
  { value: "antigravity", label: "Antigravity" },
];

const modelOptions = [
  { value: "__sync_upstream__", label: "同步上游模型" },
  { value: "__custom__", label: "自定义输入" },
];

const LOG_DIALOG_PAGE_SIZE = 20;

function num(value: string) {
  return Number(value || 0);
}

function decimalNumber(value: string | number, fallback = 0) {
  const parsed = typeof value === "number" ? value : Number(value.trim());
  return Number.isFinite(parsed) ? parsed : fallback;
}

function normalizePlatform(value?: string) {
  return (value ?? "").trim().toLowerCase();
}

function platformLabel(value?: string) {
  const normalized = normalizePlatform(value);
  return (
    syncPlatformOptions.find((item) => item.value === normalized)?.label ??
    value ??
    "未分类"
  );
}

function accountToForm(account: UpstreamSyncAccount): SyncAccountForm {
  return {
    id: account.id,
    source_kind:
      account.source_kind === "gateway_group" ? "gateway_group" : "channel",
    source_channel_id: account.source_channel_id,
    source_group_id:
      account.source_group_id == null ? "" : String(account.source_group_id),
    source_group_name: account.source_group_name ?? "",
    gateway_group_id:
      account.gateway_group_id == null ? "" : String(account.gateway_group_id),
    gateway_rate_mode: account.gateway_rate_mode === "min" ? "min" : "max",
    gateway_rate_min: String(account.gateway_rate_min ?? 0),
    gateway_rate_max: String(account.gateway_rate_max ?? 0),
    proxy_id: account.proxy_id == null ? "" : String(account.proxy_id),
    concurrency: account.concurrency || 10,
    weight: account.weight || 1,
    rate_convert_mode: account.rate_convert_mode || "raw",
    rate_convert_value: String(account.rate_convert_value ?? 1),
    enabled: account.enabled,
    test_enabled: account.test_enabled ?? false,
    test_model: account.test_model ?? "",
  };
}

function syncAccountPayload(account: SyncAccountForm) {
  const isGatewayRateSync = account.source_kind === "gateway_group";
  return {
    ...account,
    source_kind: account.source_kind,
    source_channel_id: isGatewayRateSync ? 0 : account.source_channel_id,
    source_group_id:
      !isGatewayRateSync && account.source_group_id
        ? Number(account.source_group_id)
        : null,
    source_group_name: isGatewayRateSync
      ? ""
      : account.source_group_name.trim(),
    gateway_group_id:
      isGatewayRateSync && account.gateway_group_id
        ? Number(account.gateway_group_id)
        : null,
    gateway_rate_min: Math.max(0, decimalNumber(account.gateway_rate_min)),
    gateway_rate_max: Math.max(0, decimalNumber(account.gateway_rate_max)),
    rate_convert_value: decimalNumber(account.rate_convert_value, 1),
    proxy_id: isGatewayRateSync
      ? null
      : account.proxy_id
        ? Number(account.proxy_id)
        : null,
    test_enabled: isGatewayRateSync ? false : account.test_enabled,
    test_model: isGatewayRateSync ? "" : account.test_model.trim(),
  };
}

function accountRateMultiplier(
  account: SyncAccountForm,
  groups: RateSnapshot[],
) {
  if (account.rate_convert_mode === "custom") {
    return decimalNumber(account.rate_convert_value);
  }
  const sourceGroupName = account.source_group_name.trim();
  const sourceGroupID = Number(account.source_group_id || 0);
  const sourceRatio =
    (sourceGroupName
      ? groups.find((group) => group.model_name === sourceGroupName)?.ratio
      : groups.find((group) => group.remote_group_id === sourceGroupID)
          ?.ratio) ?? 1;
  switch (account.rate_convert_mode) {
    case "multiply_100":
      return sourceRatio * 100;
    case "divide_100":
      return sourceRatio / 100;
    default:
      return sourceRatio;
  }
}

function sortSyncAccountRows(
  accounts: SyncAccountForm[],
  sourceGroupsByChannel: Record<number, RateSnapshot[]>,
  direction: "asc" | "desc",
) {
  const rateDirection = direction === "desc" ? -1 : 1;
  return accounts
    .map((account, index) => ({
      account,
      index,
      rate: accountRateMultiplier(
        account,
        account.source_channel_id
          ? (sourceGroupsByChannel[account.source_channel_id] ?? [])
          : [],
      ),
    }))
    .sort((a, b) => {
      const rateDiff = (a.rate - b.rate) * rateDirection;
      if (rateDiff !== 0) return rateDiff;
      const weightDiff = b.account.weight - a.account.weight;
      return weightDiff === 0 ? a.index - b.index : weightDiff;
    });
}

function formatRate(value: number) {
  if (!Number.isFinite(value)) return "0";
  return Number(value.toFixed(8)).toString();
}

function sourceGroupOptionValue(group: RateSnapshot) {
  return group.remote_group_id == null
    ? `name:${group.model_name}`
    : `id:${group.remote_group_id}`;
}

function sourceGroupSelectValue(account: SyncAccountForm) {
  // 有 remote id 时优先 id 匹配选项（sub2api）；名称仅用于展示
  if (account.source_group_id) return `id:${account.source_group_id}`;
  if (account.source_group_name.trim()) {
    return `name:${account.source_group_name.trim()}`;
  }
  return "none";
}

function normalizeModelInput(value: string) {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)
    .filter((item, index, list) => list.indexOf(item) === index)
    .join(",");
}

function splitModelInput(value: string) {
  const normalized = normalizeModelInput(value);
  return normalized ? normalized.split(",") : [];
}

function uniqueModels(list: string[]) {
  return list
    .map((item) => item.trim())
    .filter(Boolean)
    .filter((item, index, all) => all.indexOf(item) === index);
}

function TestModelPicker({
  enabled,
  value,
  models,
  loading,
  onChange,
}: {
  enabled: boolean;
  value: string;
  models: string[];
  loading?: boolean;
  onChange: (enabled: boolean, model: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const selectedModel = value.trim();
  const customModel = query.trim();
  const modelList = uniqueModels([...models, selectedModel]);
  const hasCustomModel =
    customModel &&
    !modelList.some(
      (model) => model.toLowerCase() === customModel.toLowerCase(),
    );
  const label = !enabled ? "不测试" : selectedModel || "自动选择";

  useEffect(() => {
    if (!open) setQuery("");
  }, [open]);

  function select(enabled: boolean, model: string) {
    onChange(enabled, model);
    setOpen(false);
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          type="button"
          variant="outline"
          role="combobox"
          aria-expanded={open}
          className="h-9 w-56 justify-between"
        >
          <span className="min-w-0 truncate">{label}</span>
          <ChevronsUpDown className="ml-2 size-3.5 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-64 p-0" align="start">
        <Command>
          <CommandInput
            value={query}
            onValueChange={setQuery}
            placeholder="搜索或输入模型"
          />
          <CommandList>
            <CommandEmpty>没有匹配模型</CommandEmpty>
            <CommandGroup>
              <CommandItem value="不测试" onSelect={() => select(false, "")}>
                <Check
                  className={cn(
                    "size-4",
                    !enabled ? "opacity-100" : "opacity-0",
                  )}
                />
                不测试
              </CommandItem>
              <CommandItem value="自动选择" onSelect={() => select(true, "")}>
                <Check
                  className={cn(
                    "size-4",
                    enabled && !selectedModel ? "opacity-100" : "opacity-0",
                  )}
                />
                自动选择
              </CommandItem>
            </CommandGroup>
            <CommandSeparator />
            <CommandGroup>
              {loading ? (
                <CommandItem value="模型加载中" disabled>
                  模型加载中
                </CommandItem>
              ) : null}
              {!loading && modelList.length === 0 ? (
                <CommandItem value="暂无可选模型" disabled>
                  暂无可选模型
                </CommandItem>
              ) : null}
              {modelList.map((model) => (
                <CommandItem
                  key={model}
                  value={model}
                  onSelect={() => select(true, model)}
                >
                  <Check
                    className={cn(
                      "size-4",
                      enabled && selectedModel === model
                        ? "opacity-100"
                        : "opacity-0",
                    )}
                  />
                  <span className="truncate">{model}</span>
                </CommandItem>
              ))}
              {hasCustomModel ? (
                <CommandItem
                  value={`自定义 ${customModel}`}
                  onSelect={() => select(true, customModel)}
                >
                  <Check className="size-4 opacity-0" />
                  <span className="min-w-0 truncate">
                    使用自定义：{customModel}
                  </span>
                </CommandItem>
              ) : null}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}

export function UpstreamSyncSettings() {
  const channels = useChannels();
  const { confirm, dialog } = useConfirm();
  const [targets, setTargets] = useState<UpstreamSyncTarget[]>([]);
  const [syncGroupList, setSyncGroupList] = useState<UpstreamSyncGroup[]>([]);
  const [targetGroups, setTargetGroups] = useState<UpstreamSyncTargetGroup[]>(
    [],
  );
  const [targetProxies, setTargetProxies] = useState<UpstreamSyncTargetProxy[]>(
    [],
  );
  const [sourceGroupsByChannel, setSourceGroupsByChannel] = useState<
    Record<number, RateSnapshot[]>
  >({});
  const [logs, setLogs] = useState<UpstreamSyncLog[]>([]);
  const [targetForm, setTargetForm] = useState<TargetForm>(emptyTargetForm);
  const [newAPITargetForm, setNewAPITargetForm] = useState<NewAPITargetForm>(
    emptyNewAPITargetForm,
  );
  const [newAPITargetDialogOpen, setNewAPITargetDialogOpen] = useState(false);
  const [autoGroupsDialogTarget, setAutoGroupsDialogTarget] =
    useState<UpstreamSyncTarget | null>(null);
  const [autoGroups, setAutoGroups] = useState<string[]>([]);
  const [autoGroupDragOver, setAutoGroupDragOver] = useState<string | null>(
    null,
  );
  const [availableAutoGroups, setAvailableAutoGroups] = useState<string[]>([]);
  const [newAutoGroup, setNewAutoGroup] = useState("");
  const [autoGroupsLoading, setAutoGroupsLoading] = useState(false);
  const [newAPIGroupsDialogTarget, setNewAPIGroupsDialogTarget] =
    useState<UpstreamSyncTarget | null>(null);
  const [newAPIGroups, setNewAPIGroups] = useState<NewAPIGroup[]>([]);
  const [newAPIGroupsLoading, setNewAPIGroupsLoading] = useState(false);
  const [newAPIGroupForm, setNewAPIGroupForm] =
    useState<NewAPIGroupForm>(emptyNewAPIGroupForm);
  const [editingNewAPIGroupName, setEditingNewAPIGroupName] = useState<
    string | null
  >(null);
  const [syncGroupForm, setSyncGroupForm] =
    useState<SyncGroupForm>(emptySyncGroupForm);
  const [selectedTargetID, setSelectedTargetID] = useState<number | null>(null);
  const [targetDialogOpen, setTargetDialogOpen] = useState(false);
  const [syncGroupDialogOpen, setSyncGroupDialogOpen] = useState(false);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState<string | null>(null);
  const [logSyncGroupID, setLogSyncGroupID] = useState<number | null>(null);
  const [logPage, setLogPage] = useState(1);
  const [logMeta, setLogMeta] = useState({ total: 0, pages: 1 });
  const [logLoading, setLogLoading] = useState(false);
  const [logError, setLogError] = useState<string | null>(null);

  useEffect(() => {
    void loadBase();
  }, []);

  useEffect(() => {
    if (!syncGroupForm.target_id) {
      setTargetGroups([]);
      setTargetProxies([]);
      return;
    }
    void loadTargetGroups(syncGroupForm.target_id);
    void loadTargetProxies(syncGroupForm.target_id);
  }, [syncGroupForm.target_id]);

  useEffect(() => {
    if (!logSyncGroupID) return;
    let cancelled = false;
    setLogLoading(true);
    setLogError(null);
    apiFetch<UpstreamSyncLogPage>(
      `/upstream-sync/sync-groups/${logSyncGroupID}/logs?page=${logPage}&page_size=${LOG_DIALOG_PAGE_SIZE}`,
    )
      .then((res) => {
        if (cancelled) return;
        const next = Array.isArray(res?.items) ? res.items : [];
        setLogs((prev) => (logPage === 1 ? next : [...prev, ...next]));
        setLogMeta({
          total: res?.total ?? 0,
          pages: Math.max(1, res?.pages ?? 1),
        });
      })
      .catch((err) => {
        if (cancelled) return;
        setLogError(err instanceof Error ? err.message : "加载日志失败");
      })
      .finally(() => {
        if (!cancelled) setLogLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [logSyncGroupID, logPage]);

  const targetSyncGroups = useMemo(
    () =>
      syncGroupList.filter(
        (syncGroup) => syncGroup.target_id === selectedTargetID,
      ),
    [syncGroupList, selectedTargetID],
  );

  const sub2APITargets = useMemo(
    () => targets.filter((target) => target.target_type !== "newapi"),
    [targets],
  );

  const newAPITargets = useMemo(
    () => targets.filter((target) => target.target_type === "newapi"),
    [targets],
  );

  const activeLogSyncGroup = useMemo(
    () =>
      syncGroupList.find((syncGroup) => syncGroup.id === logSyncGroupID) ??
      null,
    [syncGroupList, logSyncGroupID],
  );

  async function loadBase() {
    setLoading(true);
    try {
      const [targetList, syncGroups] = await Promise.all([
        apiFetch<UpstreamSyncTarget[]>("/upstream-sync/targets"),
        apiFetch<UpstreamSyncGroup[]>("/upstream-sync/sync-groups"),
      ]);
      setTargets(targetList);
      setSyncGroupList(syncGroups);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "加载上游动态同步失败");
    } finally {
      setLoading(false);
    }
  }

  async function loadTargetGroups(targetID: number) {
    try {
      const list = await apiFetch<UpstreamSyncTargetGroup[]>(
        `/upstream-sync/targets/${targetID}/groups?include_missing=1`,
      );
      setTargetGroups(list);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "加载目标分组失败");
    }
  }

  async function loadTargetProxies(targetID: number) {
    try {
      const list = await apiFetch<UpstreamSyncTargetProxy[]>(
        `/upstream-sync/targets/${targetID}/proxies`,
      );
      setTargetProxies(list);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "加载目标代理失败");
    }
  }

  async function loadSourceGroups(channelID: number) {
    if (!channelID) return;
    try {
      const list = await apiFetch<RateSnapshot[]>(
        `/channels/${channelID}/rates`,
      );
      setSourceGroupsByChannel((prev) => ({ ...prev, [channelID]: list }));
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "加载源分组失败");
    }
  }

  function openGroupManagement(target: UpstreamSyncTarget) {
    if (selectedTargetID === target.id) {
      setSelectedTargetID(null);
      setLogs([]);
      setLogSyncGroupID(null);
      return;
    }
    setSelectedTargetID(target.id);
    setLogs([]);
    setLogSyncGroupID(null);
    void loadTargetGroups(target.id);
    void loadTargetProxies(target.id);
  }

  function openNewSyncGroupDialog(targetID = selectedTargetID) {
    if (!targetID) return;
    setSelectedTargetID(targetID);
    setSyncGroupForm({
      ...emptySyncGroupForm,
      target_id: targetID,
      accounts: [{ ...emptySyncAccountForm }],
    });
    setSyncGroupDialogOpen(true);
    void loadTargetGroups(targetID);
    void loadTargetProxies(targetID);
  }

  function openEditSyncGroupDialog(syncGroup: UpstreamSyncGroup) {
    const form = syncGroupToForm(syncGroup);
    setSyncGroupForm(form);
    setSyncGroupDialogOpen(true);
    void loadTargetGroups(syncGroup.target_id);
    void loadTargetProxies(syncGroup.target_id);
    form.accounts.forEach((account) => {
      if (account.source_channel_id)
        void loadSourceGroups(account.source_channel_id);
    });
  }

  function openTargetDialog(target?: UpstreamSyncTarget) {
    setTargetForm(
      target
        ? {
            id: target.id,
            name: target.name,
            base_url: target.base_url,
            admin_api_key: "",
            enabled: target.enabled,
          }
        : emptyTargetForm,
    );
    setTargetDialogOpen(true);
  }

  function openNewAPITargetDialog(target?: UpstreamSyncTarget) {
    setNewAPITargetForm(
      target
        ? {
            id: target.id,
            name: target.name,
            base_url: target.base_url,
            admin_api_key: "",
            enabled: target.enabled,
          }
        : emptyNewAPITargetForm,
    );
    setNewAPITargetDialogOpen(true);
  }

  async function saveNewAPITarget() {
    setBusy("newapi-target");
    try {
      const path = newAPITargetForm.id
        ? `/upstream-sync/targets/${newAPITargetForm.id}`
        : "/upstream-sync/targets";
      await apiFetch(path, {
        method: newAPITargetForm.id ? "PUT" : "POST",
        body: JSON.stringify({ ...newAPITargetForm, target_type: "newapi" }),
      });
      setNewAPITargetForm(emptyNewAPITargetForm);
      setNewAPITargetDialogOpen(false);
      await loadBase();
      toast.success("New API 上游已保存");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "保存 New API 上游失败");
    } finally {
      setBusy(null);
    }
  }

  const openAutoGroupsDialog = useCallback(
    async (target: UpstreamSyncTarget) => {
      setAutoGroupsDialogTarget(target);
      setAutoGroups([]);
      setAutoGroupDragOver(null);
      setAvailableAutoGroups([]);
      setNewAutoGroup("");
      setAutoGroupsLoading(true);
      try {
        const result = await apiFetch<NewAPIAutoGroups>(
          `/upstream-sync/targets/${target.id}/newapi/auto-groups`,
        );
        setAutoGroups(result.groups ?? []);
        setAvailableAutoGroups(result.available_groups ?? []);
      } catch (err) {
        setAutoGroupsDialogTarget(null);
        toast.error(err instanceof Error ? err.message : "加载自动分组失败");
      } finally {
        setAutoGroupsLoading(false);
      }
    },
    [],
  );

  const openNewAPIGroupsDialog = useCallback(
    async (target: UpstreamSyncTarget, announceSync = false) => {
      setNewAPIGroupsDialogTarget(target);
      setNewAPIGroups([]);
      setNewAPIGroupForm(emptyNewAPIGroupForm);
      setEditingNewAPIGroupName(null);
      setNewAPIGroupsLoading(true);
      try {
        const result = await apiFetch<NewAPIGroup[]>(
          `/upstream-sync/targets/${target.id}/newapi/groups`,
        );
        setNewAPIGroups(result ?? []);
        if (announceSync) {
          toast.success(`${target.name} 分组已同步`);
        }
      } catch (err) {
        setNewAPIGroupsDialogTarget(null);
        toast.error(err instanceof Error ? err.message : "加载 New API 分组失败");
      } finally {
        setNewAPIGroupsLoading(false);
      }
    },
    [],
  );

  function editNewAPIGroup(group: NewAPIGroup) {
    setEditingNewAPIGroupName(group.name);
    setNewAPIGroupForm({
      name: group.name,
      ratio: String(group.ratio),
      description: group.description ?? "",
    });
  }

  function resetNewAPIGroupForm() {
    setEditingNewAPIGroupName(null);
    setNewAPIGroupForm(emptyNewAPIGroupForm);
  }

  async function saveNewAPIGroup() {
    if (!newAPIGroupsDialogTarget) return;
    const name = newAPIGroupForm.name.trim();
    const ratio = decimalNumber(newAPIGroupForm.ratio, Number.NaN);
    if (!name) {
      toast.error("请填写分组名称");
      return;
    }
    if (!Number.isFinite(ratio) || ratio < 0) {
      toast.error("倍率必须是大于或等于 0 的数字");
      return;
    }
    const originalName = editingNewAPIGroupName;
    if (originalName && originalName !== name) {
      toast.error("New API 不支持重命名分组，请新建后删除旧分组");
      return;
    }
    setBusy(`newapi-group-${newAPIGroupsDialogTarget.id}`);
    try {
      const path = originalName
        ? `/upstream-sync/targets/${newAPIGroupsDialogTarget.id}/newapi/groups/${encodeURIComponent(originalName)}`
        : `/upstream-sync/targets/${newAPIGroupsDialogTarget.id}/newapi/groups`;
      const saved = await apiFetch<NewAPIGroup>(path, {
        method: originalName ? "PUT" : "POST",
        body: JSON.stringify({
          name,
          ratio,
          description: newAPIGroupForm.description.trim(),
        }),
      });
      setNewAPIGroups((groups) => {
        const next = groups.filter((group) => group.name !== saved.name);
        next.push(saved);
        return next.sort((left, right) => left.name.localeCompare(right.name));
      });
      resetNewAPIGroupForm();
      toast.success(originalName ? "New API 分组已更新" : "New API 分组已创建");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "保存 New API 分组失败");
    } finally {
      setBusy(null);
    }
  }

  async function deleteNewAPIGroup(group: NewAPIGroup) {
    if (!newAPIGroupsDialogTarget) return;
    const ok = await confirm({
      title: `删除 New API 分组 ${group.name}？`,
      description: "将删除该分组的倍率和说明，并从自动分组顺序中移除。",
      confirmLabel: "删除分组",
      destructive: true,
    });
    if (!ok) return;
    setBusy(`newapi-group-${newAPIGroupsDialogTarget.id}`);
    try {
      await apiFetch(
        `/upstream-sync/targets/${newAPIGroupsDialogTarget.id}/newapi/groups/${encodeURIComponent(group.name)}`,
        { method: "DELETE" },
      );
      setNewAPIGroups((groups) =>
        groups.filter((item) => item.name !== group.name),
      );
      if (editingNewAPIGroupName === group.name) resetNewAPIGroupForm();
      toast.success("New API 分组已删除");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "删除 New API 分组失败");
    } finally {
      setBusy(null);
    }
  }

  async function saveAutoGroups() {
    if (!autoGroupsDialogTarget) return;
    setBusy(`auto-groups-${autoGroupsDialogTarget.id}`);
    try {
      const result = await apiFetch<NewAPIAutoGroups>(
        `/upstream-sync/targets/${autoGroupsDialogTarget.id}/newapi/auto-groups`,
        { method: "PUT", body: JSON.stringify({ groups: autoGroups }) },
      );
      setAutoGroups(result.groups ?? []);
      await loadBase();
      toast.success("自动分组顺序已保存");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "保存自动分组失败");
    } finally {
      setBusy(null);
    }
  }

  function addAutoGroup() {
    const name = newAutoGroup.trim();
    if (!name) return;
    if (autoGroups.includes(name)) {
      toast.error("该分组已在自动分组列表中");
      return;
    }
    setAutoGroups((groups) => [...groups, name]);
    setNewAutoGroup("");
  }

  function moveAutoGroup(index: number, direction: -1 | 1) {
    setAutoGroups((groups) => {
      const nextIndex = index + direction;
      if (nextIndex < 0 || nextIndex >= groups.length) return groups;
      const next = [...groups];
      [next[index], next[nextIndex]] = [next[nextIndex], next[index]];
      return next;
    });
  }

  function moveAutoGroupByName(fromGroup: string, toGroup: string) {
    if (fromGroup === toGroup) return;
    setAutoGroups((groups) => {
      const fromIndex = groups.indexOf(fromGroup);
      const toIndex = groups.indexOf(toGroup);
      if (fromIndex < 0 || toIndex < 0) return groups;
      const next = [...groups];
      const [item] = next.splice(fromIndex, 1);
      next.splice(toIndex, 0, item);
      return next;
    });
  }

  function toggleAvailableAutoGroup(group: string, checked: boolean) {
    setAutoGroups((groups) => {
      if (checked) return groups.includes(group) ? groups : [...groups, group];
      return groups.filter((item) => item !== group);
    });
  }

  async function saveTarget() {
    setBusy("target");
    try {
      const path = targetForm.id
        ? `/upstream-sync/targets/${targetForm.id}`
        : "/upstream-sync/targets";
      const method = targetForm.id ? "PUT" : "POST";
      await apiFetch(path, {
        method,
        body: JSON.stringify(targetForm),
      });
      setTargetForm(emptyTargetForm);
      setTargetDialogOpen(false);
      await loadBase();
      toast.success("Sub2API 上游已保存");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "保存 Sub2API 上游失败");
    } finally {
      setBusy(null);
    }
  }

  async function checkTarget(target: UpstreamSyncTarget) {
    setBusy(`check-${target.id}`);
    try {
      await apiFetch(`/upstream-sync/targets/${target.id}/check`, {
        method: "POST",
      });
      await loadBase();
      toast.success(`${target.name} 检测通过`);
    } catch (err) {
      await loadBase();
      toast.error(err instanceof Error ? err.message : "检测失败");
    } finally {
      setBusy(null);
    }
  }

  async function syncTargetGroups(target: UpstreamSyncTarget) {
    setBusy(`groups-${target.id}`);
    try {
      await apiFetch(`/upstream-sync/targets/${target.id}/groups/sync`, {
        method: "POST",
      });
      if (
        syncGroupForm.target_id === target.id ||
        selectedTargetID === target.id
      ) {
        await loadTargetGroups(target.id);
        await loadTargetProxies(target.id);
      }
      toast.success(`${target.name} 分组已同步`);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "同步分组失败");
    } finally {
      setBusy(null);
    }
  }

  async function reorderTargetGroups(
    target: UpstreamSyncTarget,
    orderedGroups: UpstreamSyncTargetGroup[],
  ) {
    const orderedIDs = orderedGroups.map((group) => group.remote_group_id);
    setBusy(`group-order-${target.id}`);
    try {
      const result = await apiFetch<UpstreamSyncTargetGroup[]>(
        `/upstream-sync/targets/${target.id}/groups/sort-order`,
        {
          method: "PUT",
          body: JSON.stringify({ ordered_ids: orderedIDs }),
        },
      );
      setTargetGroups(result);
      toast.success(`${target.name} 上游分组排序已更新`);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "更新上游分组排序失败");
      await loadTargetGroups(target.id);
    } finally {
      setBusy(null);
    }
  }

  async function refreshSyncGroups(target: UpstreamSyncTarget) {
    setBusy(`sync-groups-refresh-${target.id}`);
    try {
      const syncGroups = await apiFetch<UpstreamSyncGroup[]>(
        "/upstream-sync/sync-groups",
      );
      setSyncGroupList(syncGroups);
      toast.success(`${target.name} 同步分组列表已刷新`);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "刷新同步分组列表失败");
    } finally {
      setBusy(null);
    }
  }

  async function deleteTarget(target: UpstreamSyncTarget) {
    const targetLabel = target.target_type === "newapi" ? "New API" : "Sub2API";
    const ok = await confirm({
      title: `删除 ${targetLabel} 上游 ${target.name}？`,
      description:
        target.target_type === "newapi"
          ? "只删除本地连接配置，不会修改远端 New API 设置。"
          : "会同时删除该上游下的本地同步分组、分组缓存和执行日志。",
      confirmLabel: "删除",
      destructive: true,
    });
    if (!ok) return;
    setBusy(`delete-target-${target.id}`);
    try {
      await apiFetch(`/upstream-sync/targets/${target.id}`, {
        method: "DELETE",
      });
      if (selectedTargetID === target.id) {
        setSelectedTargetID(null);
      }
      await loadBase();
      toast.success(`${targetLabel} 上游已删除`);
    } catch (err) {
      toast.error(
        err instanceof Error ? err.message : `删除 ${targetLabel} 上游失败`,
      );
    } finally {
      setBusy(null);
    }
  }

  function syncGroupToForm(syncGroup: UpstreamSyncGroup): SyncGroupForm {
    const allAccounts = syncGroup.accounts?.map(accountToForm) ?? [];
    const gatewayRateSync =
      allAccounts.find((account) => account.source_kind === "gateway_group") ??
      null;
    return {
      id: syncGroup.id,
      sync_mode: gatewayRateSync ? "gateway_rate" : "account",
      display_name: syncGroup.display_name || syncGroup.name,
      name_template: syncGroup.name_template,
      target_id: syncGroup.target_id,
      target_group_ids: syncGroup.target_group_ids ?? [],
      platform: normalizePlatform(syncGroup.platform),
      model_limits_mode:
        syncGroup.model_limits_mode === "sync_upstream"
          ? "sync_upstream"
          : "custom",
      model_limits: syncGroup.model_limits ?? "",
      pool_mode_enabled: syncGroup.pool_mode_enabled,
      pool_mode_retry_count: syncGroup.pool_mode_retry_count,
      pool_mode_retry_status_codes:
        syncGroup.pool_mode_retry_status_codes ?? "",
      custom_error_codes_enabled: syncGroup.custom_error_codes_enabled,
      custom_error_codes: syncGroup.custom_error_codes ?? "",
      rate_sort_direction: syncGroup.rate_sort_direction || "asc",
      accounts:
        allAccounts.filter((account) => account.source_kind !== "gateway_group")
          .length > 0
          ? allAccounts.filter(
              (account) => account.source_kind !== "gateway_group",
            )
          : [{ ...emptySyncAccountForm }],
      gateway_rate_sync: gatewayRateSync,
      enabled: syncGroup.enabled ?? true,
    };
  }

  function buildSyncGroupPayload(
    groupsByChannel: Record<number, RateSnapshot[]> = sourceGroupsByChannel,
  ) {
    const configuredAccounts = syncGroupForm.accounts.filter(
      (account) => account.source_channel_id > 0,
    );
    const sortedAccounts = sortSyncAccountRows(
      configuredAccounts,
      groupsByChannel,
      syncGroupForm.rate_sort_direction,
    );
    const accounts =
      syncGroupForm.sync_mode === "gateway_rate"
        ? syncGroupForm.gateway_rate_sync
          ? [syncAccountPayload(syncGroupForm.gateway_rate_sync)]
          : []
        : sortedAccounts.map(({ account }) => syncAccountPayload(account));
    return {
      ...syncGroupForm,
      target_id: selectedTargetID ?? syncGroupForm.target_id,
      model_limits:
        syncGroupForm.model_limits_mode === "custom"
          ? normalizeModelInput(syncGroupForm.model_limits)
          : "",
      accounts,
    };
  }

  async function saveSyncGroup() {
    const targetID = selectedTargetID ?? syncGroupForm.target_id;
    setBusy("sync-group");
    try {
      if (
        syncGroupForm.sync_mode === "gateway_rate" &&
        !syncGroupForm.gateway_rate_sync
      ) {
        toast.error("倍率同步未配置网关组");
        return;
      }
      if (
        syncGroupForm.sync_mode === "gateway_rate" &&
        syncGroupForm.gateway_rate_sync &&
        !syncGroupForm.gateway_rate_sync.gateway_group_id
      ) {
        toast.error("倍率同步未选择网关");
        return;
      }
      if (
        syncGroupForm.sync_mode === "gateway_rate" &&
        syncGroupForm.gateway_rate_sync &&
        decimalNumber(syncGroupForm.gateway_rate_sync.gateway_rate_min) > 0 &&
        decimalNumber(syncGroupForm.gateway_rate_sync.gateway_rate_max) > 0 &&
        decimalNumber(syncGroupForm.gateway_rate_sync.gateway_rate_min) >
          decimalNumber(syncGroupForm.gateway_rate_sync.gateway_rate_max)
      ) {
        toast.error("最低调整倍率不能大于最高调整倍率");
        return;
      }
      const path = syncGroupForm.id
        ? `/upstream-sync/sync-groups/${syncGroupForm.id}`
        : "/upstream-sync/sync-groups";
      const method = syncGroupForm.id ? "PUT" : "POST";
      await apiFetch(path, {
        method,
        body: JSON.stringify(buildSyncGroupPayload()),
      });
      setSyncGroupForm({ ...emptySyncGroupForm, target_id: targetID });
      setSyncGroupDialogOpen(false);
      await loadBase();
      if (targetID) await loadTargetGroups(targetID);
      toast.success("同步分组已保存");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "保存同步分组失败");
    } finally {
      setBusy(null);
    }
  }

  async function applySyncGroup(syncGroup: UpstreamSyncGroup) {
    setBusy(`apply-${syncGroup.id}`);
    try {
      await apiFetch(`/upstream-sync/sync-groups/${syncGroup.id}/apply`, {
        method: "POST",
      });
      await loadBase();
      toast.success(`${syncGroup.name} 已应用`);
    } catch (err) {
      await loadBase();
      toast.error(err instanceof Error ? err.message : "应用失败");
    } finally {
      setBusy(null);
    }
  }

  async function deleteManaged(syncGroup: UpstreamSyncGroup) {
    const ok = await confirm({
      title: `删除 ${syncGroup.name} 的托管对象？`,
      description:
        "会删除目标 Sub2API 账号和源渠道 API Key，不会删除目标分组。",
      confirmLabel: "删除托管对象",
      destructive: true,
    });
    if (!ok) return;
    setBusy(`managed-${syncGroup.id}`);
    try {
      await apiFetch(
        `/upstream-sync/sync-groups/${syncGroup.id}/delete-managed`,
        {
          method: "POST",
        },
      );
      await loadBase();
      toast.success("托管对象已删除");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "删除托管对象失败");
    } finally {
      setBusy(null);
    }
  }

  async function deleteSyncGroup(syncGroup: UpstreamSyncGroup) {
    const ok = await confirm({
      title: `删除同步分组 ${syncGroup.name}？`,
      description: "只删除本地同步分组，不会自动删除远端托管对象。",
      confirmLabel: "删除同步分组",
      destructive: true,
    });
    if (!ok) return;
    setBusy(`sync-group-delete-${syncGroup.id}`);
    try {
      await apiFetch(`/upstream-sync/sync-groups/${syncGroup.id}`, {
        method: "DELETE",
      });
      await loadBase();
      toast.success("同步分组已删除");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "删除同步分组失败");
    } finally {
      setBusy(null);
    }
  }

  function loadLogs(syncGroup: UpstreamSyncGroup) {
    setLogs([]);
    setLogMeta({ total: 0, pages: 1 });
    setLogError(null);
    setLogPage(1);
    setLogSyncGroupID(syncGroup.id);
  }

  function loadMoreLogs() {
    if (logLoading || logPage >= logMeta.pages) return;
    setLogPage((prev) => prev + 1);
  }

  function toggleTargetGroup(id: number, checked: boolean) {
    setSyncGroupForm((prev) => ({
      ...prev,
      target_group_ids: checked
        ? [...prev.target_group_ids, id]
        : prev.target_group_ids.filter((item) => item !== id),
    }));
  }

  if (loading) {
    return (
      <p className="text-sm text-muted-foreground">上游动态同步加载中...</p>
    );
  }

  return (
    <div className="space-y-5">
      <Panel
        title="Sub2API 上游列表"
        description="主列表只展示可写入的 Sub2API 上游；点击分组管理后在对应卡片内维护同步分组。"
        action={
          <Button
            size="sm"
            variant="outline"
            onClick={() => openTargetDialog()}
          >
            <Plus className="size-3.5" />
            新增上游
          </Button>
        }
      >
        {sub2APITargets.length === 0 ? (
          <EmptyBox text="还没有 Sub2API 上游配置。" />
        ) : (
          <div className="space-y-3">
            {sub2APITargets.map((target) => {
              const syncGroupCount = syncGroupList.filter(
                (syncGroup) => syncGroup.target_id === target.id,
              ).length;
              const isGroupOpen = selectedTargetID === target.id;
              return (
                <div
                  key={target.id}
                  className="rounded-2xl border border-border bg-background/80 p-4"
                >
                  <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
                    <div className="min-w-0 space-y-1">
                      <div className="flex flex-wrap items-center gap-2">
                        <p className="truncate text-sm font-semibold text-foreground">
                          {target.name}
                        </p>
                        <StatusBadge
                          status={target.enabled ? "enabled" : "disabled"}
                        />
                        {target.last_check_status ? (
                          <StatusBadge status={target.last_check_status} />
                        ) : null}
                        <Badge
                          variant="outline"
                          className="border-border bg-muted/40"
                        >
                          {syncGroupCount} 个同步分组
                        </Badge>
                      </div>
                      <p className="break-all text-xs text-muted-foreground">
                        {target.base_url}
                      </p>
                      <p
                        className={cn(
                          "text-xs",
                          target.last_check_error
                            ? "text-destructive"
                            : "text-muted-foreground",
                        )}
                      >
                        {target.last_check_error
                          ? target.last_check_error
                          : target.last_check_at
                            ? `上次检测 ${relativeTime(target.last_check_at)}`
                            : "尚未检测"}
                      </p>
                    </div>
                    <div className="flex w-full items-center gap-2 xl:w-auto xl:justify-end">
                      <Button
                        size="sm"
                        variant={isGroupOpen ? "default" : "outline"}
                        className="h-8 flex-1 px-3 xl:flex-none"
                        onClick={() => openGroupManagement(target)}
                        aria-expanded={isGroupOpen}
                      >
                        <ListTree className="size-3.5" />
                        {isGroupOpen ? "收起分组" : "分组管理"}
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        className="h-8 px-3"
                        onClick={() => syncTargetGroups(target)}
                        disabled={busy === `groups-${target.id}`}
                      >
                        <RefreshCw className="size-3.5" />
                        同步分组
                      </Button>
                      <DropdownMenu>
                        <DropdownMenuTrigger asChild>
                          <Button
                            size="icon-sm"
                            variant="outline"
                            aria-label="上游操作"
                          >
                            <MoreHorizontal className="size-4" />
                          </Button>
                        </DropdownMenuTrigger>
                        <DropdownMenuContent align="end" className="w-44">
                          <DropdownMenuItem
                            onSelect={() => checkTarget(target)}
                            disabled={busy === `check-${target.id}`}
                          >
                            <CheckCircle2 className="size-4" />
                            检测连接
                          </DropdownMenuItem>
                          <DropdownMenuItem
                            onSelect={() => openTargetDialog(target)}
                          >
                            <PencilLine className="size-4" />
                            编辑上游
                          </DropdownMenuItem>
                          <DropdownMenuSeparator />
                          <DropdownMenuItem
                            variant="destructive"
                            onSelect={() => deleteTarget(target)}
                            disabled={busy === `delete-target-${target.id}`}
                          >
                            <Trash2 className="size-4" />
                            删除上游
                          </DropdownMenuItem>
                        </DropdownMenuContent>
                      </DropdownMenu>
                    </div>
                  </div>
                  {isGroupOpen ? (
                    <div className="mt-4 border-t border-border pt-4">
                      <TargetGroupOrderList
                        target={target}
                        groups={targetGroups}
                        busy={busy === `group-order-${target.id}`}
                        onReorder={(groups) =>
                          void reorderTargetGroups(target, groups)
                        }
                      />
                      <SyncGroupList
                        syncGroups={targetSyncGroups}
                        busy={busy}
                        onAdd={() => openNewSyncGroupDialog(target.id)}
                        onRefresh={() => refreshSyncGroups(target)}
                        refreshBusy={
                          busy === `sync-groups-refresh-${target.id}`
                        }
                        onApply={applySyncGroup}
                        onLogs={loadLogs}
                        onEdit={openEditSyncGroupDialog}
                        onDeleteManaged={deleteManaged}
                        onDelete={deleteSyncGroup}
                      />
                    </div>
                  ) : null}
                </div>
              );
            })}
          </div>
        )}
      </Panel>

      <Panel
        title="New API 上游列表"
        description="管理 New API 管理员设置中的分组、倍率与自动分组顺序；该列表不会进入 Sub2API 的账号托管同步流程。"
        action={
          <Button
            size="sm"
            variant="outline"
            onClick={() => openNewAPITargetDialog()}
          >
            <Plus className="size-3.5" />
            新增 New API
          </Button>
        }
      >
        {newAPITargets.length === 0 ? (
          <EmptyBox text="还没有 New API 上游配置。" />
        ) : (
          <div className="space-y-3">
            {newAPITargets.map((target) => (
              <div
                key={target.id}
                className="rounded-2xl border border-border bg-background/80 p-4"
              >
                <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
                  <div className="min-w-0 space-y-1">
                    <div className="flex flex-wrap items-center gap-2">
                      <p className="truncate text-sm font-semibold text-foreground">
                        {target.name}
                      </p>
                      <Badge variant="outline">New API</Badge>
                      <StatusBadge
                        status={target.enabled ? "enabled" : "disabled"}
                      />
                      {target.last_check_status ? (
                        <StatusBadge status={target.last_check_status} />
                      ) : null}
                    </div>
                    <p className="break-all text-xs text-muted-foreground">
                      {target.base_url}
                    </p>
                    <p
                      className={cn(
                        "text-xs",
                        target.last_check_error
                          ? "text-destructive"
                          : "text-muted-foreground",
                      )}
                    >
                      {target.last_check_error
                        ? target.last_check_error
                        : target.last_check_at
                          ? `上次检测 ${relativeTime(target.last_check_at)}`
                          : "尚未检测"}
                    </p>
                  </div>
                  <div className="flex w-full items-center gap-2 xl:w-auto xl:justify-end">
                    <Button
                      size="sm"
                      variant="outline"
                      className="h-8 flex-1 px-3 xl:flex-none"
                      onClick={() => void openNewAPIGroupsDialog(target)}
                    >
                      <ListTree className="size-3.5" />
                      分组管理
                    </Button>
                    <Button
                      size="sm"
                      variant="outline"
                      className="h-8 px-3"
                      onClick={() =>
                        void openNewAPIGroupsDialog(target, true)
                      }
                      disabled={newAPIGroupsLoading}
                    >
                      <RefreshCw className="size-3.5" />
                      同步分组
                    </Button>
                    <Button
                      size="sm"
                      variant="outline"
                      className="h-8 flex-1 px-3 xl:flex-none"
                      onClick={() => void openAutoGroupsDialog(target)}
                    >
                      <ListTree className="size-3.5" />
                      自动分组
                    </Button>
                    <DropdownMenu>
                      <DropdownMenuTrigger asChild>
                        <Button
                          size="icon-sm"
                          variant="outline"
                          aria-label="New API 上游操作"
                        >
                          <MoreHorizontal className="size-4" />
                        </Button>
                      </DropdownMenuTrigger>
                      <DropdownMenuContent align="end" className="w-44">
                        <DropdownMenuItem
                          onSelect={() => checkTarget(target)}
                          disabled={busy === `check-${target.id}`}
                        >
                          <CheckCircle2 className="size-4" />
                          检测连接
                        </DropdownMenuItem>
                        <DropdownMenuItem
                          onSelect={() => openNewAPITargetDialog(target)}
                        >
                          <PencilLine className="size-4" />
                          编辑上游
                        </DropdownMenuItem>
                        <DropdownMenuSeparator />
                        <DropdownMenuItem
                          variant="destructive"
                          onSelect={() => deleteTarget(target)}
                          disabled={busy === `delete-target-${target.id}`}
                        >
                          <Trash2 className="size-4" />
                          删除上游
                        </DropdownMenuItem>
                      </DropdownMenuContent>
                    </DropdownMenu>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </Panel>

      <Dialog
        open={logSyncGroupID != null}
        onOpenChange={(open) => {
          if (!open) {
            setLogSyncGroupID(null);
            setLogs([]);
            setLogPage(1);
            setLogMeta({ total: 0, pages: 1 });
            setLogError(null);
          }
        }}
      >
        <DialogContent className="max-h-[85vh] overflow-hidden sm:max-w-3xl">
          <DialogHeader>
            <DialogTitle>同步日志</DialogTitle>
            <DialogDescription>
              {activeLogSyncGroup
                ? `${activeLogSyncGroup.display_name || activeLogSyncGroup.name} 执行记录。`
                : "当前同步分组执行记录。"}
            </DialogDescription>
          </DialogHeader>
          <div
            className="max-h-[60vh] overflow-y-auto overscroll-contain rounded-md border border-border"
            onScroll={(e) => {
              const target = e.target as HTMLElement;
              if (
                target.scrollTop + target.clientHeight >=
                target.scrollHeight - 32
              ) {
                loadMoreLogs();
              }
            }}
          >
            <div className="divide-y divide-border">
              {logs.map((log) => (
                <div key={log.id} className="bg-background px-4 py-3">
                  <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
                    <div className="flex items-center gap-2">
                      <StatusBadge status={log.success ? "ok" : "failed"} />
                      <span className="text-sm font-medium">
                        {syncLogActionLabel(log.action)}
                      </span>
                    </div>
                    <span className="text-xs text-muted-foreground">
                      {relativeTime(log.created_at)}
                    </span>
                  </div>
                  <pre className="max-h-72 overflow-auto whitespace-pre-wrap break-words rounded-md bg-muted/30 px-3 py-2 text-xs leading-5 text-muted-foreground">
                    {log.message || "—"}
                  </pre>
                </div>
              ))}
              {logLoading && logs.length === 0 ? (
                <div className="px-4 py-6 text-sm text-muted-foreground">
                  加载中…
                </div>
              ) : null}
              {!logLoading && logs.length === 0 ? (
                <div className="px-4 py-6 text-sm text-muted-foreground">
                  暂无执行日志。
                </div>
              ) : null}
              {logLoading && logs.length > 0 ? (
                <div className="px-4 py-3 text-xs text-muted-foreground">
                  加载更多中…
                </div>
              ) : null}
            </div>
          </div>
          {logError ? <p className="text-sm text-danger">{logError}</p> : null}
          <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
            <span className="text-xs text-muted-foreground">
              {logMeta.total > 0
                ? `已加载 ${logs.length} / ${logMeta.total} 条`
                : ""}
            </span>
            <Button
              variant="outline"
              size="sm"
              disabled={logLoading || logPage >= logMeta.pages}
              onClick={loadMoreLogs}
            >
              {logLoading && logPage > 1 ? "加载中…" : "查看更多"}
            </Button>
          </div>
        </DialogContent>
      </Dialog>

      <Dialog
        open={syncGroupDialogOpen}
        onOpenChange={(open) => {
          setSyncGroupDialogOpen(open);
          if (!open) {
            setSyncGroupForm({
              ...emptySyncGroupForm,
              target_id: selectedTargetID ?? 0,
            });
          }
        }}
      >
        <DialogContent className="flex max-h-[calc(100dvh-1rem)] w-[calc(100vw-1rem)] max-w-[calc(100vw-1rem)] flex-col overflow-hidden p-3 sm:w-[calc(100vw-2rem)] sm:max-w-none sm:p-6 xl:w-[1280px] 2xl:w-[1440px]">
          <DialogHeader className="shrink-0 pr-8 text-left">
            <DialogTitle>
              {syncGroupForm.id ? "编辑同步分组" : "新增同步分组"}
            </DialogTitle>
            <DialogDescription className="text-xs leading-5 sm:text-sm">
              同步分组配置在上方，同步账号在下方，可添加多条账号。
            </DialogDescription>
          </DialogHeader>
          <SyncGroupFormView
            syncGroupForm={syncGroupForm}
            sourceGroupsByChannel={sourceGroupsByChannel}
            targetGroups={targetGroups}
            targetProxies={targetProxies}
            channels={channels.data ?? []}
            busy={busy}
            onChange={setSyncGroupForm}
            onSave={saveSyncGroup}
            onCancel={() => setSyncGroupDialogOpen(false)}
            onToggleTargetGroup={toggleTargetGroup}
            onLoadSourceGroups={loadSourceGroups}
          />
        </DialogContent>
      </Dialog>

      <Dialog
        open={targetDialogOpen}
        onOpenChange={(open) => {
          setTargetDialogOpen(open);
          if (!open) setTargetForm(emptyTargetForm);
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              {targetForm.id ? "编辑 Sub2API 上游" : "新增 Sub2API 上游"}
            </DialogTitle>
            <DialogDescription>
              配置可写入的目标 Sub2API 管理端；管理密钥留空则保留原值。
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <Field label="名称">
              <Input
                value={targetForm.name}
                onChange={(e) =>
                  setTargetForm((prev) => ({ ...prev, name: e.target.value }))
                }
              />
            </Field>
            <Field label="管理端地址">
              <Input
                value={targetForm.base_url}
                placeholder="http://localhost:8080"
                onChange={(e) =>
                  setTargetForm((prev) => ({
                    ...prev,
                    base_url: e.target.value,
                  }))
                }
              />
            </Field>
            <Field label="管理 API Key">
              <Input
                value={targetForm.admin_api_key}
                placeholder={
                  targetForm.id ? "留空则保留原值" : "Sub2API 管理 API Key"
                }
                onChange={(e) =>
                  setTargetForm((prev) => ({
                    ...prev,
                    admin_api_key: e.target.value,
                  }))
                }
              />
            </Field>
            <SwitchLine
              id="target-enabled"
              label="启用上游"
              description="禁用后同步分组应用会跳过该上游。"
              checked={targetForm.enabled}
              onCheckedChange={(checked) =>
                setTargetForm((prev) => ({ ...prev, enabled: checked }))
              }
            />
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setTargetDialogOpen(false)}
            >
              取消编辑
            </Button>
            <Button onClick={saveTarget} disabled={busy === "target"}>
              保存上游
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog
        open={newAPITargetDialogOpen}
        onOpenChange={(open) => {
          setNewAPITargetDialogOpen(open);
          if (!open) setNewAPITargetForm(emptyNewAPITargetForm);
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              {newAPITargetForm.id ? "编辑 New API 上游" : "新增 New API 上游"}
            </DialogTitle>
            <DialogDescription>
              使用 New API Root 权限令牌读取和更新管理员设置中的
              AutoGroups。令牌留空则保留原值。
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <Field label="名称">
              <Input
                value={newAPITargetForm.name}
                onChange={(e) =>
                  setNewAPITargetForm((prev) => ({
                    ...prev,
                    name: e.target.value,
                  }))
                }
              />
            </Field>
            <Field label="New API 地址">
              <Input
                value={newAPITargetForm.base_url}
                placeholder="https://new-api.example.com"
                onChange={(e) =>
                  setNewAPITargetForm((prev) => ({
                    ...prev,
                    base_url: e.target.value,
                  }))
                }
              />
            </Field>
            <Field label="Root API Token">
              <Input
                type="password"
                autoComplete="new-password"
                value={newAPITargetForm.admin_api_key}
                placeholder={
                  newAPITargetForm.id ? "留空则保留原值" : "New API Root Token"
                }
                onChange={(e) =>
                  setNewAPITargetForm((prev) => ({
                    ...prev,
                    admin_api_key: e.target.value,
                  }))
                }
              />
            </Field>
            <SwitchLine
              id="newapi-target-enabled"
              label="启用上游"
              description="禁用后仍保留连接配置，但不会用于后续同步操作。"
              checked={newAPITargetForm.enabled}
              onCheckedChange={(enabled) =>
                setNewAPITargetForm((prev) => ({ ...prev, enabled }))
              }
            />
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setNewAPITargetDialogOpen(false)}
            >
              取消编辑
            </Button>
            <Button
              onClick={saveNewAPITarget}
              disabled={busy === "newapi-target"}
            >
              保存上游
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog
        open={autoGroupsDialogTarget != null}
        onOpenChange={(open) => {
          if (!open) {
            setAutoGroupsDialogTarget(null);
            setAutoGroups([]);
            setAutoGroupDragOver(null);
            setAvailableAutoGroups([]);
            setNewAutoGroup("");
          }
        }}
      >
        <DialogContent className="sm:max-w-xl">
          <DialogHeader>
            <DialogTitle>自动分组顺序</DialogTitle>
            <DialogDescription>
              {autoGroupsDialogTarget
                ? `${autoGroupsDialogTarget.name} 的 AutoGroups 按从上到下的顺序生效。`
                : ""}
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3">
            {availableAutoGroups.length > 0 ? (
              <div className="space-y-2">
                <Label>选择已有分组</Label>
                <div className="grid max-h-36 grid-cols-1 gap-1 overflow-y-auto rounded-md border p-2 sm:grid-cols-2">
                  {availableAutoGroups.map((group) => {
                    const checked = autoGroups.includes(group);
                    return (
                      <label
                        key={group}
                        className="flex min-w-0 cursor-pointer items-center gap-2 rounded-sm px-2 py-1.5 text-sm hover:bg-muted/60"
                      >
                        <Checkbox
                          checked={checked}
                          disabled={autoGroupsLoading}
                          onCheckedChange={(value) =>
                            toggleAvailableAutoGroup(group, value === true)
                          }
                        />
                        <span className="min-w-0 flex-1 truncate">{group}</span>
                        {checked ? (
                          <span className="text-xs tabular-nums text-muted-foreground">
                            {autoGroups.indexOf(group) + 1}
                          </span>
                        ) : null}
                      </label>
                    );
                  })}
                </div>
              </div>
            ) : null}
            <div className="flex gap-2">
              <Input
                value={newAutoGroup}
                placeholder="输入分组名称"
                disabled={autoGroupsLoading}
                onChange={(e) => setNewAutoGroup(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
                    e.preventDefault();
                    addAutoGroup();
                  }
                }}
              />
              <Button
                type="button"
                variant="outline"
                onClick={addAutoGroup}
                disabled={autoGroupsLoading}
              >
                <Plus className="size-4" />
                添加
              </Button>
            </div>
            <div className="max-h-72 space-y-2 overflow-y-auto rounded-md border p-2">
              {autoGroupsLoading ? (
                <p className="px-2 py-5 text-center text-sm text-muted-foreground">
                  加载中...
                </p>
              ) : autoGroups.length === 0 ? (
                <p className="px-2 py-5 text-center text-sm text-muted-foreground">
                  尚未配置自动分组。
                </p>
              ) : (
                autoGroups.map((group, index) => (
                  <div
                    key={group}
                    onDragOver={(event) => {
                      event.preventDefault();
                      event.dataTransfer.dropEffect = "move";
                      setAutoGroupDragOver(group);
                    }}
                    onDragLeave={() => {
                      if (autoGroupDragOver === group)
                        setAutoGroupDragOver(null);
                    }}
                    onDrop={(event) => {
                      event.preventDefault();
                      const fromGroup =
                        event.dataTransfer.getData("text/plain");
                      setAutoGroupDragOver(null);
                      if (fromGroup) moveAutoGroupByName(fromGroup, group);
                    }}
                    className={cn(
                      "flex items-center gap-2 rounded-md border bg-background px-2 py-1.5",
                      autoGroupDragOver === group &&
                        "border-primary/60 ring-1 ring-primary/25",
                    )}
                  >
                    <Button
                      type="button"
                      size="icon"
                      variant="ghost"
                      className="size-7 shrink-0 cursor-grab touch-none active:cursor-grabbing"
                      title="拖动调整优先级"
                      draggable={!autoGroupsLoading}
                      disabled={autoGroupsLoading || autoGroups.length < 2}
                      onDragStart={(event) => {
                        event.stopPropagation();
                        event.dataTransfer.effectAllowed = "move";
                        event.dataTransfer.setData("text/plain", group);
                      }}
                      onDragEnd={() => setAutoGroupDragOver(null)}
                    >
                      <GripVertical className="size-4 text-muted-foreground" />
                      <span className="sr-only">拖动调整优先级</span>
                    </Button>
                    <span className="w-6 text-center text-xs tabular-nums text-muted-foreground">
                      {index + 1}
                    </span>
                    <span className="min-w-0 flex-1 break-all text-sm">
                      {group}
                    </span>
                    <Button
                      size="icon"
                      variant="ghost"
                      className="size-7"
                      title="上移"
                      disabled={index === 0}
                      onClick={() => moveAutoGroup(index, -1)}
                    >
                      <ChevronUp className="size-4" />
                      <span className="sr-only">上移</span>
                    </Button>
                    <Button
                      size="icon"
                      variant="ghost"
                      className="size-7"
                      title="下移"
                      disabled={index === autoGroups.length - 1}
                      onClick={() => moveAutoGroup(index, 1)}
                    >
                      <ChevronDown className="size-4" />
                      <span className="sr-only">下移</span>
                    </Button>
                    <Button
                      size="icon"
                      variant="ghost"
                      className="size-7 text-destructive hover:text-destructive"
                      title="移除"
                      onClick={() =>
                        setAutoGroups((groups) =>
                          groups.filter((_, i) => i !== index),
                        )
                      }
                    >
                      <Trash2 className="size-4" />
                      <span className="sr-only">移除</span>
                    </Button>
                  </div>
                ))
              )}
            </div>
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setAutoGroupsDialogTarget(null)}
            >
              取消
            </Button>
            <Button
              onClick={saveAutoGroups}
              disabled={
                autoGroupsLoading ||
                (autoGroupsDialogTarget != null &&
                  busy === `auto-groups-${autoGroupsDialogTarget.id}`)
              }
            >
              保存顺序
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog
        open={newAPIGroupsDialogTarget != null}
        onOpenChange={(open) => {
          if (!open) {
            setNewAPIGroupsDialogTarget(null);
            setNewAPIGroups([]);
            resetNewAPIGroupForm();
          }
        }}
      >
        <DialogContent className="max-h-[85vh] overflow-y-auto sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>New API 分组管理</DialogTitle>
            <DialogDescription>
              {newAPIGroupsDialogTarget
                ? `管理 ${newAPIGroupsDialogTarget.name} 的分组倍率和说明。创建后可在自动分组中设置优先级。`
                : ""}
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="grid gap-3 rounded-lg border border-border bg-muted/15 p-3 sm:grid-cols-[minmax(0,1fr)_120px_minmax(0,1fr)_auto] sm:items-end">
              <Field label="分组名称">
                <Input
                  value={newAPIGroupForm.name}
                  placeholder="例如 premium"
                  disabled={newAPIGroupsLoading || editingNewAPIGroupName != null}
                  onChange={(event) =>
                    setNewAPIGroupForm((form) => ({
                      ...form,
                      name: event.target.value,
                    }))
                  }
                />
              </Field>
              <Field label="倍率">
                <Input
                  type="number"
                  min="0"
                  step="0.01"
                  value={newAPIGroupForm.ratio}
                  disabled={newAPIGroupsLoading}
                  onChange={(event) =>
                    setNewAPIGroupForm((form) => ({
                      ...form,
                      ratio: event.target.value,
                    }))
                  }
                />
              </Field>
              <Field label="说明">
                <Input
                  value={newAPIGroupForm.description}
                  placeholder="可选"
                  disabled={newAPIGroupsLoading}
                  onChange={(event) =>
                    setNewAPIGroupForm((form) => ({
                      ...form,
                      description: event.target.value,
                    }))
                  }
                />
              </Field>
              <div className="flex gap-2">
                {editingNewAPIGroupName ? (
                  <Button
                    type="button"
                    size="icon"
                    variant="outline"
                    title="取消编辑"
                    onClick={resetNewAPIGroupForm}
                    disabled={newAPIGroupsLoading}
                  >
                    <XCircle className="size-4" />
                    <span className="sr-only">取消编辑</span>
                  </Button>
                ) : null}
                <Button
                  type="button"
                  onClick={() => void saveNewAPIGroup()}
                  disabled={
                    newAPIGroupsLoading ||
                    (newAPIGroupsDialogTarget != null &&
                      busy === `newapi-group-${newAPIGroupsDialogTarget.id}`)
                  }
                >
                  <Plus className="size-4" />
                  {editingNewAPIGroupName ? "保存" : "创建"}
                </Button>
              </div>
            </div>
            {newAPIGroupsLoading ? (
              <p className="py-8 text-center text-sm text-muted-foreground">
                加载中...
              </p>
            ) : newAPIGroups.length === 0 ? (
              <EmptyBox text="还没有配置 New API 分组。" />
            ) : (
              <div className="overflow-hidden rounded-lg border border-border">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>分组</TableHead>
                      <TableHead>倍率</TableHead>
                      <TableHead>说明</TableHead>
                      <TableHead className="w-24 text-right">操作</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {newAPIGroups.map((group) => (
                      <TableRow key={group.name}>
                        <TableCell className="font-medium">{group.name}</TableCell>
                        <TableCell className="tabular-nums">
                          {formatRatio(group.ratio)}
                        </TableCell>
                        <TableCell className="max-w-56 truncate text-muted-foreground">
                          {group.description || "-"}
                        </TableCell>
                        <TableCell>
                          <div className="flex justify-end gap-1">
                            <Button
                              type="button"
                              size="icon"
                              variant="ghost"
                              title="编辑分组"
                              disabled={newAPIGroupsLoading}
                              onClick={() => editNewAPIGroup(group)}
                            >
                              <PencilLine className="size-4" />
                              <span className="sr-only">编辑分组</span>
                            </Button>
                            <Button
                              type="button"
                              size="icon"
                              variant="ghost"
                              className="text-destructive hover:text-destructive"
                              title="删除分组"
                              disabled={newAPIGroupsLoading}
                              onClick={() => void deleteNewAPIGroup(group)}
                            >
                              <Trash2 className="size-4" />
                              <span className="sr-only">删除分组</span>
                            </Button>
                          </div>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            )}
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setNewAPIGroupsDialogTarget(null)}
            >
              关闭
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {dialog}
    </div>
  );
}

function TargetGroupOrderList({
  target,
  groups,
  busy,
  onReorder,
}: {
  target: UpstreamSyncTarget;
  groups: UpstreamSyncTargetGroup[];
  busy: boolean;
  onReorder: (groups: UpstreamSyncTargetGroup[]) => void;
}) {
  const dragIDRef = useRef<number | null>(null);
  const [draggingID, setDraggingID] = useState<number | null>(null);
  const [dragOverID, setDragOverID] = useState<number | null>(null);

  function moveGroup(fromID: number, toID: number) {
    if (fromID === toID) return;
    const fromIndex = groups.findIndex((group) => group.id === fromID);
    const toIndex = groups.findIndex((group) => group.id === toID);
    if (fromIndex < 0 || toIndex < 0) return;
    const next = [...groups];
    const [item] = next.splice(fromIndex, 1);
    next.splice(toIndex, 0, item);
    onReorder(next);
  }

  return (
    <section className="mb-4 space-y-3 rounded-lg border border-border bg-muted/15 p-3">
      <div className="space-y-1">
        <p className="text-sm font-semibold text-foreground">上游分组排序</p>
        <p className="text-xs text-muted-foreground">
          拖动手柄调整 {target.name} 的远端分组排序顺序。
        </p>
      </div>
      {groups.length === 0 ? (
        <p className="py-3 text-center text-sm text-muted-foreground">
          请先同步上游分组。
        </p>
      ) : (
        <div className="max-h-64 space-y-1.5 overflow-y-auto">
          {groups.map((group, index) => {
            const isDragging = draggingID === group.id;
            const isDragOver =
              dragOverID === group.id && draggingID !== group.id;
            return (
              <div
                key={group.id}
                onDragOver={(event) => {
                  event.preventDefault();
                  event.dataTransfer.dropEffect = "move";
                  if (dragOverID !== group.id) setDragOverID(group.id);
                }}
                onDragLeave={() => {
                  if (dragOverID === group.id) setDragOverID(null);
                }}
                onDrop={(event) => {
                  event.preventDefault();
                  const raw = event.dataTransfer.getData("text/plain");
                  const fromID = dragIDRef.current ?? Number(raw);
                  dragIDRef.current = null;
                  setDraggingID(null);
                  setDragOverID(null);
                  if (Number.isFinite(fromID) && fromID > 0) {
                    moveGroup(fromID, group.id);
                  }
                }}
                className={cn(
                  "flex min-w-0 items-center gap-2 rounded-md border bg-background px-2 py-1.5 transition-colors",
                  isDragging && "opacity-50",
                  isDragOver && "border-primary/60 ring-1 ring-primary/25",
                )}
              >
                <Button
                  type="button"
                  size="icon"
                  variant="ghost"
                  className="size-7 shrink-0 cursor-grab touch-none active:cursor-grabbing"
                  title="拖动调整排序"
                  draggable={!busy}
                  disabled={busy || groups.length < 2}
                  onDragStart={(event) => {
                    event.stopPropagation();
                    dragIDRef.current = group.id;
                    setDraggingID(group.id);
                    event.dataTransfer.effectAllowed = "move";
                    event.dataTransfer.setData("text/plain", String(group.id));
                  }}
                  onDragEnd={() => {
                    dragIDRef.current = null;
                    setDraggingID(null);
                    setDragOverID(null);
                  }}
                >
                  <GripVertical className="size-4 text-muted-foreground" />
                  <span className="sr-only">拖动调整排序</span>
                </Button>
                <span className="w-6 text-center text-xs tabular-nums text-muted-foreground">
                  {index + 1}
                </span>
                <span className="min-w-0 flex-1 truncate text-sm font-medium">
                  {group.name}
                </span>
                <Badge variant="outline" className="shrink-0 font-normal">
                  {group.platform || "openai"}
                </Badge>
                <span className="shrink-0 text-xs tabular-nums text-muted-foreground">
                  {formatRatio(group.ratio)}
                </span>
              </div>
            );
          })}
        </div>
      )}
    </section>
  );
}

function SyncGroupList({
  syncGroups,
  busy,
  onAdd,
  onRefresh,
  refreshBusy,
  onApply,
  onLogs,
  onEdit,
  onDeleteManaged,
  onDelete,
}: {
  syncGroups: UpstreamSyncGroup[];
  busy: string | null;
  onAdd: () => void;
  onRefresh: () => void;
  refreshBusy: boolean;
  onApply: (syncGroup: UpstreamSyncGroup) => void;
  onLogs: (syncGroup: UpstreamSyncGroup) => void;
  onEdit: (syncGroup: UpstreamSyncGroup) => void;
  onDeleteManaged: (syncGroup: UpstreamSyncGroup) => void;
  onDelete: (syncGroup: UpstreamSyncGroup) => void;
}) {
  return (
    <div className="space-y-3">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div className="space-y-1">
          <p className="text-sm font-semibold text-foreground">同步分组</p>
          <p className="text-xs text-muted-foreground">
            保存同步分组不会写远端，点击应用才会创建或更新托管对象。
          </p>
        </div>
        <div className="flex gap-2">
          <Button
            size="sm"
            variant="outline"
            onClick={onRefresh}
            disabled={refreshBusy}
          >
            <RefreshCw
              className={cn("size-3.5", refreshBusy && "animate-spin")}
            />
            刷新
          </Button>
          <Button size="sm" onClick={onAdd}>
            <Plus className="size-3.5" />
            新增分组
          </Button>
        </div>
      </div>
      {syncGroups.length === 0 ? (
        <EmptyBox text="该 Sub2API 上游还没有同步分组。" />
      ) : (
        <div className="space-y-3">
          {syncGroups.map((syncGroup) => {
            const isApplying = busy === `apply-${syncGroup.id}`;
            return (
              <div
                key={syncGroup.id}
                className="rounded-xl border border-border bg-background/80 p-3 sm:p-4"
              >
                <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_auto] xl:items-start">
                  <div className="min-w-0 space-y-1">
                    <div className="flex flex-wrap items-center gap-2">
                      <p className="truncate text-sm font-semibold">
                        {syncGroup.display_name || syncGroup.name}
                      </p>
                      <StatusBadge
                        status={
                          syncGroup.enabled ? "cron_enabled" : "cron_disabled"
                        }
                      />
                      {syncGroup.apply_status ? (
                        <StatusBadge status={syncGroup.apply_status} />
                      ) : null}
                    </div>
                    <p className="text-xs text-muted-foreground">
                      同步名称：{syncGroup.name} · 同步账号{" "}
                      {syncGroup.accounts?.length ?? 0} 个 · 目标分组{" "}
                      {syncGroup.target_group_ids.length} 个
                    </p>
                    {syncGroup.apply_error ? (
                      <ApplyMessageBox message={syncGroup.apply_error} />
                    ) : (
                      <p className="text-xs text-muted-foreground">
                        {syncGroup.last_applied_at
                          ? `最近应用 ${relativeTime(syncGroup.last_applied_at)}`
                          : "尚未应用"}
                      </p>
                    )}
                  </div>
                  <div className="flex w-full items-center gap-2 xl:w-auto xl:justify-end">
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={() => onApply(syncGroup)}
                      disabled={isApplying}
                      className={cn(
                        "relative h-8 flex-1 overflow-hidden px-3 xl:flex-none",
                        isApplying &&
                          "animate-pulse border-primary/50 bg-primary/10 text-primary shadow-[0_0_0_1px_hsl(var(--primary)/0.12)]",
                      )}
                    >
                      {isApplying ? (
                        <RefreshCw className="size-3.5 animate-spin" />
                      ) : (
                        <Play className="size-3.5" />
                      )}
                      {isApplying ? "应用中" : "应用"}
                    </Button>
                    <Button
                      size="sm"
                      variant="outline"
                      className="h-8 px-3"
                      onClick={() => onLogs(syncGroup)}
                    >
                      日志
                    </Button>
                    <DropdownMenu>
                      <DropdownMenuTrigger asChild>
                        <Button
                          size="icon-sm"
                          variant="outline"
                          aria-label="同步分组操作"
                        >
                          <MoreHorizontal className="size-4" />
                        </Button>
                      </DropdownMenuTrigger>
                      <DropdownMenuContent align="end" className="w-44">
                        <DropdownMenuItem onSelect={() => onEdit(syncGroup)}>
                          <PencilLine className="size-4" />
                          编辑分组
                        </DropdownMenuItem>
                        <DropdownMenuSeparator />
                        <DropdownMenuItem
                          variant="destructive"
                          onSelect={() => onDeleteManaged(syncGroup)}
                          disabled={busy === `managed-${syncGroup.id}`}
                        >
                          <Trash2 className="size-4" />
                          删除托管对象
                        </DropdownMenuItem>
                        <DropdownMenuItem
                          variant="destructive"
                          onSelect={() => onDelete(syncGroup)}
                          disabled={
                            busy === `sync-group-delete-${syncGroup.id}`
                          }
                        >
                          <Trash2 className="size-4" />
                          删除同步分组
                        </DropdownMenuItem>
                      </DropdownMenuContent>
                    </DropdownMenu>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

function SyncGroupFormView({
  syncGroupForm,
  sourceGroupsByChannel,
  targetGroups,
  targetProxies,
  channels,
  busy,
  onChange,
  onSave,
  onCancel,
  onToggleTargetGroup,
  onLoadSourceGroups,
}: {
  syncGroupForm: SyncGroupForm;
  sourceGroupsByChannel: Record<number, RateSnapshot[]>;
  targetGroups: UpstreamSyncTargetGroup[];
  targetProxies: UpstreamSyncTargetProxy[];
  channels: { id: number; name: string }[];
  busy: string | null;
  onChange: React.Dispatch<React.SetStateAction<SyncGroupForm>>;
  onSave: () => void;
  onCancel: () => void;
  onToggleTargetGroup: (id: number, checked: boolean) => void;
  onLoadSourceGroups: (channelID: number) => void;
}) {
  const filteredTargetGroups = useMemo(() => {
    const direction = syncGroupForm.rate_sort_direction === "desc" ? -1 : 1;
    return targetGroups
      .filter(
        (group) =>
          normalizePlatform(group.platform) ===
          normalizePlatform(syncGroupForm.platform),
      )
      .sort((a, b) => {
        const diff = (a.ratio - b.ratio) * direction;
        return diff === 0 ? a.id - b.id : diff;
      });
  }, [targetGroups, syncGroupForm.platform, syncGroupForm.rate_sort_direction]);
  const sortedAccountRows = useMemo(
    () =>
      sortSyncAccountRows(
        syncGroupForm.accounts,
        sourceGroupsByChannel,
        syncGroupForm.rate_sort_direction,
      ),
    [
      sourceGroupsByChannel,
      syncGroupForm.accounts,
      syncGroupForm.rate_sort_direction,
    ],
  );
  const testModelOptions = useMemo(
    () => splitModelInput(syncGroupForm.model_limits),
    [syncGroupForm.model_limits],
  );
  const [sourceModelsByRow, setSourceModelsByRow] = useState<
    Record<number, string[]>
  >({});
  const [sourceModelsLoadingByRow, setSourceModelsLoadingByRow] = useState<
    Record<number, boolean>
  >({});

  useEffect(() => {
    syncGroupForm.accounts.forEach((account, index) => {
      if (account.source_channel_id) {
        void loadSourceModels(index, syncGroupForm.platform, account);
      }
    });
  }, [syncGroupForm.id]);

  function updateAccount(index: number, patch: Partial<SyncAccountForm>) {
    onChange((prev) => ({
      ...prev,
      accounts: prev.accounts.map((item, i) =>
        i === index ? { ...item, ...patch } : item,
      ),
    }));
  }

  function addAccount() {
    onChange((prev) => ({
      ...prev,
      accounts: [...prev.accounts, { ...emptySyncAccountForm }],
    }));
  }

  function removeAccount(index: number) {
    if (syncGroupForm.accounts.length <= 1) return;
    const nextAccounts = syncGroupForm.accounts.filter((_, i) => i !== index);
    onChange((prev) => ({
      ...prev,
      accounts: nextAccounts,
    }));
    setSourceModelsByRow({});
    setSourceModelsLoadingByRow({});
    nextAccounts.forEach((account, nextIndex) => {
      if (account.source_channel_id) {
        void loadSourceModels(nextIndex, syncGroupForm.platform, account);
      }
    });
  }

  function sourceGroupsFor(account: SyncAccountForm) {
    return account.source_channel_id
      ? (sourceGroupsByChannel[account.source_channel_id] ?? [])
      : [];
  }

  async function loadSourceModels(
    index: number,
    platform: string,
    account: SyncAccountForm,
  ) {
    if (!account.source_channel_id) {
      setSourceModelsByRow((prev) => {
        const next = { ...prev };
        delete next[index];
        return next;
      });
      return;
    }
    const params = new URLSearchParams({
      channel_id: String(account.source_channel_id),
      platform,
    });
    if (account.id) params.set("sync_account_id", String(account.id));
    if (account.source_group_id) {
      params.set("source_group_id", account.source_group_id);
    }
    if (account.source_group_name.trim()) {
      params.set("source_group_name", account.source_group_name.trim());
    }
    setSourceModelsLoadingByRow((prev) => ({ ...prev, [index]: true }));
    try {
      const list = await apiFetch<string[]>(
        `/upstream-sync/source-models?${params.toString()}`,
      );
      setSourceModelsByRow((prev) => ({
        ...prev,
        [index]: uniqueModels(list),
      }));
    } catch (err) {
      setSourceModelsByRow((prev) => {
        const next = { ...prev };
        delete next[index];
        return next;
      });
      toast.error(err instanceof Error ? err.message : "加载模型失败");
    } finally {
      setSourceModelsLoadingByRow((prev) => ({
        ...prev,
        [index]: false,
      }));
    }
  }

  return (
    <div className="min-h-0 flex-1 space-y-4 overflow-y-auto pr-1 sm:space-y-5 sm:pr-2">
      <section className="rounded-lg border border-border bg-background/80 p-3">
        <div className="mb-3 flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
          <p className="text-sm font-semibold text-foreground">同步分组</p>
          <div className="flex items-center gap-2">
            <Label className="text-xs text-muted-foreground">倍率排序</Label>
            <Select
              value={syncGroupForm.rate_sort_direction}
              onValueChange={(value) =>
                onChange((prev) => ({
                  ...prev,
                  rate_sort_direction: value as "asc" | "desc",
                }))
              }
            >
              <SelectTrigger className="h-8 w-28">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="asc">倍率升序</SelectItem>
                <SelectItem value="desc">倍率降序</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>
        <div className="space-y-3">
          <div className="grid gap-3 md:grid-cols-4">
            <Field label="分组名称">
              <Input
                className="h-8"
                value={syncGroupForm.display_name}
                onChange={(e) =>
                  onChange((prev) => ({
                    ...prev,
                    display_name: e.target.value,
                  }))
                }
              />
            </Field>
            <Field label="分组模板名称">
              <Input
                className="h-8"
                value={syncGroupForm.name_template}
                disabled={Boolean(syncGroupForm.id)}
                onChange={(e) =>
                  onChange((prev) => ({
                    ...prev,
                    name_template: e.target.value,
                  }))
                }
              />
            </Field>
            <Field label="平台">
              <Select
                value={normalizePlatform(syncGroupForm.platform)}
                disabled={Boolean(syncGroupForm.id)}
                onValueChange={(value) => {
                  onChange((prev) => ({
                    ...prev,
                    platform: value,
                    target_group_ids: prev.target_group_ids.filter((id) => {
                      const group = targetGroups.find((item) => item.id === id);
                      return normalizePlatform(group?.platform) === value;
                    }),
                  }));
                  syncGroupForm.accounts.forEach((account, index) => {
                    if (account.source_channel_id) {
                      void loadSourceModels(index, value, account);
                    }
                  });
                }}
              >
                <SelectTrigger className="h-8 w-full">
                  <SelectValue placeholder="选择平台" />
                </SelectTrigger>
                <SelectContent>
                  {syncPlatformOptions.map((platform) => (
                    <SelectItem key={platform.value} value={platform.value}>
                      {platform.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </Field>
            <Field label="模型选择">
              <Select
                value={
                  syncGroupForm.model_limits_mode === "sync_upstream"
                    ? "__sync_upstream__"
                    : "__custom__"
                }
                onValueChange={(value) =>
                  onChange((prev) => ({
                    ...prev,
                    model_limits_mode:
                      value === "__sync_upstream__"
                        ? "sync_upstream"
                        : "custom",
                    model_limits: "",
                  }))
                }
              >
                <SelectTrigger className="h-8 w-full">
                  <SelectValue placeholder="选择模型" />
                </SelectTrigger>
                <SelectContent>
                  {modelOptions.map((model) => (
                    <SelectItem key={model.value} value={model.value}>
                      {model.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </Field>
          </div>

          {syncGroupForm.model_limits_mode === "custom" ? (
            <Field label="自定义模型">
              <Input
                className="h-8"
                value={syncGroupForm.model_limits}
                placeholder="多个模型用英文逗号分隔。"
                onChange={(e) =>
                  onChange((prev) => ({
                    ...prev,
                    model_limits: e.target.value,
                  }))
                }
                onBlur={(e) =>
                  onChange((prev) => ({
                    ...prev,
                    model_limits: normalizeModelInput(e.target.value),
                  }))
                }
              />
            </Field>
          ) : null}
          {syncGroupForm.model_limits_mode === "sync_upstream" &&
          syncGroupForm.model_limits ? (
            <Field label="已同步模型">
              <Input
                className="h-8"
                value={syncGroupForm.model_limits}
                disabled
              />
            </Field>
          ) : null}

          <Field label="目标分组">
            <div className="max-h-32 space-y-1 overflow-auto rounded-md border border-border bg-muted/20 p-2">
              {targetGroups.length === 0 ? (
                <p className="text-xs text-muted-foreground">
                  请先同步目标分组。
                </p>
              ) : filteredTargetGroups.length === 0 ? (
                <p className="text-xs text-muted-foreground">
                  当前平台没有可选目标分组。
                </p>
              ) : (
                filteredTargetGroups.map((group) => (
                  <label
                    key={group.id}
                    className="flex cursor-pointer items-center gap-2 rounded-md px-2 py-1 text-sm hover:bg-background"
                  >
                    <Checkbox
                      checked={syncGroupForm.target_group_ids.includes(
                        group.id,
                      )}
                      onCheckedChange={(checked) =>
                        onToggleTargetGroup(group.id, checked === true)
                      }
                    />
                    <span className="min-w-0 flex-1 truncate">
                      {group.name} · {platformLabel(group.platform)} · 远端 ID{" "}
                      {group.remote_group_id} · 倍率 {formatRatio(group.ratio)}
                    </span>
                    <StatusBadge status={group.status || "active"} />
                  </label>
                ))
              )}
            </div>
          </Field>

          <div className="grid gap-2 md:grid-cols-4">
            <CompactSwitchLine
              id="sync-group-cron-enabled"
              label="Cron 自动应用"
              checked={syncGroupForm.enabled}
              onCheckedChange={(checked) =>
                onChange((prev) => ({ ...prev, enabled: checked }))
              }
            />
            <CompactSwitchLine
              id="pool-mode"
              label="池模式"
              checked={syncGroupForm.pool_mode_enabled}
              onCheckedChange={(checked) =>
                onChange((prev) => ({ ...prev, pool_mode_enabled: checked }))
              }
            />
            <CompactSwitchLine
              id="custom-errors"
              label="自定义错误码"
              checked={syncGroupForm.custom_error_codes_enabled}
              onCheckedChange={(checked) =>
                onChange((prev) => ({
                  ...prev,
                  custom_error_codes_enabled: checked,
                }))
              }
            />
            <div className="flex h-9 items-center justify-between gap-3 rounded-md border border-border bg-background/90 px-3">
              <Label className="text-xs font-medium text-foreground">
                同步功能
              </Label>
              <Select
                value={syncGroupForm.sync_mode}
                onValueChange={(value) =>
                  onChange((prev) => ({
                    ...prev,
                    sync_mode: value as "account" | "gateway_rate",
                    gateway_rate_sync:
                      value === "gateway_rate"
                        ? (prev.gateway_rate_sync ?? {
                            ...emptyGatewayRateSyncForm,
                          })
                        : prev.gateway_rate_sync,
                  }))
                }
              >
                <SelectTrigger className="h-7 w-28">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="account">同步账号</SelectItem>
                  <SelectItem value="gateway_rate">倍率同步</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>

          {syncGroupForm.pool_mode_enabled ||
          syncGroupForm.custom_error_codes_enabled ? (
            <div className="grid gap-3 md:grid-cols-3">
              {syncGroupForm.pool_mode_enabled ? (
                <>
                  <Field label="池模式重试次数">
                    <Input
                      className="h-8"
                      type="number"
                      value={String(syncGroupForm.pool_mode_retry_count)}
                      onChange={(e) =>
                        onChange((prev) => ({
                          ...prev,
                          pool_mode_retry_count: num(e.target.value),
                        }))
                      }
                    />
                  </Field>
                  <Field label="池模式重试状态码">
                    <Input
                      className="h-8"
                      value={syncGroupForm.pool_mode_retry_status_codes}
                      placeholder="429,500,502"
                      onChange={(e) =>
                        onChange((prev) => ({
                          ...prev,
                          pool_mode_retry_status_codes: e.target.value,
                        }))
                      }
                    />
                  </Field>
                </>
              ) : null}
              {syncGroupForm.custom_error_codes_enabled ? (
                <Field label="自定义错误码">
                  <Input
                    className="h-8"
                    value={syncGroupForm.custom_error_codes}
                    placeholder="40001,40002"
                    onChange={(e) =>
                      onChange((prev) => ({
                        ...prev,
                        custom_error_codes: e.target.value,
                      }))
                    }
                  />
                </Field>
              ) : null}
            </div>
          ) : null}
        </div>
      </section>

      {syncGroupForm.sync_mode === "account" ? (
        <section className="rounded-xl border border-border bg-background/80 p-3 sm:p-4">
          <div className="mb-4 flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
            <div className="space-y-1">
              <p className="text-sm font-semibold text-foreground">同步账号</p>
              <p className="text-xs text-muted-foreground">
                每行会独立创建源 API Key 和目标账号。
              </p>
            </div>
            <Button
              size="sm"
              variant="outline"
              className="w-full sm:w-auto"
              onClick={addAccount}
            >
              <Plus className="size-3.5" />
              添加账号
            </Button>
          </div>

          <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="min-w-44">源渠道</TableHead>
                  <TableHead className="min-w-44">源分组</TableHead>
                  <TableHead className="min-w-28">倍率换算</TableHead>
                  <TableHead className="min-w-32">账号计费倍率</TableHead>
                  <TableHead className="min-w-28">权重/负载</TableHead>
                  <TableHead className="min-w-24">并发</TableHead>
                  <TableHead className="min-w-32">代理</TableHead>
                  <TableHead className="min-w-24">状态</TableHead>
                  <TableHead className="min-w-56">
                    <span className="flex items-center gap-1">
                      测试模型
                      <Tooltip delayDuration={150}>
                        <TooltipTrigger asChild>
                          <button
                            type="button"
                            className="text-muted-foreground transition-colors hover:text-foreground"
                          >
                            <HelpCircle className="size-3.5" />
                            <span className="sr-only">测试功能说明</span>
                          </button>
                        </TooltipTrigger>
                        <TooltipContent side="top" className="max-w-64 text-xs">
                          选择模型后，应用同步会调用 Sub2API
                          测试目标账号；测试通过启用调度，失败禁用调度。
                        </TooltipContent>
                      </Tooltip>
                    </span>
                  </TableHead>
                  <TableHead className="w-12" />
                </TableRow>
              </TableHeader>
              <TableBody>
                {sortedAccountRows.map(({ account, index, rate }) => {
                  const sourceGroups = sourceGroupsFor(account);
                  const calculatedRate = rate;
                  const selectedTestModel = account.test_model.trim();
                  const sourceTestModels = sourceModelsByRow[index] ?? [];
                  const rowTestModels = uniqueModels([
                    ...testModelOptions,
                    ...sourceTestModels,
                    selectedTestModel,
                  ]);
                  const isLoadingModels = sourceModelsLoadingByRow[index];
                  return (
                    <TableRow key={account.id ?? index}>
                      <TableCell>
                        <Select
                          value={
                            account.source_channel_id
                              ? String(account.source_channel_id)
                              : "0"
                          }
                          onValueChange={(value) => {
                            const channelID = Number(value);
                            const nextAccount = {
                              ...account,
                              source_channel_id: channelID,
                              source_group_id: "",
                              source_group_name: "",
                              test_model: "",
                            };
                            updateAccount(index, nextAccount);
                            void onLoadSourceGroups(channelID);
                            void loadSourceModels(
                              index,
                              syncGroupForm.platform,
                              nextAccount,
                            );
                          }}
                        >
                          <SelectTrigger className="w-44">
                            <SelectValue placeholder="选择源渠道" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="0">请选择</SelectItem>
                            {channels.map((channel) => (
                              <SelectItem
                                key={channel.id}
                                value={String(channel.id)}
                              >
                                {channel.name}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </TableCell>
                      <TableCell>
                        <Select
                          value={sourceGroupSelectValue(account)}
                          onValueChange={(value) => {
                            const sourceGroup = sourceGroups.find(
                              (group) =>
                                sourceGroupOptionValue(group) === value,
                            );
                            const patch = {
                              source_group_id:
                                sourceGroup?.remote_group_id == null
                                  ? ""
                                  : String(sourceGroup.remote_group_id),
                              // 有 ID 时也保留名称（sub2api）；仅 none 时清空
                              source_group_name:
                                value === "none"
                                  ? ""
                                  : (sourceGroup?.model_name ?? "").trim(),
                              test_model: "",
                            };
                            const nextAccount = { ...account, ...patch };
                            updateAccount(index, patch);
                            void loadSourceModels(
                              index,
                              syncGroupForm.platform,
                              nextAccount,
                            );
                          }}
                          disabled={!account.source_channel_id}
                        >
                          <SelectTrigger className="w-44">
                            {(() => {
                              const selected = sourceGroups.find(
                                (g) =>
                                  sourceGroupOptionValue(g) ===
                                  sourceGroupSelectValue(account),
                              );
                              if (selected) {
                                return (
                                  <SelectValue>
                                    {selected.model_name} ·{" "}
                                    {formatRatio(selected.ratio)}
                                  </SelectValue>
                                );
                              }
                              return <SelectValue placeholder="不绑定分组" />;
                            })()}
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="none">不绑定分组</SelectItem>
                            {sourceGroups.map((group) => (
                              <SelectItem
                                key={group.id}
                                value={sourceGroupOptionValue(group)}
                              >
                                <span className="flex flex-col items-start">
                                  <span>
                                    {group.model_name} ·{" "}
                                    {formatRatio(group.ratio)}
                                  </span>
                                  <span className="max-w-96 whitespace-normal break-words text-[11px] text-muted-foreground">
                                    {group.description || "无描述"}
                                  </span>
                                </span>
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </TableCell>
                      <TableCell>
                        <Select
                          value={account.rate_convert_mode}
                          onValueChange={(value) =>
                            updateAccount(index, {
                              rate_convert_mode:
                                value as UpstreamSyncRateConvertMode,
                            })
                          }
                        >
                          <SelectTrigger className="w-28">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="raw">原值</SelectItem>
                            <SelectItem value="multiply_100">x100</SelectItem>
                            <SelectItem value="divide_100">/100</SelectItem>
                            <SelectItem value="custom">自定义</SelectItem>
                          </SelectContent>
                        </Select>
                      </TableCell>
                      <TableCell>
                        <Input
                          className="w-32"
                          type="text"
                          inputMode="decimal"
                          step="0.0001"
                          value={
                            account.rate_convert_mode === "custom"
                              ? String(account.rate_convert_value)
                              : formatRate(calculatedRate)
                          }
                          disabled={account.rate_convert_mode !== "custom"}
                          onChange={(e) =>
                            updateAccount(index, {
                              rate_convert_value: e.target.value,
                            })
                          }
                        />
                      </TableCell>
                      <TableCell>
                        <Input
                          className="w-28"
                          type="number"
                          step="0.01"
                          value={String(account.weight)}
                          onChange={(e) =>
                            updateAccount(index, {
                              weight: num(e.target.value),
                            })
                          }
                        />
                      </TableCell>
                      <TableCell>
                        <Input
                          className="w-24"
                          type="number"
                          value={String(account.concurrency)}
                          onChange={(e) =>
                            updateAccount(index, {
                              concurrency: num(e.target.value),
                            })
                          }
                        />
                      </TableCell>
                      <TableCell>
                        <Select
                          value={account.proxy_id || "none"}
                          onValueChange={(value) =>
                            updateAccount(index, {
                              proxy_id: value === "none" ? "" : value,
                            })
                          }
                        >
                          <SelectTrigger className="w-32">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="none">不使用代理</SelectItem>
                            {targetProxies.map((proxy) => (
                              <SelectItem
                                key={proxy.id}
                                value={String(proxy.id)}
                              >
                                {proxy.name} · {proxy.protocol} · {proxy.host}:
                                {proxy.port}
                              </SelectItem>
                            ))}
                            {account.proxy_id &&
                            !targetProxies.some(
                              (proxy) => String(proxy.id) === account.proxy_id,
                            ) ? (
                              <SelectItem value={account.proxy_id}>
                                代理 ID {account.proxy_id}
                              </SelectItem>
                            ) : null}
                          </SelectContent>
                        </Select>
                      </TableCell>
                      <TableCell>
                        <Select
                          value={account.enabled ? "enabled" : "disabled"}
                          onValueChange={(value) =>
                            updateAccount(index, {
                              enabled: value === "enabled",
                            })
                          }
                        >
                          <SelectTrigger className="w-24">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="enabled">启用</SelectItem>
                            <SelectItem value="disabled">禁用</SelectItem>
                          </SelectContent>
                        </Select>
                      </TableCell>
                      <TableCell>
                        <TestModelPicker
                          enabled={account.test_enabled}
                          value={selectedTestModel}
                          models={rowTestModels}
                          loading={isLoadingModels}
                          onChange={(enabled, model) =>
                            updateAccount(index, {
                              test_enabled: enabled,
                              test_model: model,
                            })
                          }
                        />
                      </TableCell>
                      <TableCell>
                        <Button
                          size="icon-sm"
                          variant="ghost"
                          className="text-destructive hover:bg-destructive/10 hover:text-destructive"
                          onClick={() => removeAccount(index)}
                          disabled={syncGroupForm.accounts.length <= 1}
                        >
                          <Trash2 className="size-4" />
                        </Button>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </div>
        </section>
      ) : null}

      {syncGroupForm.sync_mode === "gateway_rate" ? (
        <GatewayRateSyncCard
          syncGroupForm={syncGroupForm}
          onChange={onChange}
        />
      ) : null}

      <DialogFooter className="sticky bottom-0 -mx-3 border-t bg-background/95 px-3 pt-3 backdrop-blur sm:static sm:mx-0 sm:border-0 sm:bg-transparent sm:px-0 sm:pt-0">
        <Button
          variant="outline"
          className="w-full sm:w-auto"
          onClick={onCancel}
        >
          取消
        </Button>
        <Button
          className="w-full sm:w-auto"
          onClick={onSave}
          disabled={busy === "sync-group"}
        >
          {syncGroupForm.id ? "保存分组" : "新增分组"}
        </Button>
      </DialogFooter>
    </div>
  );
}

function GatewayRateSyncCard({
  syncGroupForm,
  onChange,
}: {
  syncGroupForm: SyncGroupForm;
  onChange: React.Dispatch<React.SetStateAction<SyncGroupForm>>;
}) {
  const [gatewayGroups, setGatewayGroups] = useState<GatewayGroup[]>([]);
  const [gatewayRoutes, setGatewayRoutes] = useState<GatewayRoute[]>([]);
  const [groupsLoading, setGroupsLoading] = useState(false);
  const [routesLoading, setRoutesLoading] = useState(false);
  const rateSync = syncGroupForm.gateway_rate_sync;
  const gatewayGroupID = Number(rateSync?.gateway_group_id || 0);

  useEffect(() => {
    if (rateSync) return;
    onChange((prev) => ({
      ...prev,
      gateway_rate_sync: { ...emptyGatewayRateSyncForm },
    }));
  }, [onChange, rateSync]);

  useEffect(() => {
    let cancelled = false;
    setGroupsLoading(true);
    apiFetch<{ items: GatewayGroup[] }>("/gateway/groups")
      .then((res) => {
        if (!cancelled) setGatewayGroups(res.items ?? []);
      })
      .catch((err) => {
        if (!cancelled) {
          toast.error(err instanceof Error ? err.message : "加载网关组失败");
        }
      })
      .finally(() => {
        if (!cancelled) setGroupsLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!gatewayGroupID) {
      setGatewayRoutes([]);
      return;
    }
    let cancelled = false;
    setRoutesLoading(true);
    apiFetch<{ items: GatewayRoute[] }>(
      `/gateway/groups/${gatewayGroupID}/routes`,
    )
      .then((res) => {
        if (!cancelled) setGatewayRoutes(res.items ?? []);
      })
      .catch((err) => {
        if (!cancelled) {
          toast.error(err instanceof Error ? err.message : "加载网关路由失败");
        }
      })
      .finally(() => {
        if (!cancelled) setRoutesLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [gatewayGroupID]);

  const rateRange = useMemo(() => {
    const rates = gatewayRoutes
      .filter((route) => route.enabled && route.billing_rate_multiplier > 0)
      .map((route) => route.billing_rate_multiplier)
      .sort((a, b) => a - b);
    return {
      min: rates[0] ?? 0,
      max: rates[rates.length - 1] ?? 0,
    };
  }, [gatewayRoutes]);

  function setRateSync(patch: Partial<SyncAccountForm>) {
    onChange((prev) => ({
      ...prev,
      gateway_rate_sync: {
        ...(prev.gateway_rate_sync ?? emptyGatewayRateSyncForm),
        ...patch,
      },
    }));
  }

  const selectedBaseRate =
    rateSync?.gateway_rate_mode === "min" ? rateRange.min : rateRange.max;
  const calculatedRate = rateSync
    ? clampGatewayRate(
        applyGatewayRateOperation(
          selectedBaseRate,
          rateSync.rate_convert_mode,
          decimalNumber(rateSync.rate_convert_value),
        ),
        decimalNumber(rateSync.gateway_rate_min),
        decimalNumber(rateSync.gateway_rate_max),
      )
    : 0;
  const rateVariable =
    rateSync?.gateway_rate_mode === "min" ? "网关组最低倍率" : "网关组最高倍率";

  return (
    <section className="rounded-xl border border-border bg-background/80 p-3 sm:p-4">
      <div className="mb-4 flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
        <p className="text-sm font-semibold text-foreground">倍率同步</p>
      </div>

      {rateSync ? (
        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="min-w-36">源渠道</TableHead>
                <TableHead className="min-w-44">网关</TableHead>
                <TableHead className="min-w-32">最高倍率/最低倍率</TableHead>
                <TableHead className="min-w-28">倍率计算方式</TableHead>
                <TableHead className="min-w-44">倍率换算</TableHead>
                <TableHead className="min-w-56">倍率限制</TableHead>
                <TableHead className="min-w-44">分组倍率变量</TableHead>
                <TableHead className="min-w-28">权重/负载</TableHead>
                <TableHead className="min-w-24">并发</TableHead>
                <TableHead className="min-w-24">状态</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              <TableRow>
                <TableCell>
                  <Select
                    value={rateSync.source_kind}
                    onValueChange={(value) => {
                      if (value === "0") {
                        onChange((prev) => ({
                          ...prev,
                          gateway_rate_sync: null,
                        }));
                        return;
                      }
                      setRateSync({ source_kind: "gateway_group" });
                    }}
                  >
                    <SelectTrigger className="w-36">
                      <SelectValue placeholder="选择源渠道" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="0">请选择</SelectItem>
                      <SelectItem value="gateway_group">网关组</SelectItem>
                    </SelectContent>
                  </Select>
                </TableCell>
                <TableCell>
                  <Select
                    value={rateSync.gateway_group_id || "0"}
                    onValueChange={(value) =>
                      setRateSync({
                        gateway_group_id: value === "0" ? "" : value,
                      })
                    }
                  >
                    <SelectTrigger className="w-44">
                      <SelectValue
                        placeholder={groupsLoading ? "加载中..." : "选择网关"}
                      />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="0">请选择</SelectItem>
                      {gatewayGroups.map((group) => (
                        <SelectItem key={group.id} value={String(group.id)}>
                          {group.name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </TableCell>
                <TableCell>
                  <span className="whitespace-nowrap font-mono text-sm tabular-nums">
                    {routesLoading
                      ? "加载中..."
                      : `${formatRate(rateRange.max)} / ${formatRate(rateRange.min)}`}
                  </span>
                </TableCell>
                <TableCell>
                  <Select
                    value={rateSync.gateway_rate_mode}
                    onValueChange={(value) =>
                      setRateSync({
                        gateway_rate_mode: value === "min" ? "min" : "max",
                      })
                    }
                  >
                    <SelectTrigger className="w-28">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="max">最高倍率</SelectItem>
                      <SelectItem value="min">最低倍率</SelectItem>
                    </SelectContent>
                  </Select>
                </TableCell>
                <TableCell>
                  <div className="flex items-center gap-2">
                    <Select
                      value={rateSync.rate_convert_mode}
                      onValueChange={(value) =>
                        setRateSync({
                          rate_convert_mode:
                            value as UpstreamSyncRateConvertMode,
                        })
                      }
                    >
                      <SelectTrigger className="w-16">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="add">+</SelectItem>
                        <SelectItem value="subtract">-</SelectItem>
                        <SelectItem value="multiply">*</SelectItem>
                        <SelectItem value="divide">/</SelectItem>
                      </SelectContent>
                    </Select>
                    <Input
                      className="w-24"
                      type="text"
                      inputMode="decimal"
                      step="0.0001"
                      value={String(rateSync.rate_convert_value)}
                      onChange={(e) =>
                        setRateSync({
                          rate_convert_value: e.target.value,
                        })
                      }
                    />
                  </div>
                </TableCell>
                <TableCell>
                  <div className="flex items-center gap-2">
                    <Input
                      className="w-24"
                      type="text"
                      inputMode="decimal"
                      min="0"
                      step="0.0001"
                      value={rateSync.gateway_rate_max}
                      aria-label="最高调整倍率"
                      placeholder="最高"
                      onChange={(e) =>
                        setRateSync({
                          gateway_rate_max: e.target.value,
                        })
                      }
                    />
                    <Input
                      className="w-24"
                      type="text"
                      inputMode="decimal"
                      min="0"
                      step="0.0001"
                      value={rateSync.gateway_rate_min}
                      aria-label="最低调整倍率"
                      placeholder="最低"
                      onChange={(e) =>
                        setRateSync({
                          gateway_rate_min: e.target.value,
                        })
                      }
                    />
                  </div>
                </TableCell>
                <TableCell>
                  <div className="space-y-0.5 font-mono text-xs tabular-nums">
                    <p>{rateVariable}</p>
                    <p className="text-muted-foreground">
                      = {formatRate(calculatedRate)}
                    </p>
                  </div>
                </TableCell>
                <TableCell>
                  <Input
                    className="w-28"
                    type="number"
                    step="0.01"
                    value={String(rateSync.weight)}
                    onChange={(e) =>
                      setRateSync({ weight: num(e.target.value) })
                    }
                  />
                </TableCell>
                <TableCell>
                  <Input
                    className="w-24"
                    type="number"
                    value={String(rateSync.concurrency)}
                    onChange={(e) =>
                      setRateSync({ concurrency: num(e.target.value) })
                    }
                  />
                </TableCell>
                <TableCell>
                  <Select
                    value={rateSync.enabled ? "enabled" : "disabled"}
                    onValueChange={(value) =>
                      setRateSync({ enabled: value === "enabled" })
                    }
                  >
                    <SelectTrigger className="w-24">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="enabled">启用</SelectItem>
                      <SelectItem value="disabled">禁用</SelectItem>
                    </SelectContent>
                  </Select>
                </TableCell>
              </TableRow>
            </TableBody>
          </Table>
        </div>
      ) : null}
    </section>
  );
}

function applyGatewayRateOperation(
  base: number,
  operation: UpstreamSyncRateConvertMode,
  value: number,
) {
  switch (operation) {
    case "add":
      return base + value;
    case "subtract":
      return base - value;
    case "multiply":
      return base * value;
    case "divide":
      return value === 0 ? base : base / value;
    default:
      return base;
  }
}

function clampGatewayRate(value: number, minValue: number, maxValue: number) {
  const min = Math.max(0, minValue || 0);
  const max = Math.max(0, maxValue || 0);
  return Math.min(
    max > 0 ? max : Number.POSITIVE_INFINITY,
    Math.max(min, value),
  );
}

function Panel({
  title,
  description,
  action,
  children,
}: {
  title: string;
  description: string;
  action?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <section className="rounded-3xl border border-border/80 bg-muted/20 p-5">
      <div className="mb-5 flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div className="space-y-1">
          <p className="text-sm font-semibold text-foreground">{title}</p>
          <p className="max-w-2xl text-sm leading-6 text-muted-foreground">
            {description}
          </p>
        </div>
        {action}
      </div>
      {children}
    </section>
  );
}

function Field({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="space-y-2">
      <Label className="text-xs font-medium text-foreground">{label}</Label>
      {children}
    </div>
  );
}

function SwitchLine({
  id,
  label,
  description,
  checked,
  onCheckedChange,
}: {
  id: string;
  label: string;
  description: string;
  checked: boolean;
  onCheckedChange: (checked: boolean) => void;
}) {
  return (
    <div className="flex items-start justify-between gap-4 rounded-2xl border border-border bg-background/90 px-4 py-3">
      <div className="space-y-1">
        <Label htmlFor={id} className="text-sm font-medium text-foreground">
          {label}
        </Label>
        <p className="text-[11px] leading-5 text-muted-foreground">
          {description}
        </p>
      </div>
      <Switch id={id} checked={checked} onCheckedChange={onCheckedChange} />
    </div>
  );
}

function CompactSwitchLine({
  id,
  label,
  checked,
  onCheckedChange,
}: {
  id: string;
  label: string;
  checked: boolean;
  onCheckedChange: (checked: boolean) => void;
}) {
  return (
    <div className="flex h-9 items-center justify-between gap-3 rounded-md border border-border bg-background/90 px-3">
      <Label htmlFor={id} className="text-xs font-medium text-foreground">
        {label}
      </Label>
      <Switch id={id} checked={checked} onCheckedChange={onCheckedChange} />
    </div>
  );
}

function syncLogActionLabel(action: string) {
  const labels: Record<string, string> = {
    apply: "应用同步",
    delete_managed: "删除托管对象",
  };
  return labels[action] ?? action;
}

type ApplyMessageRow = {
  label: string;
  text: string;
  tone: "success" | "danger";
};

function ApplyMessageBox({ message }: { message: string }) {
  const rows = buildApplyMessageRows(message);

  return (
    <div className="mt-3 max-h-80 overflow-auto rounded-lg border border-border bg-muted/30 px-3 py-2.5 animate-in fade-in slide-in-from-top-1 transition-all duration-500 ease-out opacity-100">
      <ul className="space-y-1.5">
        {rows.map((row, index) => {
          const Icon = row.tone === "danger" ? XCircle : CheckCircle2;
          return (
            <li
              key={`${row.label}-${index}`}
              className="flex items-start gap-2 text-xs animate-in fade-in duration-200"
            >
              <Icon
                className={cn(
                  "mt-0.5 size-3.5 shrink-0",
                  row.tone === "danger" ? "text-danger" : "text-success",
                )}
                aria-hidden="true"
              />
              <span className="w-16 shrink-0 truncate text-[11px] text-muted-foreground">
                {row.label}
              </span>
              <div className="min-w-0 flex-1">
                <span
                  className={cn(
                    "block whitespace-pre-wrap break-words text-foreground",
                    row.tone === "danger" && "text-danger",
                  )}
                >
                  {row.text}
                </span>
              </div>
            </li>
          );
        })}
      </ul>
    </div>
  );
}

function buildApplyMessageRows(message: string): ApplyMessageRow[] {
  const rows: ApplyMessageRow[] = [];
  let section: ApplyMessageRow["tone"] = "success";

  for (const rawLine of message.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line) continue;

    if (line === "成功账号：" || line === "成功账号:") {
      section = "success";
      continue;
    }
    if (line === "失败账号：" || line === "失败账号:") {
      section = "danger";
      continue;
    }
    if (line === "变动账号：" || line === "变动账号:") {
      section = "success";
      continue;
    }

    if (line.startsWith("- ")) {
      const content = line.slice(2).trim();
      const parsed = parseApplyMessageLine(content);
      rows.push({
        label: parsed.label || (section === "danger" ? "失败" : "明细"),
        text: parsed.text || content,
        tone: section,
      });
      continue;
    }

    const parsed = parseApplyMessageLine(line);
    rows.push({
      label: parsed.label || "结果",
      text: parsed.text || line,
      tone: line.includes("failed") ? "danger" : "success",
    });
  }

  return rows.length > 0
    ? rows
    : [{ label: "错误", text: message, tone: "danger" }];
}

function parseApplyMessageLine(line: string) {
  const match = line.match(/^([^：:]+)[：:]\s*(.*)$/);
  if (!match) return { label: "", text: line };

  const labels: Record<string, string> = {
    同步分组: "分组",
    目标上游: "上游",
    目标分组: "目标",
    排序方向: "排序",
  };
  const label = match[1].trim();
  return {
    label: labels[label] ?? label,
    text: match[2].trim(),
  };
}

function StatusBadge({ status }: { status: string }) {
  const labelMap: Record<string, string> = {
    enabled: "启用",
    disabled: "禁用",
    ok: "正常",
    failed: "失败",
    active: "启用",
    inactive: "停用",
    missing: "缺失",
    applied: "已应用",
    blocked_missing_group: "分组缺失",
    cron_enabled: "Cron 启用",
    cron_disabled: "Cron 停用",
  };
  const isDanger =
    status === "failed" ||
    status === "missing" ||
    status === "blocked_missing_group" ||
    status === "inactive";
  if (isDanger) {
    return (
      <Badge
        variant="outline"
        className="border-transparent bg-danger/10 text-danger"
      >
        {labelMap[status] ?? status}
      </Badge>
    );
  }
  const tone =
    status === "disabled" || status === "cron_disabled"
      ? "bg-slate-100 text-slate-500"
      : "bg-emerald-50 text-emerald-700";
  return (
    <Badge variant="outline" className={cn("border-transparent", tone)}>
      {labelMap[status] ?? status}
    </Badge>
  );
}

function EmptyBox({ text }: { text: string }) {
  return (
    <div className="rounded-2xl border border-dashed border-border bg-background/70 px-4 py-6 text-sm text-muted-foreground">
      {text}
    </div>
  );
}

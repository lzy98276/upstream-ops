import { Check, Copy, ExternalLink, LoaderCircle, RefreshCw } from "lucide-react"
import { useMemo, useState } from "react"
import { toast } from "sonner"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { copyText } from "@/lib/clipboard"
import type { AppVersion } from "@/lib/api-types"

interface UpdateDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  version: AppVersion | undefined
  checking: boolean
  onCheck: () => Promise<void>
}

function formatPublishedAt(value: string | undefined) {
  if (!value) return null
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return null
  return new Intl.DateTimeFormat("zh-CN", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date)
}

export function UpdateDialog({
  open,
  onOpenChange,
  version,
  checking,
  onCheck,
}: UpdateDialogProps) {
  const [copied, setCopied] = useState<"posix" | "powershell" | null>(null)
  const latest = version?.latest_version?.trim() || "latest"
  const publishedAt = formatPublishedAt(version?.published_at)
  const releaseURL = version?.release_url?.trim() || version?.repo_url?.trim()

  const commands = useMemo(
    () => ({
      posix: `export IMAGE_TAG=${latest}\ndocker compose pull app\ndocker compose up -d app`,
      powershell: `$env:IMAGE_TAG = "${latest}"\ndocker compose pull app\ndocker compose up -d app`,
    }),
    [latest],
  )

  async function handleCopy(target: "posix" | "powershell") {
    try {
      await copyText(commands[target])
      setCopied(target)
      toast.success("更新命令已复制")
      window.setTimeout(() => setCopied(null), 2000)
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "复制失败")
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>发现新版本 {latest}</DialogTitle>
          <DialogDescription>
            当前版本 v{version?.version || "-"}
            {publishedAt ? `，发布于 ${publishedAt}` : ""}
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          {version?.release_notes ? (
            <section className="space-y-1.5">
              <h3 className="text-sm font-medium">发布说明</h3>
              <pre className="max-h-48 overflow-auto whitespace-pre-wrap rounded-md border bg-muted/40 p-3 font-sans text-sm leading-6 text-muted-foreground">
                {version.release_notes}
              </pre>
            </section>
          ) : null}

          <section className="space-y-2">
            <h3 className="text-sm font-medium">更新命令</h3>
            <Tabs defaultValue="posix">
              <TabsList>
                <TabsTrigger value="posix">Linux / macOS</TabsTrigger>
                <TabsTrigger value="powershell">Windows PowerShell</TabsTrigger>
              </TabsList>
              <TabsContent value="posix" className="mt-2">
                <CommandBlock
                  command={commands.posix}
                  copied={copied === "posix"}
                  onCopy={() => void handleCopy("posix")}
                />
              </TabsContent>
              <TabsContent value="powershell" className="mt-2">
                <CommandBlock
                  command={commands.powershell}
                  copied={copied === "powershell"}
                  onCopy={() => void handleCopy("powershell")}
                />
              </TabsContent>
            </Tabs>
          </section>

          <p className="text-sm leading-6 text-muted-foreground">
            在包含 docker-compose.yml 的部署目录执行。更新会重建应用容器，data/ 中的配置和数据会保留。
          </p>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => void onCheck()} disabled={checking}>
            {checking ? <LoaderCircle className="animate-spin" /> : <RefreshCw />}
            检查更新
          </Button>
          {releaseURL ? (
            <Button asChild>
              <a href={releaseURL} target="_blank" rel="noopener noreferrer">
                查看发布页
                <ExternalLink />
              </a>
            </Button>
          ) : null}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function CommandBlock({
  command,
  copied,
  onCopy,
}: {
  command: string
  copied: boolean
  onCopy: () => void
}) {
  return (
    <div className="relative">
      <pre className="overflow-x-auto rounded-md border bg-muted/40 p-3 pr-24 font-mono text-xs leading-6 text-foreground">
        {command}
      </pre>
      <Button
        type="button"
        variant="secondary"
        size="sm"
        onClick={onCopy}
        className="absolute top-2 right-2"
      >
        {copied ? <Check /> : <Copy />}
        {copied ? "已复制" : "复制"}
      </Button>
    </div>
  )
}

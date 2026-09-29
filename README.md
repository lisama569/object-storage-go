# object-storage-go

一个**仅依赖本地磁盘**的内容寻址（content-addressable）、带多版本的对象存储。
纯 Go 标准库实现，无网络、无外部服务、无凭据。它面向"单目录、单写者进程 +
进程内多 goroutine"的场景，目标是让**并发写入、版本变更与空间回收在进程异常
中断后仍然一致、可复现、可解释**。

- 模块路径：`github.com/lisama569/object-storage-go`
- 包：`objectstore`（目录 `objectstore/`）
- Go 版本：见 `go.mod`

---

## 1. 快速开始

```go
import (
    "context"
    "io"
    "strings"

    "github.com/lisama569/object-storage-go/objectstore"
)

ctx := context.Background()
store, err := objectstore.Open(ctx, "/var/lib/myobj", objectstore.Options{})
if err != nil { /* ... */ }
defer store.Close()

// 写入：相同内容去重，不产生第二个数据副本
vi, err := store.Put(ctx, "photos/cat.png", strings.NewReader("...bytes..."))

// 读取当前版本（读取时自动做完整性校验）
rc, info, err := store.Get(ctx, "photos/cat.png")
defer rc.Close()
data, _ := io.ReadAll(rc)

// —— 版本保留：把某版本锁到截止时间（或永久），扛重启 ——
_, _ = store.PutRetention(ctx, "photos/cat.png", info.Version,
    objectstore.RetainUntil(time.Now().AddDate(1, 0, 0)))
st, _ := store.RetentionOf(ctx, "photos/cat.png", info.Version)
fmt.Println(st.Retained, st.Until)
// store.ReleaseRetention(ctx, "photos/cat.png", info.Version) // 提前解除

// —— 容量水位：占用超 1 GiB 时按固定优先级自动腾空间 ——
s2, _ := objectstore.Open(ctx, "/var/lib/myobj", objectstore.Options{
    MaxPhysicalBytes: 1 << 30,
})
report, stats, err := s2.SweepToWatermark(ctx)
if errors.Is(err, objectstore.ErrWatermark) {
    // 能腾的都腾了仍超限：受保护数据没被动，按业务处理
    _ = report
}
```

---

## 2. 磁盘布局

```
<root>/
├── .lock                     # flock 进程级互斥锁
├── manifest.json             # 已提交的元数据（原子替换）
├── blobs/
│   └── <aa>/<full-sha256>    # 不可变数据文件，按摘要前两位分片
└── tmp/
    └── stage-*               # 正在写入的暂存文件（崩溃后可安全清理）
```

- **数据文件不可变**：blob 安装后永不原地修改，避免"读一半被改写"。
- **元数据是单一文档** `manifest.json`，采用"写临时文件 → fsync → rename →
  fsync 目录"的原子提交，崩溃后要么是完整旧版、要么是完整新版，绝不会出现
  半个 JSON。

---

## 3. 对象标识与去重语义

- 对象内容的标识是其字节的 SHA-256，摘要表示为 `sha256:<64 位十六进制>`。
- 标识**只取决于字节本身**：与写入顺序、`Read` 返回的分块边界、分块大小、
  写入并发时序都无关；同一进程多次运行、不同进程，结果都一致。
- 数据先以流式写入 `tmp/stage-*`，同时计算哈希；随后 rename 到
  `blobs/<aa>/<digest>`。
- **同对象去重**：若某对象的当前活跃版本与新写入字节完全相同，则**不分配新
  版本号、也不产生新的数据副本**，直接返回原版本描述符。
- **跨对象去重**：不同对象写入相同内容会各自产生逻辑版本，但物理上共享同一
  个 blob（引用计数由元数据推导）。
- 安装 blob 前会创建（必要时）分片目录并对 `blobs/` 目录 fsync，随后 fsync
  数据文件与所在分片目录，保证数据在元数据引用它之前已落盘。

### 完整性校验

- 默认开启 `VerifyOnRead`（零值 `Options{}` 即开启）：读取时对字节再做一次
  SHA-256，读到 EOF 时同时校验**长度与摘要**；任何不一致返回
  `ErrDigestMismatch`，绝不把损坏/截断数据交给调用方。
- `Verify(ctx)` 会重新哈希所有"仍被可读版本引用"的 blob，报告损坏/缺失。

---

## 4. 版本、删除、恢复规则

每次写入不同内容都会得到一个**对象内单调递增**的版本号（从 1 开始）。

每个版本有三种状态：

| 状态 | 含义 | 数据是否在盘 | 能否读 |
|------|------|--------------|--------|
| `Active` | 正常可读版本 | 是 | 是 |
| `Deleted` | 软删除（墓碑），可恢复 | 是 | `Get` 返回 `ErrDeleted` |
| `Collected` | 已物理回收，仅保留元数据 | 否 | 返回 `ErrCollected`（不可恢复） |

操作语义：

- `Put(name, r)`：新内容生成新版本并成为 current；与 current 完全相同则去重。
- `Get(name)`：读当前版本。`GetVersion(name, v)`：读指定版本。
- `Delete(name)`：对象级软删除。记录被隐藏的具体版本（`deleted_current`），
  将该版本置为 `Deleted`，`current` 指针归零（墓碑）。
- `Restore(name)`：**精确撤销** `Delete`——把 `deleted_current` 指向的版本置
  回 `Active` 并恢复为 current；不会误选其它版本。
- `DeleteVersion / RestoreVersion`：对**非当前**版本做独立的软删/恢复，不影响
  current。
- 对已删除对象再次 `Put`：以新的、更大的版本号"复活"，旧版本历史保留。
- `PurgeVersion(name, v)`：物理回收**非当前**版本。
  - current 版本永远不能被 purge（返回 `ErrCurrent`）；
  - 只有当该版本是某 blob 的**最后一个非 Collected 引用**时才删除物理文件，
    仍被任何活跃/软删版本共享的 blob 一定保留；
  - 版本记录保留为 `Collected`，之后读取明确返回 `ErrCollected`；
  - 操作幂等。

### 读取被拒时的原因是可区分的

调用方用 `errors.Is` 判定：

| 错误 | 场景 |
|------|------|
| `ErrNotFound` | 对象/版本从未存在 |
| `ErrDeleted` | 版本存在但被软删，数据仍在、可恢复 |
| `ErrCollected` | 版本已被物理回收，不可恢复 |
| `ErrCurrent` | 试图 purge 当前版本 |
| `ErrRetained` | 版本处于有效保留期内，禁止物理回收；软删但被保留的版本读取时会**同时**命中 `ErrDeleted` 与 `ErrRetained` |
| `ErrWatermark` | 所有可回收数据都已腾完，占用仍高于容量水位 |
| `ErrDigestMismatch` | 读到的数据与内容地址不符（损坏/截断） |
| `ErrLocked` | 另一进程已打开该目录 |
| `ErrName` | 对象名非法（空串、含 NUL、超长） |

不会出现"静默返回旧内容/部分内容"。

---

## 4b. 版本保留（Retention / Legal Hold）

可以给**指定对象的指定版本**加一道"保留锁"，常用于审计留证或给下游长周期
回放。保留状态与版本状态一起写进同一份原子提交的 `manifest.json`，因此：

- `PutRetention` 一旦成功返回，保护**已经落盘**，进程重启 / 被杀都不会让它
  凭空消失；
- 保留不影响软删（`Delete/DeleteVersion` 仍可隐藏版本），但只要保留有效，
  该版本的字节就**不能被任何回收动作删除**（`PurgeVersion`、`Sweep`、容量
  水位回收都一样）。

### 保留策略与 API

| 操作 | 语义 |
|------|------|
| `RetainUntil(t)` | 构造一个**带截止时间**的保留（到 `t` 失效） |
| `RetainPermanent()` | 构造**永久保留**，只能显式解除 |
| `PutRetention(name, v, r)` | 设置 / 续期。幂等且可安全重复：只在新要求**更强**（更晚的截止时间或永久）时才替换，重复设置同样或更弱的要求不会缩短既有保留、无副作用 |
| `ExtendRetention(name, v, r)` | 严格"加长"既有保留；已经是永久或已足够长时是幂等 no-op；对没有有效保留的版本返回 `ErrRetained`（用 `PutRetention` 重新上锁） |
| `ReleaseRetention(name, v)` | **提前解除**保留。解除本身落盘（记录 `ReleasedAt`），重复解除是幂等 no-op |
| `RetentionOf(name, v)` | 查某一个版本当前是否被保留、是否永久、截止时间、解除时间 |
| `Retentions(name)` | 列出某对象**所有版本**的保留状态（按版本号排序） |

判断规则（`now` 为当前时刻）：

- 永久保留：永远 `Retained == true`，直到显式解除；
- 期限保留：`now < 截止时间` 时有效；到期后自动失效，**无需手动操作**；
- 已解除 / 已到期：不再保护字节，但字段保留，便于解释回收顺序与审计。

到期或解除后可以再次 `PutRetention` 重新上锁。

### 被保留时读取的拒绝原因不被搅乱

- 正常可读的保留版本：照常读取，`VersionInfo.Retained / Permanent /
  RetainUntil` 会反映保留状态；
- **软删但仍在保留期内**的版本：读取返回的 error 同时
  `errors.Is(err, ErrDeleted)` **和** `errors.Is(err, ErrRetained)`——
  主体仍是"已软删"，并额外可识别"还在保留期"。它绝不会被报成
  `ErrNotFound`（从未存在）或 `ErrCollected`（已回收）；
- 已解除 / 已到期后被回收：照常返回 `ErrCollected`。

---

## 5b. 容量水位（Capacity Watermark）

给存储目录设一个占用上限，占用超限时回收会自动把空间腾到水位以下。

- 配置项：`Options.MaxPhysicalBytes`（字节，`0` = 不设水位，默认）。
- 计量口径：`Stats.OnDiskBytes` = `blobs/` 下**所有**已安装数据文件的字节
  总和（被引用数据、软删数据、无主 orphan 都计入）。水位按这个值判定。
- 入口：
  - `SweepToWatermark(ctx)`：返回 `(WatermarkReport, Stats, error)`；
  - 设了 `MaxPhysicalBytes` 时，普通 `Sweep(ctx)` 在完成原有保守/激进回收后
    会**继续**执行同样的水位回收（未设水位时行为与旧版本完全一致）。

### 确定的腾挪优先级（不随机删数据）

回收严格按以下顺序，只动"本来就允许被牺牲"的数据：

1. **无主 orphan blob**（崩溃写入残留、`PurgeVersion` 释放后仅剩的文件、
   上一次回收"元数据已提交、文件未删"留下的文件）——零代价先清；
2. 保留被**显式解除**（released）的软删版本；
3. 保留**已过期**（expired）的软删版本；
4. **从未设置过保留**的普通软删版本。

同一优先级内：先回收**最早创建**的版本；再用 digest 字符串兜底排序，
因此结果确定、与 map 遍历顺序无关。一个 blob 只有在它**完全没有**
`Active` 引用、也没有任何有效保留引用时才会被回收；与活数据共享同一 blob
的软删版本绝不会被删。所有共享该 blob 的软删版本会一起转为 `Collected`
（物理上只有一个文件）。

### 达限即停 + 无法达标时明确报错

- 一旦 `OnDiskBytes <= MaxPhysicalBytes` 立即停止，低优先级的软删版本继续
  保留、仍可 `Restore`；
- 把 orphan 与上面三类软删数据都腾完仍高于水位时，返回包裹
  `ErrWatermark` 的错误（`WatermarkReport` 里带回收前后的占用、回收字节数、
  回收版本数等），**绝不硬删、绝不牺牲保留版本或当前能正常读到的数据**。
  - 当前可读版本（`Active`）永远不参与水位回收；
  - 有效保留（永久 / 未到期）的版本永远不参与，哪怕它是软删状态。

`WatermarkReport` 字段：`Watermark / OnDiskBefore / OnDiskAfter /
OrphanBlobsRemoved / VersionsCollected / BytesReclaimed / BelowWatermark`。

### 与既有 `SweepDeletedVersions` 的关系

- 两者都遵守保留：开启 `SweepDeletedVersions` 时，仍在保留期内的软删版本
  **不会**被普通 `Sweep` 回收，只有已解除/已到期的才会；
- 即使不开启 `SweepDeletedVersions`，只要设置了水位且确实超限，水位回收仍
  会按上面的固定顺序回收（已解除/已过期/普通）软删版本——这是水位能力的
  一部分；不设水位、不开启激进回收时，旧的"保守"语义完全不变。

---

## 5. 空间回收（GC）与用量统计

- `Stats(ctx)`：不做任何删除，返回
  - `LogicalBytes`：每个**可读版本**的字节数之和（共享内容按版本计多次）；
  - `PhysicalBytes`：所有被引用 blob 的去重字节数（共享只计一次）；
  - `OnDiskBytes`：`blobs/` 下全部已安装文件的字节总和（容量水位据此判定）；
  - `RetainedBytes`：被有效保留覆盖的去重字节数（任何情况下都不会被回收）；
  - `ReferencedBlobs / OrphanBlobs / ReclaimableBytes` 等。
- `Sweep(ctx)`：幂等回收，可重复执行、可在崩溃后重跑：
  - 默认（保守策略）只删除**真正无引用**的 blob（含崩溃写入残留、已被
    `PurgeVersion` 释放的 blob）；
  - 绝不删除仍被任意 `Active`/`Deleted` 版本引用的数据；
  - 开启 `SweepDeletedVersions` 或设置了 `MaxPhysicalBytes` 时才可能回收软删
    数据，且始终尊重有效保留（见 4b / 5b）。
- 引用集合由当前 `manifest.json` 全量推导，不依赖易失的内存计数，因此重启后
  仍正确。

### 回收的崩溃安全顺序（关键）

破坏性策略下，顺序是**先提交元数据，再删数据文件**：

1. 把"仅被软删版本引用"的版本在 manifest 中标记为 `Collected` 并**原子提交**
   （水位回收按 digest 逐个提交，每次提交就是一个完整的新 manifest）；
2. 然后才删除物理 blob。

- 在第 1 步前崩溃：什么都没发生；
- 在第 1 步与第 2 步之间崩溃：元数据已自洽（版本显示已回收），残留的只是
  无引用 blob，下一次 `Sweep`/`SweepToWatermark` 会清掉；
- **任何时刻都不存在"元数据引用了已删 blob"的悬空状态**，也不会丢数据。
- `Sweep` / `SweepToWatermark` 可随意重复执行（包括中断、取消、重启后重跑），
  结果收敛、稳定，不会"越跑越乱"。
- `ctx` 在两个 digest 之间被取消时，已经提交的回收保持有效（本就是安全状态），
  未处理的候选留给下一次；元数据始终自洽。

---

## 6. 并发一致性

- **进程内**：`Store` 用一把互斥锁串行化所有变更；`manifest` 的原子提交保证
  多 goroutine 下版本号不丢失、不重复，读永远看到完整的旧版本或完整的新版本。
- **跨进程**：打开目录时对 `<root>/.lock` 取非阻塞 `flock(2)` 独占锁。
  第二进程 `Open` 立即得到 `ErrLocked`。`flock` 绑定打开的文件描述符，进程
  死亡（含 `SIGKILL`/断电后由 OS 回收）会自动释放，不会留下永久死锁。
- 读取路径不做"读到一半引用被清空"的操作：GC 只删无引用 blob，而当前版本必然
  有引用。
- 保留的加锁/续期/解除与普通写入、`Sweep`/`SweepToWatermark` 走同一把互斥锁与
  同一份原子 manifest，因此与并发写入同时发生时也不会出现"保护状态没落盘就
  误删"或"元数据说有、数据已没"的情况。

---

## 7. 崩溃恢复

- `Open` 在加锁、加载 manifest 后会自动执行一次恢复：
  1. 删除 `tmp/stage-*`（未安装的暂存数据，任何已提交元数据都不会引用它）；
  2. 删除 `.manifest-*.tmp`（未完成的元数据提交，提交版不受影响）；
  3. 遍历元数据，检查每个仍可读版本引用的 blob 是否存在，缺失则在
     `RecoveryReport.MissingBlobs` 中显式报告。
- 也可显式调用 `Recover(ctx)` 得到同样的报告。
- 不变式（invariants）：
  - 提交到 manifest 的每个引用，其 blob 必先已持久化；
  - 元数据提交是原子的；
  - 故重启后**无悬空引用、无半对象、无数据丢失**；
  - 崩溃写入的残缺数据停留在 `tmp/`，永远不会被当成有效对象读出。

---

## 8. 可调参数（`Options`）

| 字段 | 默认 | 作用 |
|------|------|------|
| `DisableReadVerification` | `false`（即默认校验） | 置 `true` 关闭读取时再哈希校验（仅在你用其它方式保证完整性时使用） |
| `SweepDeletedVersions` | `false`（保守） | 置 `true` 后 `Sweep` 会把"仅被软删版本引用"的数据也物理回收，并把这些版本转为不可逆的 `Collected`（有效保留的版本除外） |
| `MaxPhysicalBytes` | `0`（不设水位） | 容量高水位（字节，按 `Stats.OnDiskBytes` 判定）。`>0` 时 `SweepToWatermark` 以及 `Sweep` 会先清 orphan，再按"已解除→已过期→普通软删"的固定顺序腾空间到水位以下；腾不完返回 `ErrWatermark`，绝不牺牲保留版本或活数据 |

### 对旧行为的兼容范围

- `Options{}` 零值的语义与旧版本**完全一致**：默认读取校验、保守 GC、
  **不设容量水位**，软删数据不会被自动回收；
- 新增的 manifest 字段（`retain_until` / `retain_released_at`）均为
  `omitempty`，旧目录直接打开即可，无需迁移；新版本写出的 manifest 旧版本
  也能正常忽略这些字段；
- 新增的 `Stats` 字段（`OnDiskBytes` / `RetainedBytes`）是附加字段，既有
  消费方不受影响；
- 既有错误判定保持不变；新增 `ErrRetained` / `ErrWatermark` 只在显式使用新
  能力时出现。普通软删读取**不会**额外命中 `ErrRetained`（只有真正处于保留
  期时才会）。

其余常量（如流式拷贝缓冲大小 `stageBufSize`、对象名长度上限）在源码顶部定义。

---

## 9. 本地验证

无需网络或外部依赖，直接用仓库既有的 Go 方式：

```bash
go build ./...
go vet ./...
go test ./... -v
go test -race -count=5 ./...   # 竞态检测 + 重复运行，验证稳定性
```

测试覆盖（在原有全部用例之上新增）：

**保留（Retention）**

- 保留的设置、按版本/按对象查询、`VersionInfo` 暴露保留状态、重复设置不缩短
  既有保留、更强设置可续期（`TestRetentionSetQueryIdempotent`）；
- 永久保留不随时间失效、`ExtendRetention` 单调、空策略被拒
  （`TestRetentionPermanentAndExtend`）；
- 提前解除（幂等、落盘、可重新上锁）、到期自动失效、解除/到期后才能 purge，
  且解除与到期状态都能扛重启（`TestRetentionReleaseAndExpiry`）；
- 保留期内 `PurgeVersion`/激进 `Sweep` 被 `ErrRetained` 拒绝；软删但被保留的
  版本读取同时命中 `ErrDeleted`+`ErrRetained`、绝不报 `ErrNotFound`；解除后
  才能被回收（`TestRetentionBlocksPurgeAndSoftDeletedRead`、
  `TestRetentionOnCurrentVersionBlocksPurge`）。

**容量水位（Watermark）**

- 水位以下不动作、orphan 优先于软删数据被清（`TestWatermarkBelowIsNoop`、
  `TestWatermarkOrphansFirst`）；
- 固定优先级（已释放 > 已过期 > 普通）、活数据与保留数据无论如何不被牺牲、
  无法达标时明确返回 `ErrWatermark`（`TestWatermarkPriorityAndProtection`、
  `TestWatermarkSharedBlobSafety`）；
- 达限即停、低优先级软删版本保持可恢复（`TestWatermarkStopsAsSoonAsBelow`）；
- **回收中断后重启继续完成**、孤儿文件被清、结果收敛
  （`TestWatermarkInterruptedRestart`）；
- `ctx` 中途取消不留悬空引用、后续无取消回收收敛
  （`TestWatermarkCancellationStaysConsistent`）；
- **重复回收幂等稳定**（`TestWatermarkRepeatedIsStable`）；
- 普通 `Sweep` 也会自动执行水位回收并如实报错
  （`TestSweepAutoEnforcesWatermark`）；
- **并发写入 + 加保留 + 回收同时发生**，无悬空引用、保留数据始终可读
  （`TestWatermarkSweepConcurrentWithWriters`，`-race` 反复验证）。

以及原有的：基础读写、完整性校验、损坏检测、去重、版本/软删/恢复、三类拒绝
原因、跨进程锁、写入中断恢复、保守/激进回收、purge 共享 blob 安全等，均无
回归。

每个测试都会用 `t.Logf` 打印**输入、判定依据与恢复结果**，用 `-v` 可查看。
所有数据都在 `t.TempDir()` 中，用例彼此隔离、可重复运行、结论确定。
保留到期相关用例通过注入测试时钟模拟时间流逝，无需真实等待。

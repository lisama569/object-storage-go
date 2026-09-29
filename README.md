# object-storage-go

一个**仅依赖本地磁盘**的内容寻址（content-addressable）、带多版本的对象存储。
纯 Go 标准库实现，无网络、无外部服务、无凭据。它面向"单目录、单写者进程 +
进程内多 goroutine"的场景，目标是让**并发写入、版本变更、保留（legal hold）、
容量水位回收在进程异常中断后仍然一致、可复现、可解释**。

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
| `ErrRetained` | 版本处于有效保留期（或永久保留），删除/回收被拒绝 |
| `ErrWatermarkExceeded` | 水位回收已把所有"可安全牺牲"的数据清完，占用仍高于水位线 |
| `ErrDigestMismatch` | 读到的数据与内容地址不符（损坏/截断） |
| `ErrLocked` | 另一进程已打开该目录 |
| `ErrName` | 对象名非法（空串、含 NUL、超长） |

不会出现"静默返回旧内容/部分内容"。

**加入保留功能后，拒绝原因仍然严格可区分**：被保留的软删版本读取仍是
`ErrDeleted`（数据在盘、状态是删除）；保留只拦截"让数据可能消失"的操作并返回
`ErrRetained`，绝不拦截读；解除保留并被回收后读才变成 `ErrCollected`，不会被
降级报成 `ErrNotFound`。

---

## 5. 保留（Retention / Legal Hold）

可以给**指定对象的指定版本**加一道保留，使其在保留解除或到期前**不能被任何
回收动作清掉**——面向审计留档、下游长周期回放等场景。

### API 与语义

| 方法 | 语义 |
|------|------|
| `Retain(ctx, name, v, until)` | 给版本 `v` 设置保留。`until` 为零值 `time.Time{}` 表示**永久保留**；否则是一个 **UTC 截止时刻**（必须晚于当前时间）。 |
| `ReleaseRetention(ctx, name, v)` | **提前解除**保留（永久/限期均可）。本来就没有有效保留时是空操作。 |
| `Retention(ctx, name, v)` | 查询单个版本当前的保留状态。 |
| `ListRetained(ctx, name)` | 列出某对象当前所有**正在生效**的保留，按版本号排序；已过期的不出现。 |

返回的 `RetentionInfo` 字段：

- `Retained`：此刻是否受保护；
- `Permanent`：是否永久保留（`until` 为零）；
- `Until`：限期保留的截止时刻（永久/无保留时为零值）。

`VersionInfo`（`Versions`/`Put` 等返回）也同步携带 `Retained /
PermanentHold / RetainUntil`。

关键规则：

- **只增不减（extend-only）**：`Retain` 只会加强保留——永久保留不会被限期请求
  缩短；限期保留的截止时间只会取"现有截止时间与请求截止时间的较晚者"。
  因此**对同一版本重复设置相同（或更弱）的保留要求不会产生任何副作用**，也不会
  多产生一次 manifest 提交。
- **到期自动失效**：截止时刻到达（`now >= Until`）后保留即不再生效，无需手工
  清理；记录保留在元数据中但查询返回 `Retained=false`。
- **可以保留软删版本**：对已经 `Deleted` 的版本加保留也是允许的（典型的"审计
  快照"）。此时读取仍按软删返回 `ErrDeleted`，但它的字节任何 Sweep 都不能删；
  `RestoreVersion` 后即可正常读。
- **保留拦截什么**：有效期内，`Delete`（current 被保留时）、`DeleteVersion`、
  `PurgeVersion`、以及保守/激进/水位三种 Sweep 的回收，全部被拒绝或跳过；
  破坏性操作显式返回 `ErrRetained`。**读永远不受保留影响。**
- **不能给已回收版本加保留**：`StateCollected` 的版本返回 `ErrCollected`；
  未知对象/版本返回 `ErrNotFound`。
- **崩溃安全与重启持久化**：保留状态是 `manifest.json` 里每个版本的字段
  （`retain_permanent` / `retain_until`），沿用"写临时文件 → fsync → rename →
  fsync 目录"的原子提交。保留/解除在提交完成前不生效，提交完成后**进程重启保护
  不消失**；提交失败会把内存状态回滚，绝不会出现"保护丢了但调用方以为在"。
- **fail-closed**：万一磁盘上的截止时间字段无法解析，按"仍在保留"处理，宁可
  多保护也不误删。

---

## 6. 容量水位（Capacity Watermark）

### 配置

`Options.WatermarkBytes`（int64，字节，**默认 0 = 关闭**）。设为正数后，每次
`Sweep(ctx)` 除完成常规回收外，还会把**已提交 blob 文件的总大小**
（`Stats.DiskBytes`，含无引用 blob）压到水位线以下。设为负数在 `Open` 时即
报错。

### 回收优先级（确定、可复现，绝不随机挑数据）

1. **先清真正无引用的 blob**（崩溃写入残留、已被 `PurgeVersion` 释放的 blob
   等）——这是常规 Sweep 的第 2 阶段，任何水位配置下都先做；
2. 还不够时，按**最老优先**回收"软删且当前**不受保留**"的版本：
   - 依据各候选版本的创建时间（`Created`）从旧到新，digest 作为确定性
     tiebreak；重复执行选出的牺牲者完全相同。
   - 每个候选组（同一 digest 的所有软删无保留版本）遵循
     **先原子提交元数据（标 `Collected`）、再删 blob** 的崩溃安全顺序；
   - **一旦占用回到水位线立即停止**，不会多删"更新"的软删数据。
3. 无论空间多紧张，两类数据**永远不会被牺牲**：
   - 处于有效保留期或永久保留的版本（以及与它们共享同一 blob 的任何版本）；
   - 当前还能正常读到的数据（`Active` 版本，含 current）。
4. 如果该腾的都腾完了占用仍高于水位线，`Sweep` 显式返回
   **`ErrWatermarkExceeded`**（可用 `errors.Is` 判定），返回的 `Stats` 里
   `OverWatermark=true`。**绝不硬删受保护数据，也不破坏一致性来"凑"水位。**

`SweepDeletedVersions=true` 与水位可叠加：激进策略仍会先收集"全部仅被软删、
不受保留"引用的数据，随后再做水位判定；保留对两者同时生效。

水位回收**幂等且可重放**：中途进程被杀、`context` 取消、或重复执行，状态都
收敛——已提交 `Collected` 但没来得及删的 blob 会在下一次 `Sweep` 作为无引用
blob 被清掉，任何时刻都不存在悬空引用。`context` 在边界处检查：已取消时一次
提交都不会发生。

---

## 7. 空间回收（GC）与用量统计

- `Stats(ctx)`：不做任何删除，返回
  - `LogicalBytes`：每个**可读版本**的字节数之和（共享内容按版本计多次）；
  - `PhysicalBytes`：所有被引用 blob 的去重字节数（共享只计一次）；
  - `DiskBytes`：盘上**全部已提交 blob** 的字节数（含无引用 blob），即水位线
    的比较基准；
  - `RetainedVersions`：当前处于有效保留的版本数；
  - `WatermarkBytes / OverWatermark`：当前配置的水位线、以及此刻是否超标；
  - `ReferencedBlobs / OrphanBlobs / ReclaimableBytes` 等（配置水位或激进策略时，
    `ReclaimableBytes` 会把"软删且不受保留"的字节也计入，受保留的不计入）。
- `Sweep(ctx)`：幂等回收，可重复执行、可在崩溃后重跑：
  - 默认（保守策略）只删除**真正无引用**的 blob（含崩溃写入残留、已被
    `PurgeVersion` 释放的 blob）；
  - 绝不删除仍被任意 `Active`/`Deleted` 版本引用的数据；
  - **任何处于有效保留期（或永久保留）的版本引用的 blob 一律不删**，即使该
    blob 同时只被软删版本引用；
  - 配置了 `WatermarkBytes` 时额外执行水位回收（语义见第 6 节）。
- 引用集合由当前 `manifest.json` 全量推导（包括保留状态），不依赖易失的内存
  计数，因此重启后仍正确。

### 回收的崩溃安全顺序（关键）

破坏性策略下，顺序是**先提交元数据，再删数据文件**：

1. 把"仅被软删版本引用"的版本在 manifest 中标记为 `Collected` 并**原子提交**；
2. 然后才删除物理 blob。

- 在第 1 步前崩溃：什么都没发生；
- 在第 1 步与第 2 步之间崩溃：元数据已自洽（版本显示已回收），残留的只是
  无引用 blob，下一次 `Sweep` 会清掉；
- **任何时刻都不存在"元数据引用了已删 blob"的悬空状态**，也不会丢数据。
- `Sweep` 可随意重复执行，结果收敛、稳定。

---

## 8. 并发一致性

- **进程内**：`Store` 用一把互斥锁串行化所有变更；`manifest` 的原子提交保证
  多 goroutine 下版本号不丢失、不重复，读永远看到完整的旧版本或完整的新版本。
- **跨进程**：打开目录时对 `<root>/.lock` 取非阻塞 `flock(2)` 独占锁。
  第二进程 `Open` 立即得到 `ErrLocked`。`flock` 绑定打开的文件描述符，进程
  死亡（含 `SIGKILL`/断电后由 OS 回收）会自动释放，不会留下永久死锁。
- 读取路径不做"读到一半引用被清空"的操作：GC 只删无引用 blob，而当前版本必然
  有引用。

---

## 9. 崩溃恢复

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

## 10. 可调参数（`Options`）

| 字段 | 默认 | 作用 |
|------|------|------|
| `DisableReadVerification` | `false`（即默认校验） | 置 `true` 关闭读取时再哈希校验（仅在你用其它方式保证完整性时使用） |
| `SweepDeletedVersions` | `false`（保守） | 置 `true` 后 `Sweep` 会把"仅被软删版本引用且不受保留"的数据也物理回收，并把这些版本转为不可逆的 `Collected` |
| `WatermarkBytes` | `0`（关闭） | 正数启用容量水位：`Sweep` 自动按"先无引用 blob、再最老的无保留软删数据"的确定顺序把 `DiskBytes` 压到该值以下；`Active` 与受保留数据绝不牺牲，压不下去时返回 `ErrWatermarkExceeded`。负数在 `Open` 时报错 |

其余常量（如流式拷贝缓冲大小 `stageBufSize`、对象名长度上限）在源码顶部定义。

### 对旧行为的兼容范围

- **零值 `Options{}` 的行为与旧版本完全一致**：不启用水位、不启用激进回收，
  `Sweep` 只删真正无引用的 blob；保留 API 未被调用时所有版本等价于"无保留"。
- **旧 `manifest.json` 直接可读**：新增的 `retain_permanent` / `retain_until`
  字段均为 `omitempty`，旧文件缺省即"无保留"，无需迁移。
- 新字段只是 `Stats`/`VersionInfo` 的**追加字段**，旧调用方不受影响；
- 新错误 `ErrRetained`、`ErrWatermarkExceeded` 只在**显式使用**新能力（设置
  保留 / 设置水位）时才可能出现，原有错误分类（`ErrNotFound / ErrDeleted /
  ErrCollected / ErrCurrent`）的边界保持不变。

---

## 11. 本地验证

无需网络或外部依赖，直接用仓库既有的 Go 方式：

```bash
go build ./...
go vet ./...
go test ./... -v
go test -race -count=5 ./...   # 竞态检测 + 重复运行，验证稳定性
```

测试覆盖（新能力用 **粗体** 标出）：

- 基础读写、完整性校验、损坏检测（`TestPutGetRoundTrip`、
  `TestReadVerificationDetectsCorruption`）；
- 同对象/跨对象去重、跨分块边界标识稳定（`TestDedupSameContentNoBlobCopy`、
  `TestStableIdentityAcrossChunking`）；
- 版本历史、软删/恢复的精确语义、删除后再写复活；
- 三类拒绝原因可区分（`ErrNotFound / ErrDeleted / ErrCollected`）与
  `ErrCurrent / ErrLocked / ErrName` 边界；
- **保留：永久/限期设置、extend-only 续期与重复设置幂等、提前解除、到期失效、
  重启后保留仍在并继续拦截 purge（`TestRetainPermanentAndTimed`、
  `TestRetainSurvivesRestart`、`TestReleaseAndExpiry`、`TestRetainBlocksDelete`、
  `TestRetainOnDeletedVersion`）**；
- **水位：超线时按"先孤儿 blob、再最老软删"顺序精确回收到线、受保留与可读
  数据绝不牺牲、压不下去时显式 `ErrWatermarkExceeded`、低于水位时保持保守、
  重复回收收敛（`TestWatermarkReclaimsInPriorityOrder`、
  `TestWatermarkNeverTouchesRetainedAndActive`、`TestWatermarkNoOpWhenBelowMark`、
  `TestWatermarkRepeatedSweepStable`）**；
- **回收中断（元数据已提交、blob 未删）后重开继续完成、无悬空引用
  （`TestWatermarkInterruptedRestart`）；`context` 取消后零修改、重跑收敛
  （`TestSweepHonorsCancellation`）**；
- **并发写入与水位 Sweep 同时进行、保留/解除与多个激进 Sweep 交错：版本号、
  字节、保留状态、磁盘账目全部不错乱（`TestConcurrentWritesAndWatermarkSweep`、
  `TestConcurrentRetainSweepRelease`，均在 `-race` 下反复验证）**；
- **新错误分类不串味：从未存在=`ErrNotFound`、软删=`ErrDeleted`、已回收=
  `ErrCollected`、保留中拒绝破坏=`ErrRetained`，且重启后依旧
  （`TestReadRejectionReasonsStayDistinct`）**；
- **并发写入版本串行化、读写并发下无撕裂读**（`-race` 反复验证）；
- **写入中断后重开恢复**（blob 已装元数据未提交 / 仅暂存残留两种窗口）；
- **回收中断后重开继续完成**、**重复 Sweep 幂等**、保守策略不误删软删数据；
- purge 不删除被共享 blob、悬空引用被 `Recover`/`Verify` 显式报告。

### 手工快速验证保留与水位

```go
// 永久保留某个审计版本；重启后仍然有效
store.Retain(ctx, "report/2026-Q2", 3, time.Time{})

// 限期保留到 2026-12-31，重复/缩短调用不会削弱已有保留
store.Retain(ctx, "report/2026-Q2", 3, time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC))

// 查询 / 列出 / 提前解除
ri, _ := store.Retention(ctx, "report/2026-Q2", 3)
held, _ := store.ListRetained(ctx, "report/2026-Q2")
store.ReleaseRetention(ctx, "report/2026-Q2", 3)

// 100 MiB 水位：超线时 Sweep 自动腾空间；压不下去返回 ErrWatermarkExceeded
s, _ := objectstore.Open(ctx, dir, objectstore.Options{WatermarkBytes: 100 << 20})
st, err := s.Sweep(ctx) // st.DiskBytes / st.OverWatermark 说明结果
```

每个测试都会用 `t.Logf` 打印**输入、判定依据与恢复结果**，用 `-v` 可查看。
所有数据都在 `t.TempDir()` 中，用例彼此隔离、可重复运行、结论确定。

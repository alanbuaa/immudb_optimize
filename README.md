# immudb 基线读写性能测试

这个目录里放的是一个“零优化基线版”测试程序，目标是先测清楚直接使用 immudb 时的读写性能，作为后续验证你在 `优化方案.md` 里那些方案的对照组。

当前这版**刻意不做**下面这些优化：

- 不做读写分离
- 不做代理缓存
- 不做对象存储分层
- 不做批量写入（不用 `SetAll`）
- 不做异步提交
- 不修改 immudb 源码

程序只使用 immudb 官方 Go SDK 的直连接口：

- 标准模式：`Set` / `Get`
- 验证模式：`VerifiedSet` / `VerifiedGet`

## 目录结构

```text
.
├── cmd/
│   └── immudb-baseline-bench/
│       └── main.go
└── go.mod
```

## 1. 启动 immudb

如果你本机还没有启动 immudb，可以先用 Docker 跑一个最小实例：

```powershell
docker run -d --name immudb `
  -p 3322:3322 `
  -p 9497:9497 `
  codenotary/immudb:latest
```

默认账号密码通常是：

- 用户名：`immudb`
- 密码：`immudb`
- 数据库：`defaultdb`

## 2. 安装依赖

```powershell
go mod tidy
```

## 3. 运行基线测试

### 3.1 标准基线：先写后读

```powershell
go run ./cmd/immudb-baseline-bench `
  -phase write-read `
  -mode standard `
  -write-count 10000 `
  -read-count 10000 `
  -workers 8 `
  -value-size 1024
```

### 3.2 严格校验基线：VerifiedSet / VerifiedGet

这组结果可以作为你后面验证“按需验证”优化时的强一致、强校验对照组。

```powershell
go run ./cmd/immudb-baseline-bench `
  -phase write-read `
  -mode verified `
  -write-count 5000 `
  -read-count 5000 `
  -workers 4 `
  -value-size 1024
```

### 3.3 只测读

如果你已经写入过一批数据，可以只测读性能。此时需要指定已有数据的 `key-prefix` 和数量。

```powershell
go run ./cmd/immudb-baseline-bench `
  -phase read `
  -mode standard `
  -key-prefix baseline-20260408-153000 `
  -prepared-count 10000 `
  -read-count 20000 `
  -workers 8 `
  -value-size 1024
```

## 4. 重要参数

- `-mode`：`standard` 或 `verified`
- `-phase`：`write`、`read`、`write-read`
- `-workers`：并发 worker 数
- `-write-count`：写请求总数
- `-read-count`：读请求总数
- `-prepared-count`：只读模式下已有 key 数
- `-value-size`：每条 value 的字节数
- `-key-prefix`：本轮测试使用的数据前缀
- `-json-output`：把测试结果导出成 JSON，方便后续做横向对比

## 5. 输出内容

程序会输出每个阶段的：

- 完成请求数
- 错误数
- 总耗时
- `ops/s`
- 吞吐量（`bytes/s`）
- 平均延迟
- `p50 / p95 / p99`
- 最大延迟

## 6. 适合后续怎么对比

你后面验证优化方案时，建议尽量固定下面这些参数不变：

- 同一台机器
- 同一份 immudb 配置
- 同一批数据规模
- 同样的 `workers`
- 同样的 `value-size`
- 同样的测试顺序

这样才方便把这版“零优化基线”和后续版本做干净对比。

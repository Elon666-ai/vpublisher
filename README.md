# vpublisher

`vpublisher` 是一个基于ffmpeg的 Go推流进程。 支持simulcast SRT推流，支持横屏/竖屏推流，支持单SRT地址和多SRT地址。


## 功能特性

- WebSocket 注册与心跳上报
- 支持远程控制推流启停
- 支持多目标 SRT 输出（`publishUrl` + `publishUrl2`）
- 支持输入源为本地文件或 Windows dshow 采集设备
- 支持 `videoLayout`：竖屏/横屏/横竖同推
- 支持 `originDown` / `originUp` 按目标暂停与恢复
- 支持查询当前推流 PTS

## 项目结构

```text
vpublisher/
  conf/          配置加载与默认配置
  ws/            WebSocket 与 ffmpeg 管理逻辑
  tracer/        日志模块
  utils/         常量与工具函数
  pb3/           protobuf 代码
  main.go        入口
  build.sh       构建脚本
```

## 运行环境

- Go 1.24.1+
- 已安装 `ffmpeg`，并确保命令行可直接执行
- 可访问的 WebSocket 管理端（`workerMgrAddr`）

## 配置文件加载规则

- 默认配置文件：`conf/vpublisher.yml`
- 若检测到运行环境（`ENV`），则读取：`conf/vpublisher.yml.<env>`

运行环境来源优先级：

- 进程环境变量 `ENV`
- `.env` 文件中的 `ENV=...`

## 配置项说明

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `workerType` | 是 | 节点类型 |
| `workerId` | 是 | 节点 ID，必须 > 0 |
| `workerRegion` | 是 | 节点所属区域 |
| `workerMgrAddr` | 是 | WebSocket 管理地址 |
| `authApiAddr` | 否 | 动态解析推流地址接口 |
| `appSecret` | 否 | 鉴权密钥 |
| `publishUrl` | 条件 | 主推流地址（与 `publishUrl2` 至少一个非空） |
| `publishUrl2` | 条件 | 第二推流地址 |
| `inputFile` | 是 | 输入源（文件或 dshow 设备） |
| `videoLayout` | 否 | `portrait` / `landscape` / `both`，默认 `portrait` |
| `publishOnReady` | 否 | 启动后是否立即推流 |

### inputFile 写法

1. 文件输入

```yaml
inputFile: sanbado01.mp4
```

2. Windows dshow 设备输入

```yaml
inputFile: video="OBS Virtual Camera":audio="麦克风 (Realtek Audio)"
```

只采集视频也可以：

```yaml
inputFile: video="OBS Virtual Camera"
```

## videoLayout 说明

- `portrait`：输出 3 路竖屏分辨率
- `landscape`：输出 3 路横屏分辨率
- `both`：同时输出横屏+竖屏（共 6 路）

注意：`both` 会明显增加 CPU/GPU 编码负载。

## dshow 采集与多路推流

在 Windows dshow 输入场景下，如果配置了多个推流地址，程序会自动使用单个 ffmpeg 进程通过 `tee` 同时输出到多个 SRT 目标，避免“采集设备只能被一个进程占用”的问题。

## 环境变量覆盖

以下变量会覆盖 YAML 配置：

- `VPUBLISHER_WORKER_TYPE`
- `VPUBLISHER_WORKER_ID`
- `VPUBLISHER_WORKER_REGION`
- `VPUBLISHER_WORKER_MGR_ADDR`
- `VPUBLISHER_AUTH_API_ADDR`
- `VPUBLISHER_APP_SECRET`
- `VPUBLISHER_PUBLISH_URL`
- `VPUBLISHER_PUBLISH_URL2`
- `VPUBLISHER_INPUT_FILE`
- `VPUBLISHER_VIDEO_LAYOUT`
- `VPUBLISHER_PUBLISH_ON_READY`

## 示例配置

```yaml
workerType: NODE_ROLE_PUBLISHER
workerId: 1
workerRegion: Manila
workerMgrAddr: ws://localhost:8090/ws/mmx
authApiAddr: http://localhost:8090/auth/generate
appSecret: test@Vhub2024
publishUrl: srt://localhost:8890?streamid=publish:live/stream:<user>:<password>&pkt_size=1316
publishUrl2: srt://localhost:8990?streamid=publish:live/stream:<user>:<password>&pkt_size=1316
inputFile: video="OBS Virtual Camera"
videoLayout: both
publishOnReady: true
```

## 构建

### Linux/macOS

```bash
chmod +x build.sh
./build.sh
```

### Windows PowerShell

```powershell
go build -o bin/vpublish.exe main.go
```

## 运行

```bash
go run main.go
```

或：

```bash
./bin/vpublish.exe
```

## WebSocket 指令

- `COMMAND_TYPE_START_PUB`
- `COMMAND_TYPE_STOP_PUB`
- `COMMAND_TYPE_ORIGIN_DOWN`
- `COMMAND_TYPE_ORIGIN_UP`
- `COMMAND_TYPE_QUERY_PUB_PTS`

## 日志

- 日志模块：`tracer`
- 常见日志目录：`bin/logs/`

## 常见问题

1. 启动后反复重试并报 `Could not find video device`
   - dshow 设备名不匹配，检查设备实际名称。

2. 报错 `exec: "ffmpeg": executable file not found`
   - `ffmpeg` 未安装或未加入 `PATH`。

3. `publishUrl and publishUrl2 cannot both be empty`
   - 至少配置一个推流地址。

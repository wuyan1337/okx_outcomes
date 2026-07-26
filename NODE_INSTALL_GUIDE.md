# OKX 出口节点安装与接入指南

本文用于把一台新的 Ubuntu VPS 安装为 OKX Bot 的出口节点。

> 这不是在新 VPS 上再运行一套 Bot。新 VPS 只负责受限的 SSH 出口；现有 Bot 主机通过本地 SOCKS tunnel 使用它。

## 1. 最终结构

```text
现有 Bot 主机
  └─ 127.0.0.1:9116（本地 SOCKS）
       └─ SSH tunnel
            └─ 新 Ubuntu VPS
                 └─ OKX REST / WebSocket
```

安装完成后：

- 新 VPS 不运行公网 SOCKS 服务；
- `9116` 只监听 Bot 主机的 `127.0.0.1`；
- 新 VPS 的 SSH 只允许 Bot 公网 IP；
- SSH 账户只能转发到指定 OKX 域名；
- tunnel 由 systemd 自动启动和断线重连；
- Bot 内使用统一节点名，例如 `JP-TYO-06`。

## 2. 准备信息

开始前准备以下信息：

| 项目 | 示例 | 说明 |
| --- | --- | --- |
| 节点 ID | `JP-TYO-06` | 必须符合 `CC-CITY-NN` |
| 新 VPS 公网 IP | `203.0.113.20` | 下文写作 `NEW_VPS_IP` |
| Bot 主机公网 IP | `203.0.113.10` | 下文写作 `BOT_PUBLIC_IP` |
| Bot 本地端口 | `9116` | 必须未被占用 |
| OKX Region | `AU` | 账户使用 `us.okx.com` 时选 AU，使用 `www.okx.com` 时选 CN |
| 城市名称 | `Tokyo` | 写入节点 registry，用于界面显示 |

常用节点命名示例：

| 地点 | 示例名称 |
| --- | --- |
| Tokyo | `JP-TYO-06` |
| Osaka | `JP-OSA-02` |
| Singapore | `SG-SIN-02` |
| Seoul | `KR-SEL-02` |
| Sydney | `AU-SYD-03` |

编号必须是尚未使用的新编号。先在 Bot 项目目录检查：

```bash
python3 scripts/proxy_nodes.py list
```

检查本地端口是否空闲：

```bash
ss -ltn 'sport = :9116'
```

没有输出表示端口当前未监听。

## 3. 升级 Bot 项目

Bot 项目需要包含以下三个工具：

- `scripts/bootstrap-egress-node.sh`
- `scripts/install-egress-tunnel.sh`
- `scripts/proxy_nodes.py`

如果尚未部署新版升级包，先停止 Bot，备份服务器原有的 `config.yaml` 和整个 `data/`，然后将升级包覆盖到现有项目目录。

不要删除或覆盖服务器上的：

- `config.yaml`；
- `data/accounts.db` 与 `data/.key`；
- pending sells、cost layers 和 `poly_asset_map.json`；
- `data/proxy_nodes.json`；
- 现有 Nginx、Caddy、systemd 和 tunnel 配置。

## 4. 在 Bot 主机生成节点专用密钥

以下示例使用节点 `JP-TYO-06`：

```bash
sudo ssh-keygen -t ed25519 -N '' \
  -f /root/JP-TYO-06 \
  -C okx-JP-TYO-06
```

检查生成结果：

```bash
sudo ls -l /root/JP-TYO-06 /root/JP-TYO-06.pub
sudo cat /root/JP-TYO-06.pub
```

需要把 `.pub` 文件中的整行公钥传到新 VPS。

安全要求：

- `/root/JP-TYO-06` 是私钥，不能上传到新 VPS、项目目录、ZIP 或 Git；
- 只把 `/root/JP-TYO-06.pub` 公钥传到新 VPS；
- 每个出口节点使用独立密钥。

## 5. 准备新 VPS

要求：

- Ubuntu；
- 可以通过云厂商控制台以 root 或 sudo 用户操作；
- 新 VPS 可以访问 OKX 的 HTTPS/WebSocket 地址；
- 云防火墙允许 Bot 公网 IP 访问 SSH 端口；
- 不开放公网 `9111`、`9116` 或其他 SOCKS 端口。

推荐的云防火墙入站规则：

```text
TCP 22  来源 BOT_PUBLIC_IP/32
```

出站至少需要 DNS，以及 OKX 使用的 TCP 443/8443。

通过云厂商控制台、已经验证 host key 的 SFTP/SCP，或其他可信方式，把以下文件放到新 VPS：

```text
/root/bootstrap-egress-node.sh
/root/JP-TYO-06.pub
```

不要把首次 `ssh-keyscan` 的结果直接当成可信 host key。

## 6. 在新 VPS 执行 bootstrap

必须从新 VPS 控制台确认命令中的 `BOT_PUBLIC_IP` 是现有 Bot 主机的公网 IP，而不是新 VPS 的 IP。

IPv4 使用 `/32`：

```bash
sudo bash /root/bootstrap-egress-node.sh \
  --node-id JP-TYO-06 \
  --bot-cidr BOT_PUBLIC_IP/32 \
  --public-key-file /root/JP-TYO-06.pub
```

IPv6 单地址使用 `/128`。

脚本会：

1. 创建专用的 `okx-egress` nologin 用户；
2. 设置 key-only SSH；
3. 限制来源为 Bot 单一公网 IP；
4. 禁止 shell、TTY、agent forwarding 和远程 forwarding；
5. 只允许 local forwarding 到 OKX REST/WS 目标；
6. 验证 sshd 最终有效配置；
7. 检查没有公网 `9111` listener；
8. 失败时恢复原来的 sshd fragment 和 authorized key。

## 7. 从 VPS 控制台取得真实 host key

在新 VPS 控制台执行：

```bash
sudo cat /etc/ssh/ssh_host_ed25519_key.pub
```

保存输出的完整一行，例如：

```text
ssh-ed25519 AAAAC3... root@example
```

然后读取对应指纹：

```bash
sudo ssh-keygen -lf \
  /etc/ssh/ssh_host_ed25519_key.pub \
  -E sha256
```

保存输出中的：

```text
SHA256:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

host public key 和 SHA256 指纹必须通过 VPS 控制台等可信通道取得。不要用网络首次扫描结果代替控制台核验。

## 8. 把 VPS host key 保存到 Bot 主机

回到 Bot 主机：

```bash
sudo nano /root/JP-TYO-06.host-ed25519.pub
```

粘贴第 7 步取得的完整 `ssh-ed25519 ...` 行并保存。

检查文件：

```bash
sudo ssh-keygen -lf \
  /root/JP-TYO-06.host-ed25519.pub \
  -E sha256
```

显示的 SHA256 必须与 VPS 控制台记录完全一致。

## 9. 在 Bot 主机安装持久 tunnel

进入 Bot 项目目录：

```bash
cd /你的实际路径/okx-outcomes-bot
```

AU 账户示例：

```bash
sudo bash ./scripts/install-egress-tunnel.sh \
  --node-id JP-TYO-06 \
  --host NEW_VPS_IP \
  --region AU \
  --local-port 9116 \
  --private-key /root/JP-TYO-06 \
  --host-key-file /root/JP-TYO-06.host-ed25519.pub \
  --host-key-fingerprint SHA256:替换为控制台记录的完整指纹
```

CN 账户将 `--region AU` 改为：

```text
--region CN
```

安装脚本会：

- 再次验证 host public key 和 SHA256 指纹；
- 将私钥安全复制到 `/etc/okx-egress/keys/`；
- 生成固定 known_hosts 和独立 SSH 配置；
- 创建 `127.0.0.1:9116` dynamic SOCKS；
- 创建并启动 systemd 服务；
- 验证 listener 没有绑定公网地址；
- 通过 tunnel 测试对应 OKX Region；
- 测试失败时停用 systemd 服务。

## 10. 检查 tunnel

节点 `JP-TYO-06` 对应的服务名为：

```text
okx-egress-jp-tyo-06.service
```

检查状态：

```bash
sudo systemctl status okx-egress-jp-tyo-06.service
```

检查监听地址：

```bash
ss -ltn 'sport = :9116'
```

正确结果只能绑定：

```text
127.0.0.1:9116
```

测试 AU endpoint：

```bash
curl --silent --show-error --fail --noproxy '' \
  --socks5-hostname 127.0.0.1:9116 \
  https://us.okx.com/api/v5/public/time
```

测试 CN endpoint：

```bash
curl --silent --show-error --fail --noproxy '' \
  --socks5-hostname 127.0.0.1:9116 \
  https://www.okx.com/api/v5/public/time
```

## 11. 注册节点到 Bot

在 Bot 项目目录执行：

```bash
python3 scripts/proxy_nodes.py add \
  --id JP-TYO-06 \
  --location Tokyo \
  --port 9116
```

检查实际 registry：

```bash
python3 scripts/proxy_nodes.py list
```

预期包含类似：

```text
JP-TYO-06  JP-TYO-06  override  tunnel  127.0.0.1:9116  Tokyo
```

registry 写入：

```text
data/proxy_nodes.json
```

它是服务器本地配置，后续升级不能用开发机文件覆盖。

如果 ID 已存在，CLI 会拒绝覆盖。只有明确检查旧配置并确实要替换时，才使用 `--replace`。

## 12. 重启 Bot

节点 registry 在 FastAPI 启动时加载，因此注册后必须重启 Bot。

如果服务名是 `okx-bot`：

```bash
sudo systemctl restart okx-bot
sudo systemctl status okx-bot
```

如果实际服务名不同，替换为服务器自己的服务名。不要同时启动多个 Uvicorn worker 或第二个共享同一 `data/` 的 Bot 进程。

## 13. 节点测速

### 13.1 Connectivity only

不需要账户，只验证网络路径：

```bash
python3 scripts/proxy_nodes.py benchmark \
  --region AU \
  --samples 5 \
  --node JP-TYO-06
```

该结果不能当成真实行情或下单速度。

### 13.2 真实 market read

OKX `pm-books` 需要本地账户签名：

```bash
python3 scripts/proxy_nodes.py benchmark \
  --region AU \
  --samples 5 \
  --node JP-TYO-06 \
  --yes-asset-id REAL_OKX_YES_ASSET_ID \
  --poly-token-id REAL_POLY_TOKEN_ID \
  --account-id LOCAL_ACCOUNT_ID
```

要求：

- `LOCAL_ACCOUNT_ID` 是 Bot 本地数据库的账户 ID；
- 账户 Region 必须与 `--region` 一致；
- OKX asset 必须是该市场的真实 `yesAssetId`；
- Poly token 必须是对应 outcome 的真实 token。

### 13.3 最终节点选择

不要只按一次 benchmark 选择节点。建议：

1. 先排除失败、degraded 和 jitter 很高的节点；
2. 对比 warm median、max 和成功率；
3. 在高峰和低峰多次测试；
4. 最好观察 24 小时；
5. 最终以真实 Sniper `Trigger→Ack`、订单响应失败率和 jitter 决定。

如果 Bot 主机在 Tokyo，可以先用 `JP-TYO-02` Direct 作为基线，再比较新的 Tokyo tunnel。

## 14. 在网页中使用

重启 Bot 后：

1. 打开 Dashboard；
2. 打开 System Health；
3. 选择账户对应的 AU 或 CN Region；
4. 运行测速；
5. 新节点应显示为 `JP-TYO-06 | Tokyo | SSH tunnel`；
6. 新建账户时，可以在 Proxy Node 下拉框选择该节点。

当前账户设置页面没有现有账户的节点切换入口。不要在 Bot 运行时直接修改 `accounts.db`。现有账户迁移节点前，应停止并对账所有策略、订单和仓位，再通过安全迁移流程修改。

## 15. 常见故障

### 15.1 bootstrap 提示 bot CIDR 无效

IPv4 必须写成单地址 `/32`：

```text
203.0.113.10/32
```

IPv6 单地址必须使用 `/128`。

### 15.2 Bot 无法连接新 VPS

检查：

- 新 VPS 云防火墙 TCP 22 来源是否为 Bot 公网 IP；
- Bot 公网 IP 是否发生变化；
- 新 VPS 的 SSH 服务是否正在运行；
- `--host` 和 `--ssh-port` 是否正确；
- dedicated private key 是否正确。

### 15.3 host key fingerprint mismatch

停止安装，不要跳过验证。重新从 VPS 控制台读取：

```bash
sudo cat /etc/ssh/ssh_host_ed25519_key.pub
sudo ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub -E sha256
```

确认 VPS 没有被重装或替换后，再更新 Bot 主机上的 host-key 文件。

### 15.4 systemd 服务启动失败

查看日志：

```bash
sudo journalctl -u okx-egress-jp-tyo-06.service -n 100 --no-pager
```

检查本地端口是否已经被其他程序占用：

```bash
ss -ltn 'sport = :9116'
```

### 15.5 tunnel 启动但 OKX 测试失败

检查：

- `--region` 是否与账户 endpoint 一致；
- 新 VPS 是否能解析 OKX 域名；
- 新 VPS 出站 443/8443 是否允许；
- VPS IP 是否被 OKX 或云提供商区域策略限制；
- systemd tunnel 是否持续运行。

### 15.6 CLI 注册成功但网页没有节点

节点 registry 只在 Bot 启动时读取。重启 Bot 服务并强制刷新浏览器。

### 15.7 节点显示 failed 或 degraded

- `failed`：本轮 route 不可用，不参与最低 RTT；
- `degraded`：部分 warm 样本失败，不应作为首选；
- `Connectivity only`：只验证连通，不代表真实行情或订单速度；
- `Market read`：使用真实 asset/token，其中 OKX book 请求由本地账户签名。

## 16. 重要安全检查清单

- [ ] 新 VPS 没有开放公网 9111/9116；
- [ ] SSH 22 只允许 Bot 公网 IP；
- [ ] 每个节点使用独立 Ed25519 私钥；
- [ ] 私钥不在项目、ZIP 或 Git 中；
- [ ] host key 和 SHA256 指纹来自 VPS 控制台；
- [ ] Bot 本地 SOCKS 只监听 `127.0.0.1`；
- [ ] `data/proxy_nodes.json` 已备份；
- [ ] 注册节点后已经重启 Bot；
- [ ] 新节点先通过 connectivity 和 market benchmark；
- [ ] 最终选择参考真实 Sniper `Trigger→Ack`；
- [ ] 没有为了速度删除 positions、balance、订单对账或持久化风控。

## 17. 如果新服务器是新的 Bot 主机

如果你要迁移的是整套 Bot，而不是增加出口 VPS，不要执行本文流程。整机迁移还需要：

- 一致性迁移 `config.yaml`；
- 成对迁移 `data/accounts.db` 和 `data/.key`；
- 迁移 WAL/SHM 或在停服务后做一致性备份；
- 迁移 pending sells、cost layers、Poly asset map 和 node registry；
- 迁移 `/etc/okx-egress/` 与 tunnel systemd units；
- 迁移 Bot systemd、Nginx/Caddy、HTTPS 和 `/okx` base path 配置；
- 确保新旧主机不会同时运行并共享同一账户状态。

整机迁移应使用单独的部署迁移步骤。

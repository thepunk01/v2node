#!/bin/bash
# 把节点上的 v2node 换成支持 mieru 的版本（保留现有配置 /etc/v2node/config.json）。
# 用法：bash <(curl -Ls https://raw.githubusercontent.com/thepunk01/v2node/main/script/mieru_upgrade.sh)
# 指定版本：bash mieru_upgrade.sh mieru-3
set -e

REPO="${V2NODE_MIERU_REPO:-thepunk01/v2node}"
red='\033[0;31m'; green='\033[0;32m'; yellow='\033[0;33m'; plain='\033[0m'

[[ $EUID -ne 0 ]] && echo -e "${red}请用 root 运行${plain}" && exit 1

case "$(uname -m)" in
  x86_64|x64|amd64) arch="64" ;;
  aarch64|arm64) arch="arm64-v8a" ;;
  *) echo -e "${red}暂只提供 amd64 / arm64 版本，当前架构：$(uname -m)${plain}"; exit 1 ;;
esac

if [[ ! -x /usr/local/v2node/v2node ]]; then
  echo -e "${yellow}没检测到已安装的 v2node。先用官方脚本装好 v2node 并填好面板对接信息，再运行本脚本：${plain}"
  echo "wget -N https://raw.githubusercontent.com/thepunk01/v2node/main/script/install.sh && bash install.sh"
  exit 1
fi

for cmd in curl unzip; do
  command -v $cmd >/dev/null 2>&1 || { echo -e "${red}缺少 $cmd，请先安装（apt install -y $cmd 或 yum install -y $cmd）${plain}"; exit 1; }
done

tag="$1"
if [[ -z "$tag" ]]; then
  tag=$(curl -Ls "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')
fi
if [[ -z "$tag" ]]; then
  echo -e "${red}获取最新版本失败（可能是 GitHub API 限流），请稍后再试或手动指定版本，如：bash mieru_upgrade.sh mieru-1${plain}"
  exit 1
fi

url="https://github.com/${REPO}/releases/download/${tag}/v2node-linux-${arch}.zip"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
echo -e "${green}下载 ${tag} (${arch}) ...${plain}"
curl -fL "$url" -o "$tmp/v2node.zip"
unzip -o -q "$tmp/v2node.zip" -d "$tmp/x"
chmod +x "$tmp/x/v2node"

cp -f /usr/local/v2node/v2node /usr/local/v2node/v2node.bak
install -m 755 "$tmp/x/v2node" /usr/local/v2node/v2node
[[ -f "$tmp/x/geoip.dat" ]] && cp -f "$tmp/x/geoip.dat" /etc/v2node/
[[ -f "$tmp/x/geosite.dat" ]] && cp -f "$tmp/x/geosite.dat" /etc/v2node/

if command -v systemctl >/dev/null 2>&1 && systemctl list-unit-files | grep -q '^v2node'; then
  systemctl restart v2node
elif [[ -x /etc/init.d/v2node ]]; then
  /etc/init.d/v2node restart
fi

sleep 2
/usr/local/v2node/v2node version 2>/dev/null || true
echo -e "${green}完成。旧程序备份在 /usr/local/v2node/v2node.bak${plain}"
echo -e "${yellow}注意：v2node 菜单里的「更新」现在会从 thepunk01/v2node 更新（带 mieru）。${plain}"

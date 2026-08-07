import base64
import json
import time
import threading
import logging
from typing import List, Dict, Any, Tuple
from urllib.parse import urlencode
import hashlib
import hmac
import yaml
from fastapi import FastAPI, Response
from curl_cffi import requests
import curl_cffi.requests.exceptions
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============ 日志与基础配置 ============
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

UID = "f33e3c8d-6d03-40f4-963c-6cfd753e15d6"
HMAC_KEY = b'5f5749e77a9b'
VERSION = '2.3.0'
PLATFORM = 'win'
MAX_WORKERS = 20  # 线程池大小

app = FastAPI(title="Proxy Subscription Server")

# ============ 核心数据管理器 ============
class SubscriptionManager:
    def __init__(self):
        self.session = requests.Session()
        self.session.verify = False
        
        # 缓存各种格式的配置
        self.raw_nodes = []       # 原始统一节点数据
        self.clash_yaml = ""      # Clash 配置文件
        self.singbox_json = ""    # Sing-box 配置文件
        self.v2ray_base64 = ""    # V2ray/v2rayN 订阅链接
        
        self.is_ready = False     # 是否初始化完成

    def get_node_config(self, host: str) -> Dict:
        """调用 API 获取单个节点具体配置 (不进行Ping测试)"""
        timestamp_ms = int(time.time() * 1000)
        body_data_dict = {'timestamp': timestamp_ms, 'uid': UID, 'version': VERSION}
        string_to_sign = urlencode(body_data_dict)
        
        signature = base64.b64encode(
            hmac.new(HMAC_KEY, string_to_sign.encode('utf-8'), hashlib.sha256).digest()
        ).decode('utf-8')

        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 GreenHub/2.3.0",
            "content-type": "application/json",
            "accept": "application/json"
        }
        
        query_params = {'uid': UID, 'version': VERSION, 'platform': PLATFORM, 'sign': signature}
        final_url = f"https://{host}/d/v1/account?{urlencode(query_params)}"
        
        response = self.session.post(final_url, json=body_data_dict, headers=headers, timeout=10)
        return response.json()

    def _process_single_node(self, global_index: int, server_label_zh: str, node: Dict[str, Any]) -> Tuple[int, Dict]:
        """处理单节点信息提取"""
        node_name = f"[{server_label_zh}]{node['label_zh']}"
        node_domain = node["domain"]
        
        try:
            session_result = self.get_node_config(node_domain)
            data = session_result.get("data", {})
            
            if not data.get("port") or not data.get("id"):
                return None
                
            raw_node = {
                "name": node_name,
                "server": node_domain,
                "port": int(data.get("port")),
                "uuid": data.get("id"),
                "path": data.get("path", "/")
            }
            return global_index, raw_node
            
        except Exception as e:
            logger.debug(f"节点获取失败 {node_domain}: {e}")
        return None

    def refresh_nodes(self):
        """主刷新流程：获取节点列表 -> 并发获取详情 -> 构建多版本配置"""
        logger.info("开始刷新节点数据...")
        try:
            # 1. 获取服务器列表
            res = self.session.get("https://d1fumiloozdotj.cloudfront.net/v1/server_list_v7.json", timeout=10)
            server_list = res.json()
            
            if "data" not in server_list:
                logger.error("未获取到节点列表数据")
                return

            tasks = []
            global_index = 0
            for server in server_list["data"]:
                if server["id"] == "QQJD":
                    continue
                for node in server.get("servers", []):
                    tasks.append((global_index, server["label_zh"], node))
                    global_index += 1

            # 2. 并发获取节点详情
            results = []
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_index = {
                    executor.submit(self._process_single_node, idx, srv_label, node): idx 
                    for idx, srv_label, node in tasks
                }
                for future in as_completed(future_to_index):
                    result = future.result()
                    if result:
                        results.append(result)

            # 3. 按原始顺序排序并提取
            results.sort(key=lambda x: x[0])
            self.raw_nodes = [res[1] for res in results]
            logger.info(f"成功获取 {len(self.raw_nodes)} 个节点信息。开始生成配置...")

            # 4. 构建配置
            if self.raw_nodes:
                self.build_clash_config()
                self.build_singbox_config()
                self.build_v2ray_sub()
                self.is_ready = True
                logger.info("所有配置生成完毕。")
            else:
                logger.warning("获取到的有效节点数为 0。")

        except Exception as e:
            logger.error(f"刷新节点数据时发生异常: {e}")

    # ============ 配置生成器 (Builders) ============

    def build_clash_config(self):
        """构建 Clash YAML 格式"""
        proxies = []
        node_names = []
        
        for node in self.raw_nodes:
            proxy = {
                "name": node["name"], "type": "vmess", "server": node["server"],
                "port": node["port"], "uuid": node["uuid"], "alterId": 0,
                "cipher": "auto", "udp": True, "tls": True, "network": "ws",
                "ws-opts": {"path": node["path"]}
            }
            proxies.append(proxy)
            node_names.append(node["name"])

        groups = [
            {"name": "代理模式", "type": "select", "proxies": ["自动选择", "手动选择", "DIRECT"]},
            {"name": "手动选择", "type": "select", "proxies": node_names},
            {"name": "自动选择", "type": "url-test", "url": "http://www.gstatic.com/generate_204", "interval": 300, "proxies": node_names}
        ]

        # 增加中国大陆直连规则
        rules = [
            "GEOSITE,cn,DIRECT",
            "GEOIP,LAN,DIRECT",
            "GEOIP,CN,DIRECT",
            "MATCH,代理模式"
        ]

        clash_dict = {
            "port": 7890, "socks-port": 7891, "allow-lan": True, "mode": "rule",
            "log-level": "info", "proxies": proxies, "proxy-groups": groups, "rules": rules
        }
        
        self.clash_yaml = yaml.dump(clash_dict, allow_unicode=True, sort_keys=False)

    def build_singbox_config(self):
        """构建 Sing-box JSON 格式"""
        outbounds = [
            {"type": "selector", "tag": "PROXY", "outbounds": ["AUTO", "MANUAL"]},
            {"type": "urltest", "tag": "AUTO", "outbounds": []},
            {"type": "selector", "tag": "MANUAL", "outbounds": []},
            {"type": "direct", "tag": "DIRECT"},
            {"type": "block", "tag": "BLOCK"}
        ]
        
        node_tags = []
        for node in self.raw_nodes:
            tag = node["name"]
            node_tags.append(tag)
            outbounds.append({
                "type": "vmess", "tag": tag, "server": node["server"], "server_port": node["port"],
                "uuid": node["uuid"], "security": "auto",
                "tls": {"enabled": True, "server_name": node["server"], "insecure": True},
                "transport": {"type": "ws", "path": node["path"]}
            })
            
        outbounds[1]["outbounds"] = node_tags  # 放入 AUTO
        outbounds[2]["outbounds"] = node_tags  # 放入 MANUAL

        singbox_dict = {
            "log": {"level": "info"},
            "outbounds": outbounds,
            "route": {
                "rules": [
                    {"geosite": ["cn"], "outbound": "DIRECT"},
                    {"geoip": ["cn", "private"], "outbound": "DIRECT"}
                ],
                "auto_detect_interface": True
            }
        }
        self.singbox_json = json.dumps(singbox_dict, ensure_ascii=False, indent=2)

    def build_v2ray_sub(self):
        """构建标准 V2Ray/v2rayN Base64 订阅"""
        links = []
        for node in self.raw_nodes:
            v_dict = {
                "v": "2", "ps": node["name"], "add": node["server"], "port": str(node["port"]),
                "id": node["uuid"], "aid": "0", "scy": "auto", "net": "ws", "type": "none",
                "host": node["server"], "path": node["path"], "tls": "tls", "sni": node["server"]
            }
            # vmess 协议标准：vmess:// + base64(json)
            json_str = json.dumps(v_dict, separators=(',', ':')).encode('utf-8')
            vmess_link = "vmess://" + base64.b64encode(json_str).decode('utf-8')
            links.append(vmess_link)
            
        raw_sub = "\n".join(links)
        self.v2ray_base64 = base64.b64encode(raw_sub.encode('utf-8')).decode('utf-8')

# 实例化全局管理器
sub_manager = SubscriptionManager()

def background_task():
    """后台定时刷新任务"""
    while True:
        sub_manager.refresh_nodes()
        time.sleep(300)  # 5分钟刷新一次

# ============ FastAPI 路由 (拆分且语义化) ============

@app.get("/")
def index():
    return {"status": "running", "ready": sub_manager.is_ready, "nodes_count": len(sub_manager.raw_nodes)}

@app.get("/sub/clash")
def get_clash_yaml():
    """提供给 Clash / Clash Meta (Mihomo) 的订阅"""
    if not sub_manager.is_ready:
        return Response("Configuration is updating, please try again in a few seconds.", status_code=503)
    return Response(sub_manager.clash_yaml, media_type="text/plain; charset=utf-8")

@app.get("/sub/singbox")
def get_singbox_json():
    """提供给 Sing-box 的订阅"""
    if not sub_manager.is_ready:
        return Response("Configuration is updating...", status_code=503)
    return Response(sub_manager.singbox_json, media_type="application/json; charset=utf-8")

@app.get("/sub/v2ray")
def get_v2ray_base64():
    """提供给 V2Ray / v2rayN / Nekoray / Shadowrocket 等通用客户端的订阅"""
    if not sub_manager.is_ready:
        return Response("Configuration is updating...", status_code=503)
    return Response(sub_manager.v2ray_base64, media_type="text/plain; charset=utf-8")

# ============ 启动入口 ============
if __name__ == "__main__":
    logger.info("服务正在初始化...")
    
    # 首次启动时同步拉取一次数据
    sub_manager.refresh_nodes()
    
    # 启动后台刷新线程
    threading.Thread(target=background_task, daemon=True).start()
    logger.info("后台刷新线程已启动。")
    
    # 启动 FastAPI 服务
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8833)

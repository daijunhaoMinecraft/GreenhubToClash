import base64
import sys
import time
import subprocess

import curl_cffi.requests.exceptions
from curl_cffi import requests
#import requests
import json
from fastapi import FastAPI
import threading
import logging
from typing import List, Dict, Any
from urllib.parse import urlencode
import hashlib
import hmac
import yaml
from fastapi.responses import Response

serverListJson = {}
yamlConfig = {
    "port": 7890,
    "socks-port": 1456,
    "allow-lan": False,
    "mode": "rule",
    "log-level": "info",
    "external-controller": "127.0.0.1:9090",
    "proxies": [],
    "proxy-groups": [

    ],
    "rules": []
}
uid = "f33e3c8d-6d03-40f4-963c-6cfd753e15d6"

# Init
#requests.packages.urllib3.disable_warnings()
session = requests.Session()
session.verify = False
app = FastAPI()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def getNodeConfig(HOST: str):
    HMAC_KEY = b'5f5749e77a9b'
    VERSION = '2.3.0'
    PLATFORM = 'win'

    timestamp_ms = int(time.time() * 1000)
    body_data_dict = {}
    body_data_dict['timestamp'] = timestamp_ms
    body_data_dict['uid'] = uid
    body_data_dict['version'] = VERSION
    string_to_sign = urlencode(body_data_dict)

    hmac_obj = hmac.new(HMAC_KEY, string_to_sign.encode('utf-8'), hashlib.sha256)
    signature_bytes = hmac_obj.digest()
    sign = base64.b64encode(signature_bytes).decode('utf-8')

    query_params = {
        'uid': uid,
        'version': VERSION,
        'platform': PLATFORM,
        'sign': sign
    }
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) GreenHub/2.3.0 Chrome/91.0.4472.164 Electron/13.6.9 Safari/537.36",
        "content-type": "application/json",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "zh-CN",
        "accept": "application/json, text/plain, */*",
        "Connection": "keep-alive"
    }
    final_url = f"https://{HOST}/d/v1/account?{urlencode(query_params)}"
    response = requests.post(final_url, json=body_data_dict,headers=headers , verify=False)
    return response.json()


def getServerList():
    global serverListJson
    try:
        serverListJson = session.get("https://d1fumiloozdotj.cloudfront.net/v1/server_list_v7.json").json()
    except Exception as e:
        logger.fatal(f"获取节点出现错误: {e}")
        if serverListJson == {}:
            sys.exit()

def threadGetServerList():
    while True:
        getServerList()
        getServerConfig()
        time.sleep(300)

def ping_host(host: str) -> bool:
    """
    Ping主机以检查是否可达
    """
    try:
        # Windows系统使用-n参数，其他系统使用-c参数
        param = "-n" if sys.platform.lower() == "win32" else "-c"
        result = subprocess.run(
            ["ping", param, "1", host],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5
        )
        return result.returncode == 0
    except:
        return False

def process_node(server_label_zh: str, node: Dict[str, Any], proxy_groups: Dict[str, Any], 
                 proxies_list: List[Dict[str, Any]], rules_list: List[str]):
    """
    处理单个节点
    """
    nodeDomain = node["domain"]
    nodeName = "[" + server_label_zh + "]" + node["label_zh"]
    nodePath = server_label_zh + " -> " + nodeName
    
    # 先ping检测节点是否可达
    # if not ping_host(nodeDomain):
    #     logger.fatal(f"节点无法Ping通，已跳过: {nodeDomain}")
    #     return
    
    # check status
    try:
        nodeStatusResult = session.get(
            f"https://{nodeDomain}/d/v1/status?uid=f33e3c8d-6d03-40f4-963c-6cfd753e15d6&version=2.3.0&platform=win", 
            timeout=3
        ).json()
        
        # Get Session config
        sessionResult = getNodeConfig(nodeDomain)
        proxy_groups["proxies"].append(nodeName)
        proxy = {
            "name": nodeName,
            "type": "vmess",
            "server": nodeDomain,
            "port": sessionResult["data"]["port"],
            "uuid": sessionResult["data"]["id"],
            "alterId": 0,
            "cipher": "auto",
            "udp": True,
            "tls": True,
            "network": "ws",
            "ws-opts": {"path": sessionResult["data"]["path"]}
        }
        proxies_list.append(proxy)
        rules_list.append(f"DOMAIN,tiktok.com,{nodeName}")
        rules_list.append(f"DOMAIN,tiktokcdn.com,{nodeName}")
        rules_list.append(f"MATCH,{nodeName}")
    except curl_cffi.requests.exceptions.Timeout:
        logger.fatal(f"获取节点状态超时: {nodeDomain}")
    except Exception as e:
        logger.fatal(f"获取节点状态失败: {e}")

def process_server(server: Dict[str, Any], proxy_groups_list: List[Dict[str, Any]], 
                   proxies_list: List[Dict[str, Any]], rules_list: List[str]):
    """
    处理单个服务器及其所有节点
    """
    if server["id"] == "QQJD":
        return
        
    proxyGroups = {
        "name": server["label_zh"],
        "type": "select",
        "proxies": []
    }
    
    # 使用多线程处理该服务器下的所有节点
    node_threads = []
    for node in server["servers"]:
        node_thread = threading.Thread(
            target=process_node, 
            args=(server["label_zh"], node, proxyGroups, proxies_list, rules_list)
        )
        node_threads.append(node_thread)
        node_thread.start()
    
    # 等待所有节点处理完成
    for node_thread in node_threads:
        node_thread.join()
    
    # 只有当代理组中有代理时才添加到列表中
    if proxyGroups["proxies"]:
        proxy_groups_list.append(proxyGroups)

def getServerConfig():
    global yamlConfig
    yamlConfigPrivate = {
        "port": 7890,
        "socks-port": 1456,
        "allow-lan": False,
        "mode": "rule",
        "log-level": "info",
        "external-controller": "127.0.0.1:9090",
        "proxies": [],
        "proxy-groups": [

        ],
        "rules": []
    }
    if serverListJson == {}:
        logger.fatal(f"未获取到节点列表")
        return
    
    # 用于存储结果的线程安全列表
    proxy_groups_list = []
    proxies_list = []
    rules_list = []
    
    # 使用多线程处理每个服务器
    server_threads = []
    for server in serverListJson["data"]:
        server_thread = threading.Thread(
            target=process_server,
            args=(server, proxy_groups_list, proxies_list, rules_list)
        )
        server_threads.append(server_thread)
        server_thread.start()
    
    # 等待所有服务器处理完成
    for server_thread in server_threads:
        server_thread.join()
    
    # 将结果赋值给yamlConfig
    yamlConfig["proxies"] = proxies_list
    yamlConfig["proxy-groups"] = proxy_groups_list
    yamlConfig["rules"] = rules_list
    print("get ok!")


@app.get("/greenhub/v2ray.yaml")
def get_v2ray_yaml():
    return Response(yaml.dump(yamlConfig), media_type="text/plain")

if __name__ == "__main__":
    # First Get ServerList
    #print(getNodeConfig("gdus009.westmgreen.com"))
    threading.Thread(target=threadGetServerList, daemon=True).start()
    logger.info("初始化节点列表成功!")
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8833)
    # #print(serverListJson)
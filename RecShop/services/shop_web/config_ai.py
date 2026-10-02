"""
AI助手配置文件
优先从环境变量读取配置，如果没有则使用这里的默认值
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# 加载项目根目录 .env 文件（显式路径，不依赖工作目录）
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_PROJECT_ROOT / '.env', override=False)

# DeepSeek API 配置
DEEPSEEK_CONFIG = {
    'api_key': os.environ.get('DEEPSEEK_API_KEY', 'your_api_key_here'),
    'base_url': os.environ.get('DEEPSEEK_API_BASE', 'https://api.deepseek.com/v1'),
    'model': os.environ.get('DEEPSEEK_MODEL', 'deepseek-chat'),
    'temperature': 0.7,
    'max_tokens': 500
}

# 如何配置API Key:
# 方式1（推荐）：设置环境变量 DEEPSEEK_API_KEY
# 方式2：直接修改上面的 'api_key' 字段（不推荐，容易泄露）
#
# 如何获取API Key:
# 1. 访问 https://platform.deepseek.com/
# 2. 注册/登录账号
# 3. 在控制台创建API Key

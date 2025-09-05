# -*- coding: utf-8 -*-

"""
一个功能完整、经过重构和优化的Telegram弹幕机器人脚本。
最终版本：
- 修复了systemd下的启动和关停问题。
- 重构了电视剧导入流程，采用批量导入模式。
- 优化了UI和命令提示，确保示例命令可被复制。
- 【新】增加了API Key自动重置功能，在遭遇持续API速率限制或授权失败时能自动登录并更换Key。
- 【修复】补全了缺失的 start_command 和 help_command 函数定义。
- 【修复】根据F12抓包和日志，修正认证流程为 Bearer Token 模式并校准了Token字段名。
- 【优化】合并了/search和/import命令在无参数时的两条回复为一条。
- 【修复】解决了当列表内容无变化时，点击刷新按钮报错的问题。
- 【修复】修复了直接调用/tasks和/library命令时，因编辑错误消息而报错的问题。
- 【新功能】import_command 已升级，现在可以识别视频页面URL并自动抓取标题进行导入。
- 【功能增强】大幅优化了URL标题清洗逻辑，能更准确地提取媒体名称，提高匹配成功率。
- 【修复-20250905】修正了_get_title_from_url函数中的正则表达式错误，解决了URL解析失败的问题。
- 【修复-20250905】修复了`import`命令在处理多类型结果时因callback_data超长而崩溃的问题，并补全了缺失的回调处理逻辑。
- 【功能调整-20250905】调整了消息删除机制：仅在群聊中自动删除消息，私聊中保留。
- 【功能增强-20250905】重写了 `import` 命令的季数解析逻辑，支持1-99的中文数字季数（如“第三十三季”），大幅提升电视剧导入的识别准确率。
- 【逻辑修复-20250905】修正了从URL解析出季数时，会跳过手动选择而直接自动导入的逻辑问题。
- 【逻辑修复-20250905】增强了`api_call`函数的健壮性，能正确处理API返回空响应体的情况，防止JSON解析错误。
- 【逻辑重构-20250905】重构了`/import`命令，使其在任何情况下都展示搜索结果列表让用户手动选择，避免意外的自动导入。
- 【功能增强-20250905】搜索结果列表（/search 和 /import）现在会显示年份、季数和总集数，信息更丰富。
- 【新功能-20250905】新增 /reboot 命令，采用向自身进程发送SIGTERM信号的方式，配合systemd实现稳定重启。
- 【UI优化-20250905】在 /search 和 /import 的搜索结果列表下方增加了“取消”按钮。
- 【修复-20250905】修复了 /tasks 命令中操作按钮不显示媒体标题的问题，提高了可读性。
- 【UI优化-20250905】为 /tasks 和 /library 命令增加了“取消”按钮。
- 【修复-20250905】修复了 /remove 命令的搜索结果不显示季数和总集数的问题。
- 【功能增强-20250905】为 /tasks 命令增加了分页功能，每页显示20个任务，上限提升至100个。
- 【代码重构-20250905】将分散的配置项（如分页大小、抓取UA等）统一收归AppConfig类管理，增强可维护性。
- 【UI优化-20250905】将 /import 命令空参数提示调整为单行格式。
- 【核心功能升级-20250905】集成 TMDB API！当设置了 TMDB_API_KEY 环境变量后，机器人将通过官方API精确获取中英文标题和年份，彻底解决外语片识别失败问题。未设置时，将自动回退到原有的网页抓取模式。
- 【功能调整-20250905】调整了TMDB API的标题提取逻辑，默认不再附加年份 `(YYYY)`，以提高搜索成功率。此行为可通过 `TMDB_INCLUDE_YEAR` 环境变量控制。
"""

import asyncio
import json
import logging
import os
import re
import signal
import sys
import time
from enum import Enum, auto
from typing import Any, Dict, List, Optional

import httpx
from bs4 import BeautifulSoup
from telegram import (BotCommand, Chat, InlineKeyboardButton,
                      InlineKeyboardMarkup, Message, Update)
from telegram.error import BadRequest
from telegram.ext import (Application, ApplicationBuilder, CallbackQueryHandler,
                          CommandHandler, ContextTypes)

# --- 1. 集中化配置管理 ---
class AppConfig:
    """应用程序配置类"""
    def __init__(self):
        # --- 核心配置 ---
        self.telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN")
        self.danmu_server_url: str = os.getenv("DANMU_SERVER_URL")
        self.danmu_server_api_key: str = os.getenv("DANMU_SERVER_API_KEY")
        admin_ids_str = os.getenv("ADMIN_ID", "")
        self.admin_ids: set[int] = {int(id_str.strip()) for id_str in admin_ids_str.split(',') if id_str.strip()}
        self.danmu_server_admin_user: Optional[str] = os.getenv("DANMU_SERVER_ADMIN_USER")
        self.danmu_server_admin_password: Optional[str] = os.getenv("DANMU_SERVER_ADMIN_PASSWORD")
        
        self.tmdb_api_key: Optional[str] = os.getenv("TMDB_API_KEY")

        # --- 功能配置 ---
        self.request_timeout: int = int(os.getenv("REQUEST_TIMEOUT", "30"))
        self.message_delete_delay: int = int(os.getenv("MESSAGE_DELETE_DELAY", "30"))
        self.max_list_items: int = int(os.getenv("MAX_LIST_ITEMS", "100"))
        self.log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()
        self.enable_hot_reload: bool = os.getenv('ENABLE_HOT_RELOAD', 'false').lower() == 'true'
        self.auto_reset_api_key_enabled: bool = bool(self.danmu_server_admin_user and self.danmu_server_admin_password)

        # --- 全局常量配置 ---
        self.search_page_size: int = int(os.getenv("SEARCH_PAGE_SIZE", "5"))
        self.tasks_page_size: int = int(os.getenv("TASKS_PAGE_SIZE", "20"))
        self.episode_page_size: int = int(os.getenv("EPISODE_PAGE_SIZE", "20"))
        self.scraper_user_agent: str = os.getenv("SCRAPER_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        default_junk_words = "在线观看,高清,完整版,视频,在线,观看,超清,腾讯视频,爱奇艺,优酷,芒果TV,Bilibili,哔哩哔哩,综艺,电影,电视剧,动漫,中文配音,国语,日语"
        self.url_junk_words: List[str] = [word.strip() for word in os.getenv("URL_JUNK_WORDS", default_junk_words).split(',') if word.strip()]
        
        # 【新增】控制TMDB API是否返回年份，默认为False (不返回)
        self.tmdb_include_year: bool = os.getenv('TMDB_INCLUDE_YEAR', 'false').lower() == 'true'

        # --- 启动检查 ---
        if not self.telegram_bot_token:
            raise ValueError("错误: 环境变量 TELEGRAM_BOT_TOKEN 未设置。")
        if not self.danmu_server_url:
            raise ValueError("错误: 环境变量 DANMU_SERVER_URL 未设置。")
        if not self.danmu_server_api_key:
            raise ValueError("错误: 环境变量 DANMU_SERVER_API_KEY 未设置。")

# --- 2. 使用枚举(Enum)定义回调动作 ---
class CallbackAction(Enum):
    PAGE_PREV = "page_prev"; PAGE_NEXT = "page_next"; IMPORT_ITEM = "import_item"; CONFIRM_IMPORT_MOVIE = "confirm_import_movie"; CONFIRM_IMPORT_TV = "confirm_import_tv"; VIEW_TASKS = "view_tasks"; REFRESH_TASKS = "refresh_tasks"; CLEAR_COMPLETED_TASKS = "clear_tasks"; PAUSE_TASK = "pause_task"; RESUME_TASK = "resume_task"; ABORT_TASK = "abort_task"; DELETE_TASK = "delete_task"; VIEW_LIBRARY = "view_library"; REFRESH_LIBRARY = "refresh_library"; REQUEST_DELETE_CONFIRM = "req_del_confirm"; EXECUTE_DELETE = "exec_del"; CANCEL_DELETE = "cancel_del"; SHOW_EPISODE_SELECTION = "show_episodes"; PAGE_EPISODE_SELECTION = "page_episodes"; TOGGLE_EPISODE_SELECT = "toggle_episode"; SELECT_ALL_EPISODES = "select_all_ep"; CLEAR_EPISODE_SELECTION = "clear_ep_sel"; BATCH_IMPORT_EPISODES = "batch_import"
    TASKS_PAGE_PREV = "tasks_page_prev"; TASKS_PAGE_NEXT = "tasks_page_next"
    CANCEL_MESSAGE = "cancel_message"

# --- 3. 日志配置 ---
def setup_logging(log_level: str):
    level = getattr(logging, log_level, logging.INFO)
    logging.basicConfig(level=level, format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s] - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('telegram').setLevel(logging.WARNING)
    return logging.getLogger(__name__)

# --- 4. 核心服务：配置实例和日志记录器 ---
try:
    config = AppConfig()
    logger = setup_logging(config.log_level)
    if config.auto_reset_api_key_enabled:
        logger.info("✅ 检测到管理员凭据，API Key自动重置功能已启用。")
    else:
        logger.warning("⚠️ 未配置管理员凭据 (DANMU_SERVER_ADMIN_USER/PASSWORD)，API Key自动重置功能已禁用。")
    if config.tmdb_api_key:
        logger.info("✅ TMDB API Key 已配置，将启用API元数据抓取功能。")
    else:
        logger.warning("⚠️ 未配置 TMDB_API_KEY，TMDB/IMDb链接解析将回退到网页抓取模式，可能不准确。")
except ValueError as e:
    logging.basicConfig(level=logging.CRITICAL, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.critical(e)
    sys.exit(1)

# --- 5. 辅助函数 ---
def schedule_message_deletion(context: ContextTypes.DEFAULT_TYPE, message: Message):
    if message.chat.type != Chat.PRIVATE:
        delay = config.message_delete_delay
        context.application.job_queue.run_once(
            lambda ctx: ctx.bot.delete_message(chat_id=message.chat_id, message_id=message.message_id),
            delay,
            name=f"delete_{message.chat_id}_{message.message_id}"
        )

def chinese_to_arabic(cn_num_str: str) -> Optional[int]:
    if not cn_num_str: return None
    if cn_num_str.isdigit():
        try: return int(cn_num_str)
        except ValueError: return None
    cn_map = {'零': 0, '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9}
    if cn_num_str == '十': return 10
    num = 0
    if '十' in cn_num_str:
        parts = cn_num_str.split('十')
        if parts[0] == '': num = 10
        else:
            if parts[0] not in cn_map: return None
            num = cn_map[parts[0]] * 10
        if len(parts) > 1 and parts[1] != '':
            if parts[1] not in cn_map: return None
            num += cn_map[parts[1]]
    elif cn_num_str in cn_map: num = cn_map[cn_num_str]
    else: return None
    return num if num > 0 else None

# 【已修改】根据配置决定是否返回年份
async def _get_title_from_tmdb_api(media_type: str, tmdb_id: str, client: httpx.AsyncClient) -> Optional[str]:
    api_url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}?api_key={config.tmdb_api_key}&language=zh-CN"
    try:
        response = await client.get(api_url, timeout=15)
        response.raise_for_status()
        data = response.json()
        title = data.get('title') or data.get('name')
        if not title:
            return None
        
        if config.tmdb_include_year:
            date = data.get('release_date') or data.get('first_air_date')
            year = f"({date.split('-')[0]})" if date else ""
            return f"{title} {year}".strip()
        else:
            return title

    except httpx.RequestError as e:
        logger.error(f"请求TMDB API失败: {e}")
    except json.JSONDecodeError:
        logger.error("解析TMDB API响应失败。")
    return None

async def _get_tmdb_id_from_imdb_id(imdb_id: str, client: httpx.AsyncClient) -> Optional[tuple[str, str]]:
    api_url = f"https://api.themoviedb.org/3/find/{imdb_id}?api_key={config.tmdb_api_key}&external_source=imdb_id"
    try:
        response = await client.get(api_url, timeout=15)
        response.raise_for_status()
        data = response.json()
        if data.get('movie_results'):
            return "movie", str(data['movie_results'][0]['id'])
        if data.get('tv_results'):
            return "tv", str(data['tv_results'][0]['id'])
    except Exception as e:
        logger.error(f"通过IMDb ID查找TMDB ID失败: {e}")
    return None

async def _get_title_from_url(url: str, client: httpx.AsyncClient) -> Optional[str]:
    # 优先策略：如果配置了TMDB API Key，则通过API获取标题
    if config.tmdb_api_key:
        tmdb_match = re.search(r'themoviedb\.org/(movie|tv)/(\d+)', url)
        imdb_match = re.search(r'imdb\.com/title/(tt\d+)', url)

        try:
            if tmdb_match:
                media_type, tmdb_id = tmdb_match.groups()
                logger.info(f"检测到TMDB链接，类型: {media_type}, ID: {tmdb_id}。正在使用API获取标题...")
                return await _get_title_from_tmdb_api(media_type, tmdb_id, client)
            
            if imdb_match:
                imdb_id = imdb_match.group(1)
                logger.info(f"检测到IMDb链接，ID: {imdb_id}。正在查找对应的TMDB ID...")
                tmdb_info = await _get_tmdb_id_from_imdb_id(imdb_id, client)
                if tmdb_info:
                    media_type, tmdb_id = tmdb_info
                    logger.info(f"找到TMDB ID: {tmdb_id}，类型: {media_type}。正在使用API获取标题...")
                    return await _get_title_from_tmdb_api(media_type, tmdb_id, client)
                else:
                    logger.warning("未能从IMDb ID找到匹配的TMDB条目。")

        except Exception as e:
            logger.error(f"处理TMDB API时发生未知错误: {e}。将回退到网页抓取模式。")

    # 回退策略：使用网页抓取
    logger.info("正在使用网页抓取模式解析标题...")
    try:
        headers = {"User-Agent": config.scraper_user_agent, "Accept-Language": "zh-CN,zh;q=0.9"}
        response = await client.get(url, headers=headers, follow_redirects=True, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        raw_title = None
        
        # 抓取逻辑...
        og_title = soup.find('meta', property='og:title')
        if og_title and og_title.get('content'): raw_title = og_title['content']
        elif soup.title and soup.title.string: raw_title = soup.title.string.strip()
        elif soup.find('h1'): raw_title = soup.find('h1').get_text(strip=True)
        
        if not raw_title:
            logger.warning(f"无法从URL {url} 中找到任何明确的标题标签。")
            return None
            
        logger.info(f"从网页成功提取原始标题: {raw_title}")

        # 清洗逻辑...
        cleaned_title = re.split(r'[-_–—|]', raw_title)[0].strip()
        cleaned_title = re.sub(r'\s*\(\d{4}\)\s*-\s*IMDb$', '', cleaned_title, flags=re.IGNORECASE).strip()
        cleaned_title = re.sub(r'^(.*?)(第\s*\d+\s*[期集季话]|EP\d+|S\d+E\d+)', r'\1', cleaned_title, flags=re.IGNORECASE).strip()
        cleaned_title = re.sub(r'：.*', '', cleaned_title, 1).strip()
        junk_regex = '|'.join(map(re.escape, config.url_junk_words))
        cleaned_title = re.sub(junk_regex, '', cleaned_title, flags=re.IGNORECASE)
        is_metadata_site = re.match(r'https?://(www\.)?(imdb\.com/title/|themoviedb\.org/(movie|tv)/)', url)
        if not is_metadata_site:
             cleaned_title = re.sub(r'[\(（【\[].*?[\)）】\]]', '', cleaned_title)

        cleaned_title = re.sub(r'^[^\w\u4e00-\u9fa5]+|[^\w\u4e00-\u9fa5]+$', '', cleaned_title)
        cleaned_title = ' '.join(cleaned_title.split())

        logger.info(f"清洗后的最终标题: {cleaned_title}")
        return cleaned_title if cleaned_title else None
        
    except httpx.RequestError as e: logger.error(f"访问URL {url} 失败: {e}"); return None
    except Exception as e: logger.error(f"解析URL {url} 时发生未知错误: {e}"); return None


async def _reset_api_key(context: ContextTypes.DEFAULT_TYPE, reason: str) -> bool:
    logger.warning(f"API调用因“{reason}”彻底失败，正在尝试自动重置API Key...")
    login_url = f"{config.danmu_server_url}/api/ui/auth/token"
    reset_url = f"{config.danmu_server_url}/api/ui/config/externalApiKey/regenerate"
    async with httpx.AsyncClient(follow_redirects=True) as admin_client:
        try:
            login_payload = {"username": config.danmu_server_admin_user, "password": config.danmu_server_admin_password}
            login_resp = await admin_client.post(login_url, data=login_payload, timeout=config.request_timeout)
            login_resp.raise_for_status()
            login_json = login_resp.json()
            access_token = login_json.get("accessToken")
            if not access_token: logger.error(f"登录成功但未能从响应中获取 accessToken。响应: {login_json}"); return False
            logger.info("管理员登录成功并获取到Access Token，准备刷新API Key...")
            request_headers = { "Authorization": f"Bearer {access_token}" }
            reset_resp = await admin_client.post(reset_url, headers=request_headers, timeout=config.request_timeout)
            reset_resp.raise_for_status()
            response_json = reset_resp.json()
            new_key = response_json.get("value")
            if not new_key: logger.error(f"API Key重置失败：服务器未返回新的Key。收到的响应: {response_json}"); return False
            context.bot_data['danmu_server_api_key'] = new_key
            logger.info(f"🎉 API Key自动重置成功！新的Key已启用。")
            for admin_id in config.admin_ids:
                await context.bot.send_message(chat_id=admin_id, text=f"ℹ️ **通知：API Key已自动重置** ℹ️\n\n机器人检测到API因“{reason}”持续失败，并已成功自动重置并更新了API Key。服务应已恢复正常。", parse_mode="Markdown")
            return True
        except httpx.HTTPStatusError as e: logger.error(f"API Key重置失败：请求失败，状态码 {e.response.status_code}。响应: {e.response.text}。请检查管理员凭据和服务器URL。"); return False
        except Exception as e: logger.error(f"API Key自动重置过程中发生严重错误: {e}"); return False

async def api_call(context: ContextTypes.DEFAULT_TYPE, method: str, endpoint: str, retries: int = 3, **kwargs) -> Dict[str, Any]:
    client: httpx.AsyncClient = context.bot_data.get('http_client')
    if not client: raise RuntimeError("HTTP Client not found in bot_data.")
    url = f"{config.danmu_server_url}{endpoint}"
    params = kwargs.get("params", {}); params["api_key"] = context.bot_data.get('danmu_server_api_key', config.danmu_server_api_key); kwargs["params"] = params
    last_exception = None; should_trigger_reset = False; reset_reason = ""
    for attempt in range(retries):
        try:
            kwargs["params"]["api_key"] = context.bot_data.get('danmu_server_api_key')
            response = await client.request(method, url, **kwargs); response.raise_for_status()
            if not response.text or not response.text.strip(): logger.warning(f"API 端点 {endpoint} 返回了成功状态码但响应体为空。将返回一个空字典。"); return {}
            return response.json()
        except httpx.HTTPStatusError as e:
            last_exception = e
            if e.response.status_code == 429:
                should_trigger_reset = True; reset_reason = "速率限制"; wait_time = 2 ** (attempt + 1)
                logger.warning(f"收到 429 错误，第 {attempt + 1}/{retries} 次重试将在 {wait_time} 秒后开始..."); await asyncio.sleep(wait_time); continue
            if e.response.status_code == 401: should_trigger_reset = True; reset_reason = "授权失败"; logger.warning("收到 401 错误，当前API Key可能已失效。"); break 
            else: logger.error(f"HTTP错误: {e.response.status_code} - {e.response.text}"); raise ValueError(f"服务器返回错误: `{e.response.status_code}`") from e
        except httpx.RequestError as e: last_exception = e; logger.error(f"调用API失败: {e}"); raise ValueError("服务器连接失败。") from e
    if should_trigger_reset and config.auto_reset_api_key_enabled:
        if await _reset_api_key(context, reset_reason):
            logger.info("使用新的API Key重试原始请求...")
            kwargs["params"]["api_key"] = context.bot_data.get('danmu_server_api_key')
            try:
                response = await client.request(method, url, **kwargs); response.raise_for_status()
                if not response.text or not response.text.strip(): logger.warning(f"API 端点 {endpoint} 在重试后返回了成功状态码但响应体为空。将返回一个空字典。"); return {}
                logger.info("使用新Key重试成功！"); return response.json()
            except Exception as retry_e: logger.error(f"使用新Key重试失败: {retry_e}"); last_exception = retry_e
    raise ValueError(f"API调用在多次尝试后仍然失败。") from last_exception

async def _execute_auto_import(message: Message, context: ContextTypes.DEFAULT_TYPE, term: str, media_type: str, season: Optional[int]):
    status_msg_text = f"⏳ 正在自动导入 {'电视剧' if media_type == 'tv_series' else '电影'} `{term}`..."
    status_msg = await message.reply_text(status_msg_text, parse_mode="Markdown")
    try:
        import_payload = {"searchType": "keyword", "searchTerm": term, "mediaType": media_type}
        if season is not None: import_payload["season"] = season
        response_data = await api_call(context, "POST", "/api/control/import/auto", params=import_payload)
        success_text = f"✅ 自动导入任务已提交！\n- 任务ID: `{response_data.get('taskId')}`\n- 状态: `{response_data.get('message')}`"
        keyboard = [[InlineKeyboardButton("👀 查看任务列表", callback_data=json.dumps({"action": CallbackAction.VIEW_TASKS.value}))]]
        await status_msg.edit_text(success_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    except ValueError as e: await status_msg.edit_text(f"❌ 自动导入失败: {e}", parse_mode="Markdown"); schedule_message_deletion(context, status_msg)

async def _display_tasks_list(update: Update, context: ContextTypes.DEFAULT_TYPE, message_to_edit: Optional[Message] = None, page: int = 1):
    query = update.callback_query
    message = message_to_edit or (query.message if query else None)
    if not message:
        logger.error("在 _display_tasks_list 中未能确定要编辑的消息。")
        return

    page_size = config.tasks_page_size
    try:
        if 'displayed_tasks' not in context.user_data or not query:
             all_tasks = await api_call(context, "GET", "/api/control/tasks", params={"status": "all"})
             tasks_to_process = [t for t in all_tasks if t.get("status") != "已完成"]
             tasks_to_process.sort(key=lambda x: x.get('creationTime', 0), reverse=True)
             context.user_data['displayed_tasks'] = tasks_to_process[:config.max_list_items]
        
        tasks_to_process = context.user_data.get('displayed_tasks', [])

        start_index = (page - 1) * page_size
        end_index = page * page_size
        tasks_to_display = tasks_to_process[start_index:end_index]
        
        total_pages = (len(tasks_to_process) + page_size - 1) // page_size or 1
        message_text = f"📄 **任务列表 (第 {page}/{total_pages} 页, 共 {len(tasks_to_process)} 项)**\n\n"
        keyboard = []

        if not tasks_to_display:
            message_text = "📄 **任务列表**\n\n当前没有活动任务。"
        else:
            for index, task in enumerate(tasks_to_display):
                full_list_index = start_index + index
                status, title = task.get("status", "未知"), task.get('title', '无标题')
                message_text += f"**{full_list_index + 1}.** `{title}`\n    状态: `{status}` ({task.get('progress', 0)}%)\n"
                
                action_buttons = []
                cleaned_title = title
                if cleaned_title.startswith("导入:"):
                    cleaned_title = cleaned_title[3:].lstrip()
                short_title = cleaned_title[:10] + '...' if len(cleaned_title) > 10 else cleaned_title
                
                if message.chat.id in config.admin_ids:
                    if status == "运行中":
                        action_buttons.extend([
                            InlineKeyboardButton(f"⏸️ {short_title}", callback_data=json.dumps({"action": CallbackAction.PAUSE_TASK.value, "idx": full_list_index})),
                            InlineKeyboardButton(f"⏹️ {short_title}", callback_data=json.dumps({"action": CallbackAction.ABORT_TASK.value, "idx": full_list_index}))
                        ])
                    elif status == "已暂停":
                        action_buttons.extend([
                            InlineKeyboardButton(f"▶️ {short_title}", callback_data=json.dumps({"action": CallbackAction.RESUME_TASK.value, "idx": full_list_index})),
                            InlineKeyboardButton(f"⏹️ {short_title}", callback_data=json.dumps({"action": CallbackAction.ABORT_TASK.value, "idx": full_list_index}))
                        ])
                    elif status in ["失败", "已中止", "排队中"]:
                        action_buttons.append(
                            InlineKeyboardButton(f"🗑️ {short_title}", callback_data=json.dumps({"action": CallbackAction.DELETE_TASK.value, "idx": full_list_index}))
                        )
                if action_buttons:
                    keyboard.append(action_buttons)

        pagination_row = []
        if page > 1:
            pagination_row.append(InlineKeyboardButton("⬅️ 上一页", callback_data=json.dumps({"action": CallbackAction.TASKS_PAGE_PREV.value, "p": page - 1})))
        if page < total_pages:
            pagination_row.append(InlineKeyboardButton("下一页 ➡️", callback_data=json.dumps({"action": CallbackAction.TASKS_PAGE_NEXT.value, "p": page + 1})))
        
        if pagination_row:
            keyboard.append(pagination_row)

        control_row = [InlineKeyboardButton("🔄 刷新", callback_data=json.dumps({"action": CallbackAction.REFRESH_TASKS.value, "p": page}))]
        if message.chat.id in config.admin_ids:
            control_row.append(InlineKeyboardButton("清理已完成", callback_data=json.dumps({"action": CallbackAction.CLEAR_COMPLETED_TASKS.value})))
        keyboard.append(control_row)
        keyboard.append([InlineKeyboardButton("❌ 取消", callback_data=json.dumps({"action": CallbackAction.CANCEL_MESSAGE.value}))])

        await message.edit_text(message_text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
        if query: await query.answer("✅ 列表已刷新")

    except BadRequest as e:
        if "Message is not modified" in str(e):
            if query: await query.answer("列表已是最新，无需刷新。")
        else:
            logger.error(f"编辑任务列表时发生BadRequest错误: {e}")
            if query: await query.answer("❌ 操作失败", show_alert=True)
    except ValueError as e:
        await message.edit_text(f"❌ 获取任务列表失败: {e}", parse_mode="Markdown")

async def _display_library(update: Update, context: ContextTypes.DEFAULT_TYPE, message_to_edit: Optional[Message] = None):
    query = update.callback_query; message = message_to_edit or (query.message if query else None)
    if not message: logger.error("在 _display_library 中未能确定要编辑的消息。"); return
    try:
        library_items = await api_call(context, "GET", "/api/control/library"); message_text = "📄 **弹幕库列表**\n\n"
        if not library_items: message_text += "弹幕库为空。"
        else:
            for i, item in enumerate(library_items[:config.max_list_items]):
                icon = "📺" if item.get("type") == "tv_series" else "🎬"; message_text += f"{icon} **{i+1}.** `{item.get('title', '无标题')}` ({item.get('year', '?')})\n"
        keyboard = [
            [InlineKeyboardButton("🔄 刷新", callback_data=json.dumps({"action": CallbackAction.REFRESH_LIBRARY.value}))],
            [InlineKeyboardButton("❌ 取消", callback_data=json.dumps({"action": CallbackAction.CANCEL_MESSAGE.value}))]
        ]
        await message.edit_text(message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
        if query: await query.answer("✅ 媒体库已刷新")
    except BadRequest as e:
        if "Message is not modified" in str(e):
            if query: await query.answer("媒体库已是最新，无需刷新。")
        else: logger.error(f"编辑媒体库时发生BadRequest错误: {e}"); await query.answer("❌ 操作失败", show_alert=True)
    except ValueError as e: await message.edit_text(f"❌ 获取弹幕库失败: {e}", parse_mode="Markdown")

async def _display_episode_selection(message: Message, context: ContextTypes.DEFAULT_TYPE, anime_id: str, page: int = 1):
    page_size = config.episode_page_size
    try:
        ep_list_key = f"ep_list_{anime_id}"
        if ep_list_key not in context.bot_data:
            episodes_data = await api_call(context, "GET", f"/api/control/library/anime/{anime_id}/episodes")
            context.bot_data[ep_list_key] = episodes_data
        else: episodes_data = context.bot_data[ep_list_key]
        all_episodes: List[Dict] = episodes_data.get("episodes", []); anime_title = episodes_data.get("title", "未知作品")
        if not all_episodes: await message.edit_text("❌ 未能获取到该作品的剧集列表。"); return
        selection_key = f"selection_{anime_id}_{message.chat.id}"; selected_episodes: set = context.user_data.setdefault(selection_key, set())
        keyboard = []; message_text = f"📄 **{anime_title}**\n请选择要导入的剧集 (已选: {len(selected_episodes)}集):"
        start_index, end_index = (page - 1) * page_size, page * page_size; page_episodes = all_episodes[start_index:end_index]; row = []
        for episode in page_episodes:
            ep_num = episode.get("episodeNo"); is_selected = ep_num in selected_episodes; button_text = f"{'✅' if is_selected else ''}{ep_num}"
            callback_data = json.dumps({"action": CallbackAction.TOGGLE_EPISODE_SELECT.value, "id": anime_id, "ep": ep_num, "p": page})
            row.append(InlineKeyboardButton(button_text, callback_data=callback_data))
            if len(row) == 5: keyboard.append(row); row = []
        if row: keyboard.append(row)
        total_pages = (len(all_episodes) + page_size - 1) // page_size or 1; pagination_row = []
        if page > 1: pagination_row.append(InlineKeyboardButton("⬅️ 上一页", callback_data=json.dumps({"action": CallbackAction.PAGE_EPISODE_SELECTION.value, "id": anime_id, "p": page - 1})))
        pagination_row.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"))
        if page < total_pages: pagination_row.append(InlineKeyboardButton("下页 ➡️", callback_data=json.dumps({"action": CallbackAction.PAGE_EPISODE_SELECTION.value, "id": anime_id, "p": page + 1})))
        if pagination_row: keyboard.append(pagination_row)
        action_row = [InlineKeyboardButton("✅ 全选所有", callback_data=json.dumps({"action": CallbackAction.SELECT_ALL_EPISODES.value, "id": anime_id, "p": page})), InlineKeyboardButton("🗑️ 清空选择", callback_data=json.dumps({"action": CallbackAction.CLEAR_EPISODE_SELECTION.value, "id": anime_id, "p": page}))]
        keyboard.append(action_row)
        if selected_episodes: keyboard.append([InlineKeyboardButton(f"🚀 开始导入 {len(selected_episodes)} 集", callback_data=json.dumps({"action": CallbackAction.BATCH_IMPORT_EPISODES.value, "id": anime_id}))])
        keyboard.append([InlineKeyboardButton("❌ 取消", callback_data=json.dumps({"action": CallbackAction.CANCEL_MESSAGE.value}))])
        await message.edit_text(message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    except ValueError as e: await message.edit_text(f"❌ 操作失败: {e}")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    schedule_message_deletion(context, update.message); reply = await update.message.reply_text("欢迎使用弹幕机器人！\n使用 /help 查看所有可用指令。"); schedule_message_deletion(context, reply)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    schedule_message_deletion(context, update.message)
    help_text = "📄 **指令说明**\n\n`/start` - 启动机器人\n`/help` - 获取帮助说明\n`/search <名称>` - 交互式搜索电影或电视剧\n`/import <名称或URL>` - 智能导入\n`/tasks` - 查看和管理后台任务\n`/library` - 查看已收录的弹幕库\n`/remove <名称>` - [管理员] 搜索并删除作品\n`/reboot` - [管理员] 重启机器人"
    reply = await update.message.reply_text(help_text, parse_mode="Markdown")
    schedule_message_deletion(context, reply)

async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    is_callback = update.callback_query is not None
    if not is_callback:
        schedule_message_deletion(context, update.message)
        query_text = " ".join(context.args)
        if not query_text:
            help_text = "请提供搜索关键词。 **用法示例:** `/search 你的名字`"
            await update.message.reply_text(help_text, parse_mode="Markdown")
            return
        context.user_data['search_query'] = query_text; context.user_data['search_start_index'] = 0
        message_to_edit = await update.message.reply_text(f"⏳ 正在搜索 `{query_text}`...", parse_mode="Markdown")
        try:
            search_data = await api_call(context, "GET", "/api/control/search", params={"keyword": query_text})
            context.user_data['last_search_results'] = search_data.get('results', [])
        except ValueError as e: await message_to_edit.edit_text(f"❌ 搜索失败: {e}"); return
    else: message_to_edit = update.callback_query.message
    search_results = context.user_data.get('last_search_results', [])
    if not search_results: await message_to_edit.edit_text("未找到任何匹配结果。"); return
    start_index = context.user_data.get('search_start_index', 0)
    page_size = config.search_page_size
    current_page_results = search_results[start_index : start_index + page_size]; keyboard = []
    for i, result in enumerate(current_page_results):
        index_in_full_list = start_index + i
        title, year = result.get("title", "无标题"), result.get("year", "未知"); icon = "📺" if result.get("type") == "tv_series" else "🎬"
        button_text = f"{icon} {title} ({year})"
        if result.get("type") == "tv_series":
            extra_details = []
            season = result.get("season"); episode_count = result.get("episodeCount")
            if season: extra_details.append(f"季:{season}")
            if episode_count: extra_details.append(f"总集:{episode_count}")
            if extra_details: button_text += f" - {' | '.join(extra_details)}"
        callback_data = json.dumps({"action": CallbackAction.IMPORT_ITEM.value, "idx": index_in_full_list})
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
    pagination_buttons = []
    if start_index > 0: pagination_buttons.append(InlineKeyboardButton("⬅️ 上一页", callback_data=json.dumps({"action": CallbackAction.PAGE_PREV.value})))
    if len(search_results) > start_index + page_size: pagination_buttons.append(InlineKeyboardButton("下一页 ➡️", callback_data=json.dumps({"action": CallbackAction.PAGE_NEXT.value})))
    if pagination_buttons: keyboard.append(pagination_buttons)
    keyboard.append([InlineKeyboardButton("❌ 取消", callback_data=json.dumps({"action": CallbackAction.CANCEL_MESSAGE.value}))])
    await message_to_edit.edit_text("请选择要导入的条目：", reply_markup=InlineKeyboardMarkup(keyboard))

async def import_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    schedule_message_deletion(context, update.message)
    if not context.args:
        help_text = "❌ **用法错误** 请使用 `/import 庆余年` 或者 `/import <视频URL>`"
        await update.message.reply_text(help_text, parse_mode="Markdown")
        return
    is_from_url = False; term = " ".join(context.args); status_msg = None
    if re.match(r'https?://\S+', term):
        is_from_url = True
        status_msg = await update.message.reply_text(f"🔗 检测到URL，正在尝试解析标题...")
        client: httpx.AsyncClient = context.bot_data.get('http_client')
        if not client: await status_msg.edit_text("❌ 内部错误：HTTP客户端未初始化。"); return
        extracted_title = await _get_title_from_url(term, client)
        if not extracted_title: await status_msg.edit_text("❌ 解析失败，无法从该URL中获取到有效的媒体标题。"); schedule_message_deletion(context, status_msg); return
        term = extracted_title
        await status_msg.edit_text(f"✅ 成功解析标题为: `{term}`\n⏳ 正在搜索此标题...", parse_mode="Markdown")
    season = None; final_term = term.strip()
    match = re.search(r'^(.*?)\s*(?:第([一二三四五六七八九十\d]+)季|S(\d+)|Season\s*(\d+))$', final_term, re.IGNORECASE)
    if match:
        final_term = match.group(1).strip()
        season_str = next((s for s in [match.group(2), match.group(3), match.group(4)] if s), None)
        if season_str: season = chinese_to_arabic(season_str)
    if season is None:
        args_list = final_term.split()
        if len(args_list) > 1 and args_list[-1].isdigit():
            season_val = args_list.pop()
            if season_val.isdigit() and int(season_val) > 0: season = int(season_val); final_term = " ".join(args_list)
    if season and not is_from_url: await _execute_auto_import(update.message, context, final_term, 'tv_series', season); return
    if not status_msg: status_msg = await update.message.reply_text(f"⏳ 正在搜索 `{final_term}`...", parse_mode="Markdown")
    try:
        search_results = (await api_call(context, "GET", "/api/control/search", params={"keyword": final_term})).get('results', [])
        if not search_results: await status_msg.edit_text(f"❌ 找不到与 `{final_term}` 匹配的作品。"); schedule_message_deletion(context, status_msg); return
        context.user_data['last_search_results'] = search_results; context.user_data['search_start_index'] = 0
        
        start_index = 0
        page_size = config.search_page_size
        current_page_results = search_results[start_index : start_index + page_size]; keyboard = []
        for i, result in enumerate(current_page_results):
            index_in_full_list = start_index + i
            title, year = result.get("title", "无标题"), result.get("year", "未知"); icon = "📺" if result.get("type") == "tv_series" else "🎬"
            button_text = f"{icon} {title} ({year})"
            if result.get("type") == "tv_series":
                extra_details = []
                season = result.get("season"); episode_count = result.get("episodeCount")
                if season: extra_details.append(f"季:{season}")
                if episode_count: extra_details.append(f"总集:{episode_count}")
                if extra_details: button_text += f" - {' | '.join(extra_details)}"
            callback_data = json.dumps({"action": CallbackAction.IMPORT_ITEM.value, "idx": index_in_full_list})
            keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
        pagination_buttons = []
        if start_index > 0: pagination_buttons.append(InlineKeyboardButton("⬅️ 上一页", callback_data=json.dumps({"action": CallbackAction.PAGE_PREV.value})))
        if len(search_results) > start_index + page_size: pagination_buttons.append(InlineKeyboardButton("下一页 ➡️", callback_data=json.dumps({"action": CallbackAction.PAGE_NEXT.value})))
        if pagination_buttons: keyboard.append(pagination_buttons)
        keyboard.append([InlineKeyboardButton("❌ 取消", callback_data=json.dumps({"action": CallbackAction.CANCEL_MESSAGE.value}))])
        await status_msg.edit_text("请选择要导入的条目：", reply_markup=InlineKeyboardMarkup(keyboard))
    except ValueError as e: await status_msg.edit_text(f"❌ 智能分析失败: {e}", parse_mode="Markdown")

async def remove_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    schedule_message_deletion(context, update.message)
    if update.message.from_user.id not in config.admin_ids:
        reply = await update.message.reply_text("❌ 您没有权限使用此命令。"); schedule_message_deletion(context, reply); return
    keyword = " ".join(context.args)
    message = await update.message.reply_text(f"⏳ 正在弹幕库中搜索 `{keyword}`..." if keyword else "⏳ 正在获取整个弹幕库...")
    try:
        library_items = await api_call(context, "GET", "/api/control/library")
        matched_items = [item for item in library_items if not keyword or keyword.lower() in item.get("title", "").lower()]
        context.user_data['remove_list'] = matched_items
        if not matched_items: await message.edit_text(f"在弹幕库中未找到与 `{keyword}` 匹配的作品。"); return
        keyboard = []
        for index, item in enumerate(matched_items[:config.max_list_items]):
            title, year = item.get("title", "无标题"), item.get("year", "")
            item_type = item.get("type")
            icon = "📺" if item_type == "tv_series" else "🎬"
            button_text = f"{index + 1}. {icon} {title} ({year})"
            
            if item_type == "tv_series":
                extra_details = []
                season = item.get("season")
                episode_count = item.get("episodeCount")
                if season: extra_details.append(f"季:{season}")
                if episode_count: extra_details.append(f"总集:{episode_count}")
                if extra_details: button_text += f" - {' | '.join(extra_details)}"

            callback_data = json.dumps({"action": CallbackAction.REQUEST_DELETE_CONFIRM.value, "idx": index})
            keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
        
        keyboard.append([InlineKeyboardButton("❌ 取消", callback_data=json.dumps({"action": CallbackAction.CANCEL_MESSAGE.value}))])
        await message.edit_text("请选择要删除的作品：", reply_markup=InlineKeyboardMarkup(keyboard))
        schedule_message_deletion(context, message)
    except ValueError as e: await message.edit_text(f"❌ 获取弹幕库失败: {e}")

async def reboot_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id not in config.admin_ids:
        await update.message.reply_text("❌ 您没有权限执行此操作。")
        return
    await update.message.reply_text(
        "✅ **正在发送重启信号...**\n"
        "机器人将平滑关停，systemd 会自动重启。",
        parse_mode="Markdown"
    )
    await asyncio.sleep(1)
    os.kill(os.getpid(), signal.SIGTERM)

async def tasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if 'displayed_tasks' in context.user_data:
        del context.user_data['displayed_tasks']
    schedule_message_deletion(context, update.message)
    message = await update.message.reply_text("⏳ 正在获取任务列表...")
    await _display_tasks_list(update, context, message_to_edit=message, page=1)

async def library_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    schedule_message_deletion(context, update.message); message = await update.message.reply_text("⏳ 正在获取弹幕库列表..."); await _display_library(update, context, message_to_edit=message)

async def main_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query; await query.answer()
    try:
        data = json.loads(query.data);
        if data == "noop": return
        action = CallbackAction(data.get("action"))
    except (json.JSONDecodeError, ValueError, AttributeError): logger.warning(f"无法解析或未知的回调数据: {query.data}"); return
    user_id = query.from_user.id
    if action == CallbackAction.CANCEL_MESSAGE:
        try: await query.message.delete()
        except BadRequest: pass
        return
    if action in [CallbackAction.PAGE_PREV, CallbackAction.PAGE_NEXT]:
        current_index = context.user_data.get('search_start_index', 0)
        page_size = config.search_page_size
        new_index = current_index + page_size if action == CallbackAction.PAGE_NEXT else max(0, current_index - page_size)
        context.user_data['search_start_index'] = new_index
        await search_command(update, context)
    elif action in [CallbackAction.TASKS_PAGE_PREV, CallbackAction.TASKS_PAGE_NEXT]:
        page = data.get("p", 1)
        await _display_tasks_list(update, context, page=page)
    elif action == CallbackAction.IMPORT_ITEM:
        result_index = data["idx"]; selected = context.user_data.get('last_search_results', [])[result_index]; media_type = selected.get("type")
        if media_type == 'tv_series': await _display_episode_selection(query.message, context, selected.get("animeId"))
        else: await query.message.edit_text(f"⏳ 正在导入 `{selected.get('title')}`...", reply_markup=None); await _execute_auto_import(query.message, context, selected.get("title"), selected.get("type"), selected.get("season"))
    elif action == CallbackAction.CONFIRM_IMPORT_MOVIE:
        term = context.user_data.get('import_term')
        if not term: await query.message.edit_text("❌ 操作已过期或失败，请重新发起导入。"); return
        await query.message.delete(); await _execute_auto_import(query.message, context, term, 'movie', None); context.user_data.pop('import_term', None)
    elif action == CallbackAction.CONFIRM_IMPORT_TV:
        term = context.user_data.get('import_term')
        if not term: await query.message.edit_text("❌ 操作已过期或失败，请重新发起导入。"); return
        await query.message.delete(); season = data.get("season", 1); await _execute_auto_import(query.message, context, term, 'tv_series', season); context.user_data.pop('import_term', None)
    elif action == CallbackAction.PAGE_EPISODE_SELECTION: await _display_episode_selection(query.message, context, data["id"], data["p"])
    elif action == CallbackAction.TOGGLE_EPISODE_SELECT:
        anime_id, ep_num, page = data["id"], data["ep"], data["p"]; selection_key = f"selection_{anime_id}_{query.message.chat.id}"; selection_set = context.user_data.setdefault(selection_key, set())
        if ep_num in selection_set: selection_set.remove(ep_num)
        else: selection_set.add(ep_num)
        await _display_episode_selection(query.message, context, anime_id, page)
    elif action == CallbackAction.SELECT_ALL_EPISODES:
        anime_id, page = data["id"], data["p"]; ep_list = context.bot_data.get(f"ep_list_{anime_id}", {}).get("episodes", []); all_ep_nums = {ep.get("episodeNo") for ep in ep_list}
        selection_key = f"selection_{anime_id}_{query.message.chat.id}"; context.user_data[selection_key] = all_ep_nums; await _display_episode_selection(query.message, context, anime_id, page)
    elif action == CallbackAction.CLEAR_EPISODE_SELECTION:
        anime_id, page = data["id"], data["p"]; selection_key = f"selection_{anime_id}_{query.message.chat.id}"; context.user_data[selection_key] = set(); await _display_episode_selection(query.message, context, anime_id, page)
    elif action == CallbackAction.BATCH_IMPORT_EPISODES:
        anime_id = data["id"]; selection_key = f"selection_{anime_id}_{query.message.chat.id}"; selected_episodes = sorted(list(context.user_data.get(selection_key, [])))
        if not selected_episodes: await query.answer("您还没有选择任何剧集！", show_alert=True); return
        await query.message.edit_text(f"⏳ 正在提交批量导入任务 (共 {len(selected_episodes)} 集)...", reply_markup=None)
        try:
            response = await api_call(context, "POST", f"/api/control/library/anime/{anime_id}/episodes/import", json={"episode_numbers": selected_episodes})
            success_text = f"✅ 批量导入任务已提交！\n- 任务ID: `{response.get('taskId')}`"
            keyboard = [[InlineKeyboardButton("👀 查看任务列表", callback_data=json.dumps({"action": CallbackAction.VIEW_TASKS.value}))]]
            await query.message.edit_text(success_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
            if selection_key in context.user_data: del context.user_data[selection_key]
        except ValueError as e: await query.message.edit_text(f"❌ 批量导入失败: {e}")
    elif action == CallbackAction.VIEW_TASKS:
        if 'displayed_tasks' in context.user_data: del context.user_data['displayed_tasks']
        await _display_tasks_list(update, context, page=1)
    elif action == CallbackAction.REFRESH_TASKS:
        if 'displayed_tasks' in context.user_data: del context.user_data['displayed_tasks']
        page = data.get("p", 1)
        await _display_tasks_list(update, context, page=page)
    elif action == CallbackAction.CLEAR_COMPLETED_TASKS and user_id in config.admin_ids:
        await query.message.edit_text("⏳ 正在清理已完成/失败的任务...")
        all_tasks = await api_call(context, "GET", "/api/control/tasks", params={"status": "all"})
        tasks_to_delete = [t for t in all_tasks if t.get("status") in ["已完成", "失败", "已中止"]]
        delete_coroutines = [api_call(context, "DELETE", f"/api/control/tasks/{t['taskId']}") for t in tasks_to_delete]
        await asyncio.gather(*delete_coroutines)
        await query.answer(f"✅ 成功清理了 {len(tasks_to_delete)} 个任务。")
        if 'displayed_tasks' in context.user_data: del context.user_data['displayed_tasks']
        await _display_tasks_list(update, context, page=1)
    elif action in [CallbackAction.PAUSE_TASK, CallbackAction.RESUME_TASK, CallbackAction.ABORT_TASK, CallbackAction.DELETE_TASK] and user_id in config.admin_ids:
        task_index = data["idx"]; task_id = context.user_data.get('displayed_tasks', [])[task_index].get("taskId")
        action_map = {CallbackAction.PAUSE_TASK: ("POST", f"/api/control/tasks/{task_id}/pause", "暂停"), CallbackAction.RESUME_TASK: ("POST", f"/api/control/tasks/{task_id}/resume", "恢复"), CallbackAction.ABORT_TASK: ("POST", f"/api/control/tasks/{task_id}/abort", "中止"), CallbackAction.DELETE_TASK: ("DELETE", f"/api/control/tasks/{task_id}", "删除")}
        method, endpoint, msg = action_map[action]
        try: await api_call(context, method, endpoint); await query.answer(f"✅ 已发送“{msg}”指令。")
        except ValueError as e: await query.answer(f"❌ 操作失败: {e}", show_alert=True)
        if 'displayed_tasks' in context.user_data: del context.user_data['displayed_tasks']
        await asyncio.sleep(1); await _display_tasks_list(update, context, page=1)
    elif action in [CallbackAction.REFRESH_LIBRARY, CallbackAction.VIEW_LIBRARY]: await _display_library(update, context)
    elif action == CallbackAction.REQUEST_DELETE_CONFIRM and user_id in config.admin_ids:
        item_index, item_to_delete = data["idx"], context.user_data.get('remove_list', [])[data["idx"]]; title = item_to_delete.get("title")
        keyboard = [[InlineKeyboardButton("✅ 是，删除", callback_data=json.dumps({"action": CallbackAction.EXECUTE_DELETE.value, "idx": item_index})), InlineKeyboardButton("❌ 否，取消", callback_data=json.dumps({"action": CallbackAction.CANCEL_DELETE.value}))]]
        await query.message.edit_text(f"⚠️ 确认删除 `{title}` 吗？此操作不可撤销！", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    elif action == CallbackAction.EXECUTE_DELETE and user_id in config.admin_ids:
        item_index, item_to_delete = data["idx"], context.user_data.get('remove_list', [])[data["idx"]]; anime_id, title = item_to_delete.get("animeId"), item_to_delete.get("title")
        await query.message.edit_text(f"⏳ 正在提交删除 `{title}` 的任务...", reply_markup=None)
        try: await api_call(context, "DELETE", f"/api/control/library/anime/{anime_id}"); await query.message.edit_text(f"✅ 删除 `{title}` 的任务已成功提交。")
        except ValueError as e: await query.message.edit_text(f"❌ 删除失败: {e}")
    elif action == CallbackAction.CANCEL_DELETE:
        await query.message.edit_text("✅ 操作已取消。"); schedule_message_deletion(context, query.message)
        
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("捕获到未处理异常:", exc_info=context.error)
    if isinstance(update, Update) and update.effective_chat:
        try: await context.bot.send_message(chat_id=update.effective_chat.id, text="⚠️ 处理您的请求时发生内部错误，管理员已收到通知。")
        except Exception as e: logger.error(f"向用户发送错误通知失败: {e}")

def setup_hot_reload(application: Application):
    from watchdog.observers import Observer; from watchdog.events import FileSystemEventHandler
    class CodeChangeHandler(FileSystemEventHandler):
        def __init__(self, app: Application): self.app, self.last_reload_time = app, 0
        def on_modified(self, event):
            if event.src_path.endswith(".py") and time.time() - self.last_reload_time > 2:
                logger.info(f"🔥 检测到代码变更: {event.src_path}, 准备热重载..."); self.last_reload_time = time.time(); asyncio.run_coroutine_threadsafe(self.reload_handlers(), self.app.loop)
        async def reload_handlers(self):
            logger.info("🔄 正在移除旧的处理器...")
            for handler_group in self.app.handlers.values():
                for handler in list(handler_group): self.app.remove_handler(handler)
            logger.info("🔧 正在注册新的处理器..."); setup_handlers(self.app); logger.info("🎉 热重载完成！")
    observer = Observer(); observer.schedule(CodeChangeHandler(application), path=os.path.dirname(__file__) or '.', recursive=False); observer.start()
    logger.info("🔍 代码热重载服务已启动 (仅限开发模式)"); return observer

def setup_handlers(application: Application):
    application.add_handler(CommandHandler("start", start_command)); application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("search", search_command)); application.add_handler(CommandHandler("import", import_command))
    application.add_handler(CommandHandler("remove", remove_command)); application.add_handler(CommandHandler("tasks", tasks_command))
    application.add_handler(CommandHandler("library", library_command)); application.add_handler(CommandHandler("reboot", reboot_command))
    application.add_handler(CallbackQueryHandler(main_callback_handler))

async def setup_bot_commands(application: Application):
    commands = [
        BotCommand("start", "启动机器人"), BotCommand("help", "获取帮助说明"), BotCommand("search", "搜索媒体"),
        BotCommand("import", "智能导入(支持名称或URL)"), BotCommand("tasks", "查看任务"), BotCommand("library", "查看媒体库"),
        BotCommand("remove", "[管理员]删除作品"), BotCommand("reboot", "[管理员]重启机器人"),
    ]; await application.bot.set_my_commands(commands)

async def post_init(application: Application):
    application.bot_data['http_client'] = httpx.AsyncClient(timeout=config.request_timeout)
    application.bot_data['danmu_server_api_key'] = config.danmu_server_api_key
    logger.info("✅ HTTP Client and initial API Key initialized and stored in bot_data.")
    await setup_bot_commands(application); logger.info("✅ Bot commands menu set.")

async def on_shutdown(application: Application):
    client: httpx.AsyncClient = application.bot_data.get('http_client')
    if client: await client.aclose(); logger.info("✅ HTTP Client closed gracefully.")
    observer = application.bot_data.get('observer')
    if observer:
        observer.stop(); loop = asyncio.get_running_loop(); await loop.run_in_executor(None, observer.join); logger.info("🔍 Hot reload service stopped gracefully.")

def main():
    application = (ApplicationBuilder().token(config.telegram_bot_token).post_init(post_init).post_shutdown(on_shutdown).build())
    setup_handlers(application); application.add_error_handler(error_handler)
    if config.enable_hot_reload: application.bot_data['observer'] = setup_hot_reload(application)
    logger.info("🚀 机器人已启动，正在监听..."); application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()

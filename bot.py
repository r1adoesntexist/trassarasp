import re
import json
import asyncio
import unicodedata
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pytz
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, 
    CallbackQueryHandler, filters, ContextTypes
)
import logging
import os
import threading

BOT_TOKEN = os.environ.get('BOT_TOKEN')

if not BOT_TOKEN:
    print("❌ ОШИБКА: Токен не найден в переменных окружения!")
    print("Убедитесь, что на хостинге создана переменная BOT_TOKEN с вашим токеном")
    exit(1)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

MOSCOW_TZ = pytz.timezone('Europe/Moscow')
NOTIFICATION_OFFSET_MINUTES = 60
DATA_FILE = 'tournaments.json'

class TournamentData:
    def __init__(self):
        self.schedule = []
        self.default_chat_id = None
        self.default_chat_info = "Не установлен"
        self.load_data()
    
    def load_data(self):
        try:
            if os.path.exists(DATA_FILE):
                with open(DATA_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.schedule = data.get('schedule', [])
                    self.default_chat_id = data.get('default_chat_id')
                    self.default_chat_info = data.get('default_chat_info', "Не установлен")
                    
                    for match in self.schedule:
                        if 'notified' not in match:
                            match['notified'] = False
                        if 'notification_sent_time' not in match:
                            match['notification_sent_time'] = None
                        if 'chat_id' not in match:
                            match['chat_id'] = self.default_chat_id
        except Exception as e:
            logger.error(f"Error loading data: {e}")
    
    def save_data(self):
        try:
            data = {
                'schedule': self.schedule,
                'default_chat_id': self.default_chat_id,
                'default_chat_info': self.default_chat_info
            }
            with open(DATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Error saving data: {e}")

data = TournamentData()

GAME_KEYWORDS = {
    'mafioso': ['мафиосо', 'mafioso', 'осо', 'мафиози', 'мафиозо', 'oso'],
    'baku': ['баку', 'baku'],
    'true': ['тру', 'true', 'труе', 'трушка'],
    'truetales': ['былины', 'truetales', 'трыютэйлс'],
    'combat': ['комбат', 'combat', 'комб', 'comb'],
    'cotomafia': ['котомафия', 'cotomafia', 'cototmaf', 'котомаф', 'coto', 'кото', 'kotomafia', 'kotemafia', 'koto', 'kote']
}

GAME_DISPLAY = {
    'mafioso': '🏳️‍🌈 Осо',
    'true': '🕰️ Тру',
    'truetales': '🤴 Былины',
    'combat': '💉 Комбат',
    'baku': '🕋 Баку',
    'cotomafia': '🐈 Котомаф'
}

IGNORE_WORDS = ['арена', 'итог', 'arena', 'result', 'иммортал']

TEAM_SEPARATORS = [
    ' vs ', ' вс ', ' против ', ' || ', ' // ', ' | ', ' / ', ' • ',
    '|vs|', '|вс|', '|против|', 'vs', 'вс',
    '𝐯𝐬', '|𝐯𝐬|', '❘', '┃'
]

def normalize_fancy_font(text: str) -> str:
    fancy_map = {

        '𝐀': 'A', '𝐁': 'B', '𝐂': 'C', '𝐃': 'D', '𝐄': 'E', '𝐅': 'F',
        '𝐆': 'G', '𝐇': 'H', '𝐈': 'I', '𝐉': 'J', '𝐊': 'K', '𝐋': 'L',
        '𝐌': 'M', '𝐍': 'N', '𝐎': 'O', '𝐏': 'P', '𝐐': 'Q', '𝐑': 'R',
        '𝐒': 'S', '𝐓': 'T', '𝐔': 'U', '𝐕': 'V', '𝐖': 'W', '𝐗': 'X',
        '𝐘': 'Y', '𝐙': 'Z',

        '𝐚': 'a', '𝐛': 'b', '𝐜': 'c', '𝐝': 'd', '𝐞': 'e', '𝐟': 'f',
        '𝐠': 'g', '𝐡': 'h', '𝐢': 'i', '𝐣': 'j', '𝐤': 'k', '𝐥': 'l',
        '𝐦': 'm', '𝐧': 'n', '𝐨': 'o', '𝐩': 'p', '𝐪': 'q', '𝐫': 'r',
        '𝐬': 's', '𝐭': 't', '𝐮': 'u', '𝐯': 'v', '𝐰': 'w', '𝐱': 'x',
        '𝐲': 'y', '𝐳': 'z',

        '𝑨': 'A', '𝑩': 'B', '𝑪': 'C', '𝑫': 'D', '𝑬': 'E', '𝑭': 'F',
        '𝑮': 'G', '𝑯': 'H', '𝑰': 'I', '𝑱': 'J', '𝑲': 'K', '𝑳': 'L',
        '𝑴': 'M', '𝑵': 'N', '𝑶': 'O', '𝑷': 'P', '𝑸': 'Q', '𝑹': 'R',
        '𝑺': 'S', '𝑻': 'T', '𝑼': 'U', '𝑽': 'V', '𝑾': 'W', '𝑿': 'X',
        '𝒀': 'Y', '𝒁': 'Z',
        '𝒂': 'a', '𝒃': 'b', '𝒄': 'c', '𝒅': 'd', '𝒆': 'e', '𝒇': 'f',
        '𝒈': 'g', '𝒉': 'h', '𝒊': 'i', '𝒋': 'j', '𝒌': 'k', '𝒍': 'l',
        '𝒎': 'm', '𝒏': 'n', '𝒐': 'o', '𝒑': 'p', '𝒒': 'q', '𝒓': 'r',
        '𝒔': 's', '𝒕': 't', '𝒖': 'u', '𝒗': 'v', '𝒘': 'w', '𝒙': 'x',
        '𝒚': 'y', '𝒛': 'z',

        '𝟎': '0', '𝟏': '1', '𝟐': '2', '𝟑': '3', '𝟒': '4',
        '𝟓': '5', '𝟔': '6', '𝟕': '7', '𝟖': '8', '𝟗': '9',
        '𝟬': '0', '𝟭': '1', '𝟮': '2', '𝟯': '3', '𝟰': '4',
        '𝟱': '5', '𝟲': '6', '𝟳': '7', '𝟴': '8', '𝟵': '9',
    }
    
    for fancy, normal in fancy_map.items():
        text = text.replace(fancy, normal)
    
    return text

def universal_normalize(text: str) -> str:
    if not text:
        return ""
    
    text = str(text)
    
    text = normalize_fancy_font(text)
    
    text = text.replace('×', 'x').replace('||', '|').replace('| |', '|')
    
    normalized = unicodedata.normalize('NFKD', text)
    normalized = ''.join(c for c in normalized if not unicodedata.combining(c))
    
    replacements = {
        '𝟢': '0', '𝟣': '1', '𝟤': '2', '𝟥': '3', '𝟦': '4',
        '𝟧': '5', '𝟨': '6', '𝟩': '7', '𝟪': '8', '𝟫': '9',
        '¹': '1', '²': '2', '³': '3', '⁴': '4', '⁵': '5',
        '·': ' ', '∙': ' ', '◦': ' ', '—': '-', '–': '-',
        '°': ' '
    }
    
    for old, new in replacements.items():
        normalized = normalized.replace(old, new)
    
    invisible_chars = ['\u200B', '\u200C', '\u200D', '\u200E', '\u200F', '\uFEFF', '\u00A0']
    for char in invisible_chars:
        normalized = normalized.replace(char, ' ')
    
    normalized = re.sub(r'\s+', ' ', normalized)
    return normalized.strip()

def clean_team_name(name: str) -> str:
    if not name:
        return ""
    
    name = re.sub(r'^[❤️⚰️🩸🌪🎼💙\[\]\(\)\s⚔️🍷]+', '', name)
    name = re.sub(r'[❤️⚰️🩸🌪🎼💙\[\]\(\)\s⚔️🍷]+$', '', name)
    
    markers = [' баку', ' мафиосо', ' осо', ' тру', ' комбат', ' котомаф', ' до ', ' 12/', ' 15/']
    name_lower = name.lower()
    for marker in markers:
        if marker in name_lower:
            pos = name_lower.find(marker)
            name = name[:pos].strip()
            break
    
    count_patterns = [
        r'\d{1,2}\s*/\s*\d{1,2}', r'\d{1,2}\s*-\s*\d{1,2}', r'\d{1,2}\s+на\s+\d{1,2}',
        r'\d{1,2}\s*x\s*\d{1,2}', r'\[\d{1,2}[/-]\d{1,2}\]', r'\(\d{1,2}[/-]\d{1,2}\)'
    ]
    for pattern in count_patterns:
        name = re.sub(pattern, '', name)
    
    name = re.sub(r'[❤️⚰️🩸🌪🎼💙⚔️🍷]', '', name)
    name = re.sub(r'\s+', ' ', name)
    return name.strip()

def extract_text_from_poll(message) -> str:
    try:
        if message.poll:
            return message.poll.question or ""
        return ""
    except Exception as e:
        logger.error(f"Error extracting text from poll: {e}")
        return ""

def remove_arena_and_next_word(text: str) -> str:
    lines = text.split('\n')
    result_lines = []
    
    for line in lines:
        arena_patterns = [
            r'—\s*арена[:\s]*[^\n]*',
            r'-\s*арена[:\s]*[^\n]*',
            r'арена[:\s]*[^\n]*',
            r'arena[:\s]*[^\n]*'
        ]
        
        cleaned_line = line
        for pattern in arena_patterns:
            cleaned_line = re.sub(pattern, '', cleaned_line, flags=re.IGNORECASE)
        
        cleaned_line = re.sub(r'[:\-—\s]+$', '', cleaned_line)
        
        if cleaned_line.strip():
            result_lines.append(cleaned_line)
    
    return '\n'.join(result_lines)

def is_date_line(line: str) -> bool:
    line_clean = line.strip().lower()
    
    if 'date:' in line_clean or 'time:' in line_clean:
        return True
    
    line_normalized = normalize_fancy_font(line)
    
    date_patterns = [
        r'^\d{1,2}\.\d{2}\s*[вхv|×\-–—]?\s*\d{1,2}[:.]\d{2}$',
        r'^\d{1,2}[:.]\d{2}\s*[вхv|×\-–—]?\s*\d{1,2}\.\d{2}$',
        r'^\d{1,2}\.\d{2}\s*$',
        r'^\d{1,2}[:.]\d{2}\s*$',
        r'^сегодня\s*\d{1,2}[:.]\d{2}',
        r'^завтра\s*\d{1,2}[:.]\d{2}',
    ]
    
    for pattern in date_patterns:
        if re.search(pattern, line_normalized, re.IGNORECASE):
            return True
    
    return False

def has_team_separator(line: str) -> bool:
    if is_date_line(line):
        return False
    
    line_normalized = normalize_fancy_font(line)
    line_lower = line_normalized.lower()
    
    for sep in TEAM_SEPARATORS:
        if sep in line_lower or sep in line_normalized:
            return True
    
    patterns = [
        r'\bvs\b', r'\bвс\b', r'\bпротив\b', 
        r'\|\|', r'//', r'\s\|\s', r'\s/\s', r'•',
        r'❘', r'┃', r'\|'
    ]
    for pattern in patterns:
        if re.search(pattern, line_lower):
            return True
    
    return False

def extract_teams_from_text(text: str) -> Tuple[str, str]:
    lines = text.split('\n')
    
    team_line = None
    for i, line in enumerate(lines):
        line = line.strip()
        if not line or is_date_line(line):
            continue
        
        if has_team_separator(line):
            team_line = line
            break
    
    if not team_line:
        team_lines = []
        for i, line in enumerate(lines):
            line = line.strip()
            if line and not is_date_line(line) and not any(word in line.lower() for word in IGNORE_WORDS):
                team_lines.append(line)
                if len(team_lines) == 2:
                    return clean_team_name(team_lines[0]), clean_team_name(team_lines[1])
        return "Команда 1", "Команда 2"
    
    team_line_norm = normalize_fancy_font(team_line)
    team_line_norm = re.sub(r'—\s*арена[:\s]*[^\n]*', '', team_line_norm, flags=re.IGNORECASE)
    team_line_norm = re.sub(r'\d{1,2}\.\d{2}\s*\|\|\s*\d{1,2}[:.]\d{2}', '', team_line_norm)
    team_line_norm = re.sub(r'\d{1,2}[:.]\d{2}', '', team_line_norm)
    team_line_norm = re.sub(r'\s+', ' ', team_line_norm).strip()
    
    separators = sorted(TEAM_SEPARATORS, key=len, reverse=True)
    
    for sep in separators:
        if sep in team_line_norm:
            parts = team_line_norm.split(sep, 1)
            if len(parts) == 2:
                team1 = clean_team_name(parts[0])
                team2 = clean_team_name(parts[1])
                if team1 and team2 and len(team1) > 1 and len(team2) > 1:
                    return team1, team2
    
    for sep_pattern in ['\\|', '❘', '┃', 'vs', 'вс', '||']:
        parts = re.split(sep_pattern, team_line_norm, maxsplit=1, flags=re.IGNORECASE)
        if len(parts) == 2:
            team1 = clean_team_name(parts[0])
            team2 = clean_team_name(parts[1])
            if team1 and team2:
                return team1, team2
    
    return "Команда 1", "Команда 2"

def parse_date_time(text: str) -> Optional[Tuple]:
    text_normalized = normalize_fancy_font(text)
    text_lower = text_normalized.lower()
    
    clean_text = re.sub(r'[🌟🤍😺☠️🌸🧡❤️⚰️🩸🌪🎼💙⚔️🍷]', ' ', text_normalized)
    clean_text = re.sub(r'\s+', ' ', clean_text).strip()
    
    full_patterns = [
        r'(\d{1,2})\.(\d{2})\s*(?:в|в\s*|x|×|\|\||[-\–—]|)\s*(\d{1,2})[:.](\d{2})',
        r'date\s*[:=]?\s*(\d{1,2})\.(\d{2}).*?time\s*[:=]?\s*(\d{1,2})[:.](\d{2})',
    ]
    
    for pattern in full_patterns:
        match = re.search(pattern, clean_text, re.IGNORECASE)
        if match:
            groups = match.groups()
            if len(groups) == 4:
                day, month, hour, minute = groups
                try:
                    day_int, month_int, hour_int, minute_int = int(day), int(month), int(hour), int(minute)
                    if (1 <= day_int <= 31 and 1 <= month_int <= 12 and 
                        0 <= hour_int <= 23 and 0 <= minute_int <= 59):
                        return ('full', day, month, hour, minute)
                except:
                    pass
    
    today_match = re.search(r'сегодня\s*(?:в|в\s*|x|×|\|\||[-\–—]|)\s*(\d{1,2})[:.](\d{2})', text_lower)
    if today_match:
        hour, minute = today_match.groups()
        return ('today', None, None, hour, minute)
    
    tomorrow_match = re.search(r'завтра\s*(?:в|в\s*|x|×|\|\||[-\–—]|)\s*(\d{1,2})[:.](\d{2})', text_lower)
    if tomorrow_match:
        hour, minute = tomorrow_match.groups()
        return ('tomorrow', None, None, hour, minute)
    
    time_only_match = re.search(r'(\d{1,2})[:.](\d{2})', clean_text)
    if time_only_match:
        hour, minute = time_only_match.groups()
        return ('time_only', None, None, hour, minute)
    
    return None

def parse_count(text: str, normalized_text: str) -> str:
    lines = text.split('\n')
    for line in lines:
        if is_date_line(line):
            continue
        
        line_normalized = normalize_fancy_font(line)
            
        count_patterns = [
            (r'(\d{1,2})\s*/\s*(\d{1,2})', r'\1/\2'),
            (r'(\d{1,2})\s*-\s*(\d{1,2})', r'\1/\2'),
            (r'(\d{1,2})\s+на\s+(\d{1,2})', r'\1/\2'),
        ]
        
        for pattern, _ in count_patterns:
            matches = re.finditer(pattern, line_normalized)
            for match in matches:
                num1, num2 = int(match.group(1)), int(match.group(2))
                if 5 <= num1 <= 20 and 5 <= num2 <= 20:
                    return f"{num1}/{num2}"
    
    return "12/12"

def detect_game_type(text: str, normalized_text: str) -> str:
    text_normalized = normalize_fancy_font(text)
    text_lower = text_normalized.lower()
    normalized_lower = normalized_text.lower()
    
    lines = text.split('\n')
    for line in lines:
        line_normalized = normalize_fancy_font(line)
        line_lower = line_normalized.lower()
        if is_date_line(line):
            continue
        
        for game_type, keywords in GAME_KEYWORDS.items():
            for keyword in keywords:
                if keyword in line_lower:
                    return game_type
    
    for game_type, keywords in GAME_KEYWORDS.items():
        for keyword in keywords:
            if keyword in normalized_lower:
                return game_type
    
    return 'mafioso'

def parse_win_condition(text: str, normalized_text: str, normalized_lower: str) -> str:
    text_normalized = normalize_fancy_font(text)
    
    ru_patterns = [
        (r'до\s*(\d+)\s*[-хx]?\s*побед', 'до {} побед'),
        (r'до\s*(\d+)\s*[-хx]?\s*игр', 'до {} игр'),
        (r'до\s*(\d+)\s*[-хx]?$', 'до {} побед'),
        (r'(\d+)\s*[-хx]?\s*побед', 'до {} побед'),
    ]
    
    en_patterns = [
        (r'best\s+of\s+(\d+)', 'best of {}'),
        (r'bo\s*(\d+)', 'BO{}'),
        (r'up\s+to\s+(\d+)\s+wins?', 'до {} побед'),
        (r'(\d+)\s+wins?', 'до {} побед'),
    ]
    
    for pattern, template in ru_patterns:
        match = re.search(pattern, text_normalized.lower())
        if match:
            return template.format(match.group(1))
    
    for pattern, template in en_patterns:
        match = re.search(pattern, text_normalized.lower())
        if match:
            return template.format(match.group(1))
    
    return "до 3 побед"

def parse_captains(text: str) -> List[str]:
    captains = []
    seen = set()
    
    text = normalize_fancy_font(text)
    text = re.sub(r'@\.([a-zA-Z0-9_]+)', r'@\1', text)
    
    mention_pattern = r'@([a-zA-Z0-9_\.]+(?:\.[a-zA-Z0-9_]+)*)'
    for match in re.finditer(mention_pattern, text):
        username = match.group(1)
        if username.startswith('.'):
            username = username[1:]
        username = re.sub(r'[^\w\.]', '', username)
        if username and len(username) > 1:
            captain = '@' + username
            captain_lower = captain.lower()
            
            if (captain_lower not in seen and
                not re.match(r'^@(?:cap|кап|кэп)\d*$', captain_lower) and
                not re.match(r'^@\d+$', captain_lower)):
                seen.add(captain_lower)
                captains.append(captain)
    
    return captains[:5]

def parse_match(text: str) -> Optional[Dict]:
    try:
        original_text = text
        
        text = remove_arena_and_next_word(text)
        
        lines = text.split('\n')
        filtered_lines = []
        for line in lines:
            line_lower = line.lower()
            ignore = False
            for word in IGNORE_WORDS:
                if word in line_lower:
                    ignore = True
                    break
            if not ignore and line.strip():
                filtered_lines.append(line)
        
        text = '\n'.join(filtered_lines)
        normalized_text = universal_normalize(text)
        normalized_lower = normalized_text.lower()
        
        date_result = parse_date_time(text)
        if not date_result:
            return None
        
        date_type, day, month, hour, minute = date_result
        
        try:
            now = datetime.now(MOSCOW_TZ)
            
            if date_type == 'today':
                match_dt = now.replace(
                    hour=int(hour),
                    minute=int(minute),
                    second=0,
                    microsecond=0
                )
                if match_dt < now:
                    match_dt += timedelta(days=1)
            elif date_type == 'tomorrow':
                match_dt = now.replace(
                    hour=int(hour),
                    minute=int(minute),
                    second=0,
                    microsecond=0
                ) + timedelta(days=1)
            elif date_type == 'time_only':
                match_dt = now.replace(
                    hour=int(hour),
                    minute=int(minute),
                    second=0,
                    microsecond=0
                )
                if match_dt < now:
                    match_dt += timedelta(days=1)
            else:
                day = day.zfill(2)
                month = month.zfill(2)
                hour = hour.zfill(2)
                minute = minute.zfill(2)
                
                current_year = now.year
                
                match_dt = datetime(
                    year=current_year,
                    month=int(month),
                    day=int(day),
                    hour=int(hour),
                    minute=int(minute),
                    second=0,
                    microsecond=0
                )
                match_dt = MOSCOW_TZ.localize(match_dt)
                
                if match_dt < now:
                    match_dt = match_dt.replace(year=current_year + 1)
        except Exception as e:
            logger.error(f"Date parsing error: {e}")
            return None
        
        team1, team2 = extract_teams_from_text(original_text)
        game_type = detect_game_type(original_text, normalized_text)
        count = parse_count(original_text, normalized_text)
        win_condition = parse_win_condition(text, normalized_text, normalized_lower)
        captains = parse_captains(original_text)
        
        match_info = {
            'datetime': match_dt.isoformat(),
            'team1': team1,
            'team2': team2,
            'game_type': game_type,
            'count': count,
            'win_condition': win_condition,
            'captains': captains,
            'original_text': original_text[:500],
            'notified': False,
            'notification_sent_time': None,
            'chat_id': None
        }
        
        return match_info
        
    except Exception as e:
        logger.error(f"Parse error: {e}")
        return None

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "🤖 <b>Бот для расписания турниров по мафии</b>\n\n"
        "<b>Команды:</b>\n"
        "/add - Добавить турнир (ответь на сообщение или опрос)\n"
        "/list [дней] - Показать расписание\n"
        "/edit &lt;номер&gt; - Редактировать турнир\n"
        "/delete &lt;номер&gt; - Удалить турнир\n"
        "/setchat [чат] - Установить чат для уведомлений\n"
        "/clear - Очистить всё расписание\n"
        "/help - Показать эту справку\n\n"
        "<b>Формат сообщения:</b>\n"
        "<code>01.02 в 19:00\nКоманда1 vs Команда2\nмафиосо | 15/15 | до 3 побед</code>"
    )
    await update.message.reply_text(help_text, parse_mode='HTML')

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /cancel"""
    if context.user_data.get('awaiting_input'):
        context.user_data.clear()
        await update.message.reply_text("❌ Редактирование отменено.")
    else:
        await update.message.reply_text("❌ Нет активного редактирования.")

async def add_tournament(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reply = update.message.reply_to_message
    if not reply:
        await update.message.reply_text(
            "❌ <b>Ответь на сообщение с информацией о турнире!</b>",
            parse_mode='HTML'
        )
        return
    
    text = ""
    from_poll = False
    
    if reply.poll:
        from_poll = True
        text = extract_text_from_poll(reply)
    else:
        text = reply.text or reply.caption or ""
    
    if not text:
        await update.message.reply_text(
            "❌ <b>Не удалось распознать информацию о турнире!</b>",
            parse_mode='HTML'
        )
        return
    
    match_info = parse_match(text)
    
    if not match_info:
        await update.message.reply_text(
            "❌ <b>Не удалось распознать информацию о турнире. Проверьте формат!</b>",
            parse_mode='HTML'
        )
        return
    
    for match in data.schedule:
        if (match['datetime'] == match_info['datetime'] and
            match['team1'] == match_info['team1'] and
            match['team2'] == match_info['team2']):
            await update.message.reply_text(
                "❌ <b>Этот турнир уже есть в расписании!</b>",
                parse_mode='HTML'
            )
            return
    
    chat_id = None
    chat_info = "Не указан"
    
    if context.args:
        chat_arg = ' '.join(context.args)
        if chat_arg.startswith('@') or chat_arg.startswith('-100'):
            chat_info = chat_arg
            chat_id = int(chat_arg) if chat_arg.lstrip('-').isdigit() else 0
        else:
            chat_id = update.effective_chat.id
            chat_info = update.effective_chat.title or f"ID: {chat_id}"
    elif data.default_chat_id:
        chat_id = data.default_chat_id
        chat_info = data.default_chat_info
    else:
        chat_id = update.effective_chat.id
        chat_info = update.effective_chat.title or f"ID: {chat_id}"
    
    match_info['chat_id'] = chat_id
    
    now = datetime.now(MOSCOW_TZ)
    match_time = datetime.fromisoformat(match_info['datetime']).replace(tzinfo=MOSCOW_TZ)
    time_diff_minutes = (match_time - now).total_seconds() / 60
    
    if time_diff_minutes <= NOTIFICATION_OFFSET_MINUTES and time_diff_minutes > 0:
        match_info['notified'] = False
    
    data.schedule.append(match_info)
    data.save_data()
    
    match_time_str = datetime.fromisoformat(match_info['datetime']).strftime('%d.%m %H:%M')
    game_name = GAME_DISPLAY.get(match_info['game_type'], match_info['game_type'])
    
    captains_text = "\n".join(f"  • <code>{cap}</code>" for cap in match_info['captains'])
    if not captains_text:
        captains_text = "  <i>не указаны</i>"
    
    response = (
        f"{'📊 <b>Информация взята из опроса</b>' if from_poll else ''}"
        f"✅ <b>Турнир добавлен в расписание!</b>\n\n"
        f"<b>📅 Дата и время:</b> <code>{match_time_str}</code> (МСК)\n"
        f"<b>🎮 Тип игры:</b> <code>{game_name}</code>\n"
        f"<b>👥 Команды:</b> <code>{match_info['team1']}</code> vs <code>{match_info['team2']}</code>\n"
        f"<b>🔢 Количество:</b> <code>{match_info['count']}</code>\n"
        f"<b>🏆 Условие:</b> <code>{match_info['win_condition']}</code>\n\n"
        f"<b>👑 Кэпы:</b>\n{captains_text}\n\n"
        f"<b>📍 Уведомления будут в:</b> {chat_info}"
    )
    
    keyboard = [
        [InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_{len(data.schedule)-1}")],
        [InlineKeyboardButton("🗑️ Удалить", callback_data=f"delete_{len(data.schedule)-1}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(response, parse_mode='HTML', reply_markup=reply_markup)

async def list_tournaments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    days = 7
    if context.args and context.args[0].isdigit():
        days = int(context.args[0])
        if days <= 0:
            days = 7
    
    if not data.schedule:
        await update.message.reply_text(
            "📭 <b>Расписание пусто.</b>",
            parse_mode='HTML'
        )
        return
    
    now = datetime.now(MOSCOW_TZ)
    
    future_matches = []
    for i, match in enumerate(data.schedule):
        match_time = datetime.fromisoformat(match['datetime']).replace(tzinfo=MOSCOW_TZ)
        if match_time >= now:
            future_matches.append((i, match, match_time))
    
    if not future_matches:
        await update.message.reply_text(
            "📭 <b>Нет запланированных турниров.</b>",
            parse_mode='HTML'
        )
        return
    
    future_matches.sort(key=lambda x: x[2])
    
    if days > 0:
        future_cutoff = now + timedelta(days=days)
        future_matches = [m for m in future_matches if m[2] <= future_cutoff]
    
    if not future_matches:
        await update.message.reply_text(
            f"📭 <b>Нет запланированных турниров на ближайшие {days} дней.</b>",
            parse_mode='HTML'
        )
        return
    
    response = f"📋 <b>Расписание турниров (ближайшие {days} дней):</b>\n\n"
    
    for idx, (original_index, match, match_time) in enumerate(future_matches, 1):
        match_time_str = match_time.strftime('%d.%m %H:%M')
        game_name = GAME_DISPLAY.get(match['game_type'], match['game_type'])
        notified_status = "✅" if match.get('notified') else "⏳"
        
        response += (
            f"{idx}. {notified_status} <b>{match_time_str}</b> (МСК)\n"
            f"   {game_name}\n"
            f"   👥 <code>{match['team1']}</code>\n"
            f"      vs\n"
            f"   👥 <code>{match['team2']}</code>\n"
            f"   🔢 {match['count']} | {match['win_condition']}\n"
        )
        
        if match['captains']:
            captains_line = " | ".join(match['captains'][:2])
            response += f"   👑 {captains_line}\n"
        
        response += "\n"
    
    response += f"<i>Всего турниров: {len(future_matches)}</i>"
    
    await update.message.reply_text(response, parse_mode='HTML')

async def delete_tournament(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text(
            "❌ <b>Укажите номер турнира!</b>\nИспользуйте: <code>/delete 1</code>",
            parse_mode='HTML'
        )
        return
    
    index = int(context.args[0])
    
    now = datetime.now(MOSCOW_TZ)
    
    future_matches = []
    for i, match in enumerate(data.schedule):
        match_time = datetime.fromisoformat(match['datetime']).replace(tzinfo=MOSCOW_TZ)
        if match_time >= now:
            future_matches.append((i, match, match_time))
    
    future_matches.sort(key=lambda x: x[2])
    
    if not future_matches:
        await update.message.reply_text(
            "📭 <b>Нет турниров для удаления.</b>",
            parse_mode='HTML'
        )
        return
    
    if 1 <= index <= len(future_matches):
        original_index = future_matches[index - 1][0]
        removed_match = data.schedule.pop(original_index)
        data.save_data()
        
        match_time = datetime.fromisoformat(removed_match['datetime']).strftime('%d.%m %H:%M')
        response = (
            f"🗑️ <b>Турнир удален:</b>\n"
            f"<code>{match_time} - {removed_match['team1']} vs {removed_match['team2']}</code>"
        )
        
        await update.message.reply_text(response, parse_mode='HTML')
    else:
        await update.message.reply_text(
            "❌ <b>Неверный номер турнира!</b>",
            parse_mode='HTML'
        )

async def edit_tournament(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text(
            "❌ <b>Укажите номер турнира!</b>\nИспользуйте: <code>/edit 1</code>",
            parse_mode='HTML'
        )
        return
    
    index = int(context.args[0])
    
    now = datetime.now(MOSCOW_TZ)
    
    future_matches = []
    for i, match in enumerate(data.schedule):
        match_time = datetime.fromisoformat(match['datetime']).replace(tzinfo=MOSCOW_TZ)
        if match_time >= now:
            future_matches.append((i, match, match_time))
    
    future_matches.sort(key=lambda x: x[2])
    
    if not future_matches:
        await update.message.reply_text(
            "📭 <b>Нет турниров для редактирования.</b>",
            parse_mode='HTML'
        )
        return
    
    if 1 <= index <= len(future_matches):
        original_index = future_matches[index - 1][0]
        await show_edit_menu(update, context, original_index)
    else:
        await update.message.reply_text(
            "❌ <b>Неверный номер турнира!</b>",
            parse_mode='HTML'
        )

async def show_edit_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, match_index: int):
    match = data.schedule[match_index]
    match_time = datetime.fromisoformat(match['datetime']).strftime('%d.%m %H:%M')
    game_name = GAME_DISPLAY.get(match['game_type'], match['game_type'])
    
    captains_text = "\n".join(f"  • <code>{cap}</code>" for cap in match['captains'])
    if not captains_text:
        captains_text = "  <i>не указаны</i>"
    
    response = (
        f"✏️ <b>Редактирование турнира #{match_index + 1}</b>\n\n"
        f"<b>Текущие данные:</b>\n"
        f"📅 <b>datetime:</b> <code>{match['datetime']}</code>\n"
        f"🎮 <b>game_type:</b> <code>{match['game_type']}</code> → {game_name}\n"
        f"👥 <b>team1:</b> <code>{match['team1']}</code>\n"
        f"👥 <b>team2:</b> <code>{match['team2']}</code>\n"
        f"🔢 <b>count:</b> <code>{match['count']}</code>\n"
        f"🏆 <b>win_condition:</b> <code>{match['win_condition']}</code>\n"
        f"👑 <b>captains:</b> <code>{', '.join(match['captains']) if match['captains'] else 'не найдены'}</code>\n\n"
        f"<b>Выберите, что изменить:</b>"
    )
    
    keyboard = [
        [InlineKeyboardButton("📅 Дата/время", callback_data=f"edit_field_{match_index}_datetime")],
        [InlineKeyboardButton("🎮 Тип игры", callback_data=f"edit_field_{match_index}_game_type")],
        [InlineKeyboardButton("👥 Команда 1", callback_data=f"edit_field_{match_index}_team1")],
        [InlineKeyboardButton("👥 Команда 2", callback_data=f"edit_field_{match_index}_team2")],
        [InlineKeyboardButton("🔢 Количество", callback_data=f"edit_field_{match_index}_count")],
        [InlineKeyboardButton("🏆 Условие победы", callback_data=f"edit_field_{match_index}_win_condition")],
        [InlineKeyboardButton("👑 Кэпы", callback_data=f"edit_field_{match_index}_captains")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_edit")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    context.user_data['editing_match'] = match_index
    
    if update.callback_query:
        await update.callback_query.edit_message_text(response, parse_mode='HTML', reply_markup=reply_markup)
    else:
        await update.message.reply_text(response, parse_mode='HTML', reply_markup=reply_markup)

async def handle_edit_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data_callback = query.data
    
    if data_callback == "cancel_edit":
        await query.edit_message_text("❌ Редактирование отменено.")
        context.user_data.clear()
        return
    
    match = re.match(r'edit_field_(\d+)_(.+)', data_callback)
    if not match:
        return
    
    match_index = int(match.group(1))
    field = match.group(2)
    
    context.user_data['editing_match'] = match_index
    context.user_data['editing_field'] = field
    
    field_names = {
        'datetime': '📅 дату и время (в формате ДД.ММ ЧЧ:ММ или YYYY-MM-DDTHH:MM:SS+03:00)',
        'game_type': '🎮 тип игры (осо/баку/тру/комбат/котомаф/былины)',
        'team1': '👥 название первой команды',
        'team2': '👥 название второй команды',
        'count': '🔢 количество (например, 12/12)',
        'win_condition': '🏆 условие победы (например, до 3 побед)',
        'captains': '👑 кэпов (через пробел или запятую, с @ или без)'
    }
    
    current_value = data.schedule[match_index].get(field, '')
    if field == 'game_type':
        current_value = f"{current_value} → {GAME_DISPLAY.get(current_value, current_value)}"
    elif field == 'captains':
        current_value = ', '.join(current_value) if current_value else 'не указаны'
    
    response = (
        f"✏️ <b>Редактирование поля: {field_names.get(field, field)}</b>\n\n"
        f"<b>Текущее значение:</b> <code>{current_value}</code>\n\n"
        f"<b>Отправьте новое значение в чат.</b>\n"
        f"<i>Для отмены отправьте /cancel</i>"
    )
    
    await query.edit_message_text(response, parse_mode='HTML')
    
    context.user_data['awaiting_input'] = True

async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get('awaiting_input'):
        return
    
    match_index = context.user_data.get('editing_match')
    field = context.user_data.get('editing_field')
    
    if match_index is None or field is None or match_index >= len(data.schedule):
        context.user_data.clear()
        await update.message.reply_text("❌ Ошибка: турнир не найден. Редактирование отменено.")
        return
    
    new_value = update.message.text.strip()
    
    try:
        if field == 'datetime':
            try:
                dt = datetime.fromisoformat(new_value)
                if dt.tzinfo is None:
                    dt = MOSCOW_TZ.localize(dt)
            except:
                match = re.match(r'(\d{1,2})\.(\d{2})\s+(\d{1,2}):(\d{2})', new_value)
                if match:
                    day, month, hour, minute = match.groups()
                    now = datetime.now(MOSCOW_TZ)
                    dt = datetime(
                        year=now.year,
                        month=int(month),
                        day=int(day),
                        hour=int(hour),
                        minute=int(minute)
                    )
                    dt = MOSCOW_TZ.localize(dt)
                    if dt < now:
                        dt = dt.replace(year=now.year + 1)
                else:
                    raise ValueError("Неверный формат даты")
            
            data.schedule[match_index][field] = dt.isoformat()
            data.schedule[match_index]['notified'] = False
            data.schedule[match_index]['notification_sent_time'] = None
        
        elif field == 'game_type':
            new_value_lower = new_value.lower()
            found = False
            for key, keywords in GAME_KEYWORDS.items():
                if new_value_lower in keywords or new_value_lower == key:
                    data.schedule[match_index][field] = key
                    found = True
                    break
            if not found:
                data.schedule[match_index][field] = new_value
        
        elif field in ['team1', 'team2']:
            data.schedule[match_index][field] = new_value
        
        elif field == 'count':
            if not re.match(r'\d{1,2}/\d{1,2}', new_value):
                raise ValueError("Неверный формат количества. Используйте формат X/Y")
            data.schedule[match_index][field] = new_value
        
        elif field == 'win_condition':
            data.schedule[match_index][field] = new_value
        
        elif field == 'captains':
            captains = []
            mentions = re.findall(r'@(\w+)', new_value)
            if mentions:
                captains = ['@' + username for username in mentions]
            else:
                words = new_value.replace(',', ' ').split()
                captains = ['@' + word if not word.startswith('@') else word for word in words]
            data.schedule[match_index][field] = captains[:5]
        
        data.save_data()
        
        match = data.schedule[match_index]
        match_time = datetime.fromisoformat(match['datetime']).strftime('%d.%m %H:%M')
        game_name = GAME_DISPLAY.get(match['game_type'], match['game_type'])
        
        captains_text = "\n".join(f"  • <code>{cap}</code>" for cap in match['captains'])
        if not captains_text:
            captains_text = "  <i>не указаны</i>"
        
        response = (
            f"✅ <b>Турнир обновлен!</b>\n\n"
            f"<b>📅 Дата и время:</b> <code>{match_time}</code> (МСК)\n"
            f"<b>🎮 Тип игры:</b> <code>{game_name}</code>\n"
            f"<b>👥 Команды:</b> <code>{match['team1']}</code> vs <code>{match['team2']}</code>\n"
            f"<b>🔢 Количество:</b> <code>{match['count']}</code>\n"
            f"<b>🏆 Условие:</b> <code>{match['win_condition']}</code>\n\n"
            f"<b>👑 Кэпы:</b>\n{captains_text}"
        )
        
        keyboard = [[InlineKeyboardButton("✏️ Продолжить редактирование", callback_data=f"edit_{match_index}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(response, parse_mode='HTML', reply_markup=reply_markup)
        
    except Exception as e:
        await update.message.reply_text(
            f"❌ <b>Ошибка при сохранении:</b>\n<code>{str(e)}</code>\n\nПопробуйте снова или отправьте /cancel",
            parse_mode='HTML'
        )
        return
    
    context.user_data.clear()

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data_callback = query.data
    
    if data_callback.startswith('edit_') and not data_callback.startswith('edit_field_'):
        try:
            match_index = int(data_callback.split('_')[1])
            if 0 <= match_index < len(data.schedule):
                await show_edit_menu(update, context, match_index)
        except (ValueError, IndexError):
            await query.edit_message_text("❌ Ошибка: турнир не найден.")
    
    elif data_callback.startswith('delete_'):
        try:
            match_index = int(data_callback.split('_')[1])
            if 0 <= match_index < len(data.schedule):
                removed_match = data.schedule.pop(match_index)
                data.save_data()
                match_time = datetime.fromisoformat(removed_match['datetime']).strftime('%d.%m %H:%M')
                response = (
                    f"🗑️ <b>Турнир удален:</b>\n"
                    f"<code>{match_time} - {removed_match['team1']} vs {removed_match['team2']}</code>"
                )
                await query.edit_message_text(response, parse_mode='HTML')
        except (ValueError, IndexError):
            await query.edit_message_text("❌ Ошибка: турнир не найден.")
    
    elif data_callback.startswith('edit_field_'):
        await handle_edit_callback(update, context)
    
    elif data_callback == 'cancel_edit':
        await query.edit_message_text("❌ Редактирование отменено.")
        context.user_data.clear()

async def set_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        chat_id = update.effective_chat.id
        chat_info = update.effective_chat.title or f"ID: {chat_id}"
        
        data.default_chat_id = chat_id
        data.default_chat_info = chat_info
        data.save_data()
        
        await update.message.reply_text(
            f"✅ <b>Чат для уведомлений установлен:</b> {chat_info}",
            parse_mode='HTML'
        )
        return
    
    chat_arg = ' '.join(context.args)
    
    if chat_arg.startswith('@'):
        chat_info = chat_arg
        data.default_chat_id = 0
        data.default_chat_info = chat_info
        data.save_data()
        await update.message.reply_text(
            f"✅ <b>Чат для уведомлений установлен:</b> {chat_info}\n\n<i>⚠️ Для работы уведомлений бот должен быть в этом чате!</i>",
            parse_mode='HTML'
        )
    elif chat_arg.isdigit() or (chat_arg.startswith('-') and chat_arg[1:].isdigit()):
        chat_id = int(chat_arg)
        chat_info = f"ID: {chat_id}"
        data.default_chat_id = chat_id
        data.default_chat_info = chat_info
        data.save_data()
        await update.message.reply_text(
            f"✅ <b>Чат для уведомлений установлен:</b> {chat_info}",
            parse_mode='HTML'
        )
    else:
        await update.message.reply_text(
            "❌ <b>Не удалось установить чат.</b>\n\nИспользуйте ID чата или @username",
            parse_mode='HTML'
        )

async def clear_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data.schedule = []
    data.save_data()
    await update.message.reply_text(
        "🗑️ <b>Всё расписание очищено!</b>",
        parse_mode='HTML'
    )

async def notification_loop(app: Application):
    while True:
        try:
            now = datetime.now(MOSCOW_TZ)
            logger.info(f"Проверка уведомлений в {now.strftime('%H:%M:%S')}")
            
            for match in data.schedule:
                if match.get('notified'):
                    continue
                    
                match_time = datetime.fromisoformat(match['datetime']).replace(tzinfo=MOSCOW_TZ)
                chat_id = match.get('chat_id') or data.default_chat_id
                
                if not chat_id:
                    continue
                
                if match_time <= now:
                    continue
                
                time_diff_minutes = (match_time - now).total_seconds() / 60
                
                if 55 <= time_diff_minutes <= 65:
                    try:
                        match_time_str = match_time.strftime('%d.%m %H:%M')
                        game_name = GAME_DISPLAY.get(match['game_type'], match['game_type'])
                        
                        captains_text = "\n".join(f"  • <code>{cap}</code>" for cap in match['captains'])
                        if not captains_text:
                            captains_text = "  <i>не указаны</i>"
                        
                        notification = (
                            f"⏰ <b>НАПОМИНАНИЕ! Через ПОЛЧАСА начинается турнир!</b>\n\n"
                            f"<b>🕐 Время:</b> <code>{match_time_str}</code> (МСК)\n"
                            f"<b>🎮 Тип игры:</b> <code>{game_name}</code>\n"
                            f"<b>👥 Команды:</b> <code>{match['team1']}</code> vs <code>{match['team2']}</code>\n"
                            f"<b>🔢 Количество:</b> <code>{match['count']}</code>\n"
                            f"<b>🏆 Условие:</b> <code>{match['win_condition']}</code>\n\n"
                            f"<b>👑 Кэпы:</b>\n{captains_text}\n\n"
                            f"<b>Подготовьтесь к игре!</b> 🎯"
                        )
                        
                        await app.bot.send_message(chat_id=int(chat_id), text=notification, parse_mode='HTML')
                        
                        match['notified'] = True
                        match['notification_sent_time'] = now.isoformat()
                        data.save_data()
                        
                        logger.info(f"Уведомление отправлено для турнира в {match_time}")
                        
                    except Exception as e:
                        logger.error(f"Ошибка отправки уведомления: {e}")
            
        except Exception as e:
            logger.error(f"Ошибка в цикле уведомлений: {e}")
        
        await asyncio.sleep(30)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Ошибка: {context.error}")

def start_notification_loop(app: Application):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(notification_loop(app))

def main():
    app = Application.builder().token(BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("cancel", cancel_command))
    app.add_handler(CommandHandler("add", add_tournament))
    app.add_handler(CommandHandler("list", list_tournaments))
    app.add_handler(CommandHandler("delete", delete_tournament))
    app.add_handler(CommandHandler("edit", edit_tournament))
    app.add_handler(CommandHandler("setchat", set_chat))
    app.add_handler(CommandHandler("clear", clear_schedule))
    
    app.add_handler(CallbackQueryHandler(handle_callback))
    
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input))
    
    app.add_error_handler(error_handler)
    
    notification_thread = threading.Thread(target=start_notification_loop, args=(app,), daemon=True)
    notification_thread.start()
    
    logger.info("Бот запущен!")
    print("✅ Бот запущен! Нажмите Ctrl+C для остановки.")
    app.run_polling()

if __name__ == "__main__":
    main()

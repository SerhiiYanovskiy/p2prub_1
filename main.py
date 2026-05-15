import asyncio
import os
import re
import sqlite3
import smtplib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from email.utils import formatdate
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import pandas as pd
import edge_tts

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types.input_file import FSInputFile
from aiogram.client.default import DefaultBotProperties
from aiogram.enums.parse_mode import ParseMode

try:
    from zoneinfo import ZoneInfo  # py 3.9+
except Exception:
    ZoneInfo = None

# optional dependency for SendGrid via HTTP
try:
    import requests
except Exception:
    requests = None

import config

BASE_DIR = Path(__file__).resolve().parent


@dataclass
class Node:
    node_id: str
    parent_id: Optional[str]
    node_type: str
    button_text: str
    message_text: str
    sort_order: int
    show_listen: bool
    show_back: bool
    show_home: bool
    show_not_found: bool
    action_url: Optional[str] = None
    audio_text: str = ""  # from column "audio"


class ContentStore:
    def __init__(self, xlsx_path: str):
        self.xlsx_path = xlsx_path
        self.cfg: Dict[str, str] = {}
        self.ui: Dict[str, str] = {}
        self.nodes: Dict[str, Node] = {}
        self.children: Dict[str, List[str]] = {}
        self.onb: Dict[str, str] = {}

    def load(self) -> None:
        if not os.path.exists(self.xlsx_path):
            raise FileNotFoundError(f"XLSX not found: {self.xlsx_path}")

        df_cfg = pd.read_excel(self.xlsx_path, sheet_name="Config")
        self.cfg = {
            str(k).strip(): ("" if pd.isna(v) else str(v).strip())
            for k, v in zip(df_cfg["key"], df_cfg["value"])
        }

        df_ui = pd.read_excel(self.xlsx_path, sheet_name="UI")
        self.ui = {
            str(k).strip(): ("" if pd.isna(v) else str(v).strip())
            for k, v in zip(df_ui["key"], df_ui["value"])
        }

        df_nodes = pd.read_excel(self.xlsx_path, sheet_name="Nodes")

        def _b(x) -> bool:
            if isinstance(x, bool):
                return x
            if pd.isna(x):
                return False
            return str(x).strip().lower() in ("1", "true", "yes", "y")

        def _s(x) -> str:
            if pd.isna(x):
                return ""
            return str(x).strip()

        self.nodes.clear()
        self.children.clear()

        for _, r in df_nodes.iterrows():
            node_id = _s(r.get("node_id"))
            if not node_id:
                continue

            node = Node(
                node_id=node_id,
                parent_id=None if pd.isna(r.get("parent_id")) else _s(r.get("parent_id")),
                node_type=_s(r.get("node_type")),
                button_text=_s(r.get("button_text")),
                message_text=_s(r.get("message_text")),
                sort_order=int(r.get("sort_order")) if not pd.isna(r.get("sort_order")) else 0,
                show_listen=_b(r.get("show_listen", False)),
                show_back=_b(r.get("show_back", True)),
                show_home=_b(r.get("show_home", True)),
                show_not_found=_b(r.get("show_not_found", False)),
                action_url=None if pd.isna(r.get("action_url")) else _s(r.get("action_url")),
                audio_text=_s(r.get("audio")),
            )
            self.nodes[node.node_id] = node

        tmp: Dict[str, List[Tuple[int, str]]] = {}
        for n in self.nodes.values():
            if n.parent_id:
                tmp.setdefault(n.parent_id, []).append((n.sort_order, n.node_id))

        for parent, items in tmp.items():
            self.children[parent] = [nid for _, nid in sorted(items, key=lambda t: t[0])]

        # Optional sheet "Onboarding" with columns: key,value
        self.onb = {}
        try:
            df_onb = pd.read_excel(self.xlsx_path, sheet_name="Onboarding")
            if "key" in df_onb.columns and "value" in df_onb.columns:
                self.onb = {
                    str(k).strip(): ("" if pd.isna(v) else str(v).strip())
                    for k, v in zip(df_onb["key"], df_onb["value"])
                }
        except Exception:
            self.onb = {}

    def get(self, node_id: str) -> Node:
        if node_id not in self.nodes:
            raise KeyError(node_id)
        return self.nodes[node_id]

    def get_children(self, parent_id: str) -> List[str]:
        return self.children.get(parent_id, [])

    def cfg_value(self, key: str, default: str = "") -> str:
        v = self.cfg.get(key)
        return default if v is None else v

    def ui_text(self, key: str, default: str = "") -> str:
        v = self.ui.get(key, "")
        return v if v else default

    def onb_value(self, key: str, default: str = "") -> str:
        v = self.onb.get(key)
        return default if v is None else v



class AppDB:
    def __init__(self, path: str = "bot.sqlite3"):
        self.path = path
        self._ensure()

    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path, check_same_thread=False)

    def _ensure(self) -> None:
        with self._conn() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS user_consent (
                    user_id INTEGER PRIMARY KEY,
                    agreed INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS user_profile (
                    user_id INTEGER PRIMARY KEY,
                    disability INTEGER,
                    status TEXT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS user_actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    payload TEXT,
                    created_at INTEGER NOT NULL
                )
                """
            )
            con.commit()

    # consent
    def get_agreed(self, user_id: int) -> bool:
        with self._conn() as con:
            row = con.execute(
                "SELECT agreed FROM user_consent WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            if not row:
                return False
            return bool(int(row[0]))

    def set_agreed(self, user_id: int, agreed: bool) -> None:
        with self._conn() as con:
            con.execute(
                """
                INSERT INTO user_consent(user_id, agreed, updated_at)
                VALUES(?, ?, strftime('%s','now'))
                ON CONFLICT(user_id) DO UPDATE SET
                    agreed=excluded.agreed,
                    updated_at=excluded.updated_at
                """,
                (user_id, 1 if agreed else 0),
            )
            con.commit()

    # profile
    def ensure_profile_row(self, user_id: int) -> None:
        with self._conn() as con:
            row = con.execute("SELECT user_id FROM user_profile WHERE user_id=?", (user_id,)).fetchone()
            if row:
                return
            now = int(datetime.now(tz=timezone.utc).timestamp())
            con.execute(
                """
                INSERT INTO user_profile(user_id, disability, status, created_at, updated_at)
                VALUES(?, NULL, NULL, ?, ?)
                """,
                (user_id, now, now),
            )
            con.commit()

    def get_profile(self, user_id: int) -> Dict[str, Optional[str]]:
        self.ensure_profile_row(user_id)
        with self._conn() as con:
            row = con.execute(
                "SELECT disability, status FROM user_profile WHERE user_id=?",
                (user_id,),
            ).fetchone()
            if not row:
                return {"disability": None, "status": None}
            disability, status = row
            return {
                "disability": None if disability is None else int(disability),
                "status": None if status is None else str(status),
            }

    def set_disability(self, user_id: int, disability: int) -> None:
        self.ensure_profile_row(user_id)
        with self._conn() as con:
            con.execute(
                """
                UPDATE user_profile
                SET disability=?, updated_at=strftime('%s','now')
                WHERE user_id=?
                """,
                (int(disability), user_id),
            )
            con.commit()

    def set_status(self, user_id: int, status: str) -> None:
        self.ensure_profile_row(user_id)
        with self._conn() as con:
            con.execute(
                """
                UPDATE user_profile
                SET status=?, updated_at=strftime('%s','now')
                WHERE user_id=?
                """,
                (status, user_id),
            )
            con.commit()

    def is_onboarding_complete(self, user_id: int) -> bool:
        p = self.get_profile(user_id)
        return (p["disability"] is not None) and (p["status"] is not None) and (str(p["status"]).strip() != "")

    # actions
    def log_action(self, user_id: int, action: str, payload: str = "") -> None:
        with self._conn() as con:
            con.execute(
                """
                INSERT INTO user_actions(user_id, action, payload, created_at)
                VALUES(?, ?, ?, strftime('%s','now'))
                """,
                (user_id, action, payload or None),
            )
            con.commit()

    def fetch_actions_df(self) -> pd.DataFrame:
        with self._conn() as con:
            df = pd.read_sql_query(
                """
                SELECT
                    id,
                    user_id,
                    action,
                    payload,
                    created_at
                FROM user_actions
                ORDER BY id
                """,
                con,
            )
        return df

    def fetch_profiles_df(self) -> pd.DataFrame:
        with self._conn() as con:
            df = pd.read_sql_query(
                """
                SELECT
                    user_id,
                    disability,
                    status,
                    created_at,
                    updated_at
                FROM user_profile
                """,
                con,
            )
        return df

    def clear_actions_upto_id(self, max_id: int) -> None:
        with self._conn() as con:
            con.execute("DELETE FROM user_actions WHERE id <= ?", (int(max_id),))
            con.commit()



class St(StatesGroup):
    idle = State()
    consent = State()
    onb_disability = State()
    onb_status = State()
    browsing = State()




TG_INLINE_BTN_MAX = 64
MAX_MESSAGE_LEN = int(getattr(config, "MAX_MESSAGE_LEN", 3800))

def safe_btn(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return "-"
    if len(t) <= TG_INLINE_BTN_MAX:
        return t
    return t[:TG_INLINE_BTN_MAX - 1].rstrip() + "…"

def trim(text: str) -> str:
    t = (text or "").strip()
    if len(t) <= MAX_MESSAGE_LEN:
        return t
    return t[:MAX_MESSAGE_LEN - 1].rstrip() + "…"

def not_found_button_if_exists(store: "ContentStore") -> Optional[InlineKeyboardButton]:
    if "NOT_FOUND" not in store.nodes:
        return None
    txt = store.ui_text("btn_not_found", "Не знайшов своє питання")
    if "❓" not in txt:
        txt = "❓ " + txt
    return InlineKeyboardButton(text=safe_btn(txt), callback_data="open:NOT_FOUND")

def nav_rows(store: "ContentStore", *, show_back: bool, show_home: bool) -> List[List[InlineKeyboardButton]]:
    rows: List[List[InlineKeyboardButton]] = []
    btns: List[InlineKeyboardButton] = []

    if show_back:
        txt = store.ui_text("btn_back", "Назад")
        if "⬅️" not in txt:
            txt = "⬅️ " + txt
        btns.append(InlineKeyboardButton(text=safe_btn(txt), callback_data="nav:back"))

    if show_home:
        txt = store.ui_text("btn_home", "На початок")
        if "🏠" not in txt:
            txt = "🏠 " + txt
        btns.append(InlineKeyboardButton(text=safe_btn(txt), callback_data="nav:home"))

    if btns:
        rows.append(btns)
    return rows

def kb_welcome(store: "ContentStore") -> InlineKeyboardMarkup:
    txt = store.ui_text("btn_start", "Старт")
    if "✅" not in txt:
        txt = "✅ " + txt
    rows = [[InlineKeyboardButton(text=safe_btn(txt), callback_data="start")]]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_consent(store: "ContentStore") -> InlineKeyboardMarkup:
    yes = store.ui_text("btn_agree", "Згоден")
    no = store.ui_text("btn_disagree", "Не згоден")
    if "✅" not in yes:
        yes = "✅ " + yes
    if "❌" not in no:
        no = "❌ " + no
    rows: List[List[InlineKeyboardButton]] = [[
        InlineKeyboardButton(text=safe_btn(yes), callback_data="consent:yes"),
        InlineKeyboardButton(text=safe_btn(no), callback_data="consent:no"),
    ]]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_category(store: "ContentStore", parent_id: str) -> InlineKeyboardMarkup:
    # category items
    children = store.get_children(parent_id)
    rows: List[List[InlineKeyboardButton]] = []
    for cid in children:
        rows.append([InlineKeyboardButton(
            text=safe_btn(store.get(cid).button_text),
            callback_data=f"open:{cid}"
        )])

    node = store.get(parent_id)

    # ✅ CHANGE #2: add Back/Home buttons on category level, except ROOT_MENU
    if parent_id != "ROOT_MENU":
        rows += nav_rows(store, show_back=True, show_home=True)

    if node.show_not_found:
        nf = not_found_button_if_exists(store)
        if nf:
            rows.append([nf])

    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_leaf(store: "ContentStore", node: Node) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []

    tts_src = (node.audio_text or "").strip() or (node.message_text or "").strip()
    if node.show_listen and tts_src:
        txt = store.ui_text("btn_listen", "Прослухати")
        if "🔊" not in txt:
            txt = "🔊 " + txt
        rows.append([InlineKeyboardButton(text=safe_btn(txt), callback_data=f"listen:{node.node_id}")])

    rows += nav_rows(store, show_back=node.show_back, show_home=node.show_home)

    if node.show_not_found:
        nf = not_found_button_if_exists(store)
        if nf:
            rows.append([nf])

    return InlineKeyboardMarkup(inline_keyboard=rows)




def onb_disability_text(store: ContentStore) -> str:
    return store.onb_value(
        "disability_question",
        store.cfg_value("disability_question", "Чи маєте Ви інвалідність?")
    )

def onb_disability_buttons(store: ContentStore) -> InlineKeyboardMarkup:
    no_txt = store.onb_value("disability_no", store.cfg_value("disability_no", "Ні, не маю"))
    yes_txt = store.onb_value("disability_yes", store.cfg_value("disability_yes", "Так, маю"))
    rows = [[
        InlineKeyboardButton(text=safe_btn(no_txt), callback_data="onb:dis:0"),
        InlineKeyboardButton(text=safe_btn(yes_txt), callback_data="onb:dis:1"),
    ]]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def onb_status_text(store: ContentStore) -> str:
    return store.onb_value(
        "status_question",
        store.cfg_value("status_question", "Вкажіть ваш статус:")
    )

def onb_status_options(store: ContentStore) -> List[Tuple[str, str]]:
    opts: List[Tuple[str, str]] = []
    for i in range(1, 11):
        c = store.onb_value(f"status_{i}_code", "").strip()
        l = store.onb_value(f"status_{i}_label", "").strip()
        if c and l:
            opts.append((c, l))
    if opts:
        return opts

    return [
        ("idp", "Внутрішньо-переміщена особа (ВПО)"),
        ("returned", "Особа, що повернулася із-за кордону"),
        ("evacuated", "Особа, що була евакуйована"),
        ("local", "Постійно проживаю в громаді"),
    ]

def onb_status_buttons(store: ContentStore) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for code, label in onb_status_options(store):
        rows.append([InlineKeyboardButton(text=safe_btn(label), callback_data=f"onb:status:{code}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)



def sanitize_tts(text: str) -> str:
    t = (text or "").strip()
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t

def audio_path(node_id: str) -> Path:
    audio_dir = getattr(config, "AUDIO_DIR", "audio")
    ext = getattr(config, "AUDIO_EXT", "ogg")

    audio_dir_path = Path(audio_dir)
    if not audio_dir_path.is_absolute():
        audio_dir_path = BASE_DIR / audio_dir_path

    return audio_dir_path / f"{node_id}.{ext}"

async def ensure_audio(node_id: str, text: str) -> Path:
    p = audio_path(node_id)
    p.parent.mkdir(parents=True, exist_ok=True)

    if p.exists():
        try:
            if p.stat().st_size > 0:
                return p
        except OSError:
            pass

    clean = sanitize_tts(text)
    if not clean:
        raise ValueError("Empty text for TTS")

    communicate = edge_tts.Communicate(
        text=clean,
        voice=getattr(config, "TTS_VOICE", "uk-UA-PolinaNeural"),
        rate=getattr(config, "TTS_RATE", "+0%"),
        volume=getattr(config, "TTS_VOLUME", "+0%"),
    )

    timeout_sec = int(getattr(config, "TTS_TIMEOUT_SEC", 25))

    try:
        await asyncio.wait_for(communicate.save(str(p)), timeout=timeout_sec)
    except asyncio.TimeoutError:
        try:
            if p.exists():
                p.unlink()
        except Exception:
            pass
        raise TimeoutError(f"TTS timeout after {timeout_sec}s")

    if not p.exists() or p.stat().st_size == 0:
        raise RuntimeError("TTS produced empty file")

    return p

async def pregenerate_all_audio(store: "ContentStore") -> None:
    total = 0
    done = 0
    for node in store.nodes.values():
        if not node.show_listen:
            continue
        src = (node.audio_text or "").strip() or (node.message_text or "").strip()
        if not src:
            continue
        total += 1
        try:
            await ensure_audio(node.node_id, render_text_with_cfg(store, src))
            done += 1
        except Exception:
            pass
    print(f"[TTS] pregenerate: {done}/{total} files ready in {audio_path('X').parent}")



def stack_push(stack: List[str], cur: Optional[str], nxt: str) -> List[str]:
    s = list(stack or [])
    if cur and cur != nxt:
        s.append(cur)
    return s[-100:]

def stack_pop(stack: List[str]) -> Tuple[List[str], Optional[str]]:
    s = list(stack or [])
    if not s:
        return [], None
    prev = s.pop()
    return s, prev


def render_text_with_cfg(store: "ContentStore", text: str) -> str:
    out = text or ""
    for k, v in store.cfg.items():
        out = out.replace("{" + k + "}", v)
    return out

async def try_delete_message(bot: Bot, chat_id: int, message_id: Optional[int]) -> None:
    if not message_id:
        return
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass

async def cleanup_transient(
    bot: Bot,
    chat_id: int,
    state: FSMContext,
    *,
    delete_pdf: bool = False,
    delete_voice: bool = True,
) -> None:
    data = await state.get_data()

    if delete_voice:
        vid = data.get("last_voice_msg_id")
        if vid:
            await try_delete_message(bot, chat_id, vid)
            await state.update_data(last_voice_msg_id=None)

    if delete_pdf:
        pid = data.get("consent_pdf_msg_id")
        if pid:
            await try_delete_message(bot, chat_id, pid)
            await state.update_data(consent_pdf_msg_id=None)

async def upsert_text(
    bot: Bot,
    chat_id: int,
    state: FSMContext,
    store: "ContentStore",
    text: str,
    kb: InlineKeyboardMarkup
) -> None:
    text = trim(render_text_with_cfg(store, text))
    data = await state.get_data()
    last_id = data.get("last_message_id")

    if last_id:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=last_id,
                text=text,
                reply_markup=kb,
                disable_web_page_preview=True,
            )
            return
        except Exception:
            pass

    msg = await bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=kb,
        disable_web_page_preview=True,
    )
    await state.update_data(last_message_id=msg.message_id)

async def show_rules_pdf(bot: Bot, chat_id: int, state: FSMContext) -> None:
    pdf_path = getattr(config, "RULES_PDF_PATH", "")
    if not pdf_path:
        return

    pdf_p = Path(pdf_path)
    if not pdf_p.is_absolute():
        pdf_p = BASE_DIR / pdf_p

    if not pdf_p.exists():
        return

    data = await state.get_data()
    old_pid = data.get("consent_pdf_msg_id")
    if old_pid:
        await try_delete_message(bot, chat_id, old_pid)
        await state.update_data(consent_pdf_msg_id=None)

    msg = await bot.send_document(chat_id=chat_id, document=FSInputFile(str(pdf_p)))
    await state.update_data(consent_pdf_msg_id=msg.message_id)

def format_category_title(cat: Node) -> str:
    title = (cat.button_text or cat.message_text or "").strip() or "Категорія"
    return f"📌 <b>{title}</b>\n"

def format_leaf_qna(node: Node) -> str:
    q = (node.button_text or "").strip()
    a = (node.message_text or "").strip()
    if q and a:
        return f"❓ <b>{q}</b>\n\n{a}"
    return a or "-"


async def render_welcome(bot: Bot, chat_id: int, state: FSMContext, store: "ContentStore") -> None:
    await cleanup_transient(bot, chat_id, state, delete_pdf=True, delete_voice=True)
    await state.set_state(St.idle)
    await state.update_data(stack=[], current=None)
    text = store.cfg_value("welcome_text", "Вітаємо! Натисніть «Старт», щоб продовжити.")
    await upsert_text(bot, chat_id, state, store, text, kb_welcome(store))

async def render_consent(bot: Bot, chat_id: int, state: FSMContext, store: "ContentStore") -> None:
    await cleanup_transient(bot, chat_id, state, delete_pdf=False, delete_voice=True)
    await state.set_state(St.consent)
    await state.update_data(stack=[], current=None)
    await show_rules_pdf(bot, chat_id, state)

    consent_text = store.cfg_value(
        "consent_text",
        "Використовуючи чат-бот, Ви підтверджуєте ознайомлення та згоду з правилами, зокрема щодо обробки персональних даних."
    )
    await upsert_text(bot, chat_id, state, store, consent_text, kb_consent(store))

async def render_onboarding_disability(bot: Bot, chat_id: int, state: FSMContext, store: "ContentStore") -> None:
    await cleanup_transient(bot, chat_id, state, delete_pdf=True, delete_voice=True)
    await state.set_state(St.onb_disability)
    await upsert_text(bot, chat_id, state, store, onb_disability_text(store), onb_disability_buttons(store))

async def render_onboarding_status(bot: Bot, chat_id: int, state: FSMContext, store: "ContentStore") -> None:
    await cleanup_transient(bot, chat_id, state, delete_pdf=True, delete_voice=True)
    await state.set_state(St.onb_status)
    await upsert_text(bot, chat_id, state, store, onb_status_text(store), onb_status_buttons(store))

async def render_home(bot: Bot, chat_id: int, state: FSMContext, store: "ContentStore") -> None:
    await cleanup_transient(bot, chat_id, state, delete_pdf=True, delete_voice=True)
    await state.set_state(St.browsing)
    await state.update_data(current="ROOT_MENU")
    root = store.get("ROOT_MENU")
    text = root.message_text.strip() or "Оберіть категорію:"
    await upsert_text(bot, chat_id, state, store, text, kb_category(store, "ROOT_MENU"))

async def render_node(
    bot: Bot,
    chat_id: int,
    state: FSMContext,
    store: "ContentStore",
    node_id: str
) -> None:
    if node_id == "ROOT_MENU":
        await render_home(bot, chat_id, state, store)
        return

    await cleanup_transient(bot, chat_id, state, delete_pdf=True, delete_voice=True)

    await state.set_state(St.browsing)
    node = store.get(node_id)
    await state.update_data(current=node_id)

    if node.node_type == "category":
        header = format_category_title(node)
        body = (node.message_text or "").strip() or "Оберіть питання:"

        title = (node.button_text or "").strip()
        if title:
            if body.startswith(title):
                body = body[len(title):].lstrip()
            body = body.lstrip("\n").lstrip()

        await upsert_text(
            bot,
            chat_id,
            state,
            store,
            header + body,
            kb_category(store, node.node_id),
        )
        return

    await upsert_text(
        bot,
        chat_id,
        state,
        store,
        format_leaf_qna(node),
        kb_leaf(store, node),
    )



def _kyiv_tz():
    if ZoneInfo:
        return ZoneInfo(getattr(config, "REPORT_TZ", "Europe/Kyiv"))
    return None

def to_kyiv_str(ts_utc: datetime) -> str:
    tz = _kyiv_tz()
    if tz is None:
        return ts_utc.astimezone(timezone(timedelta(hours=2))).strftime("%Y-%m-%d %H:%M:%S")
    return ts_utc.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")

def next_monday_0800_kyiv(now_utc: datetime) -> datetime:
    tz = _kyiv_tz()
    if tz is None:
        kyiv = now_utc.astimezone(timezone(timedelta(hours=2)))
        tz = kyiv.tzinfo
    now_local = now_utc.astimezone(tz)

    days_ahead = (0 - now_local.weekday()) % 7
    target = (now_local + timedelta(days=days_ahead)).replace(hour=8, minute=0, second=0, microsecond=0)
    if target <= now_local:
        target = target + timedelta(days=7)
    return target.astimezone(timezone.utc)

def chunk_dataframe(df: pd.DataFrame, max_rows: int) -> List[pd.DataFrame]:
    if len(df) <= max_rows:
        return [df]
    chunks = []
    for i in range(0, len(df), max_rows):
        chunks.append(df.iloc[i:i + max_rows].copy())
    return chunks

def _status_code_to_label(store: ContentStore, code: Optional[str]) -> str:
    if not code:
        return ""
    mp = {c: l for c, l in onb_status_options(store)}
    return mp.get(str(code), str(code))

def _disability_to_label(store: ContentStore, val: Optional[int]) -> str:
    if val is None:
        return ""
    if int(val) == 1:
        return store.onb_value("disability_yes", store.cfg_value("disability_yes", "Так, маю"))
    return store.onb_value("disability_no", store.cfg_value("disability_no", "Ні, не маю"))

def _node_label_safe(store: ContentStore, node_id: str) -> str:
    try:
        n = store.get(node_id)
        return (n.button_text or n.message_text or node_id).strip()
    except Exception:
        return node_id
def _payload_str(payload: Any) -> str:
    try:
        if payload is None:
            return ""
        # pandas NaN often becomes float
        if isinstance(payload, float) and pd.isna(payload):
            return ""
        if pd.isna(payload):
            return ""
    except Exception:
        pass
    return str(payload).strip()

def _human_event(store: ContentStore, action: str, payload: Any) -> Tuple[str, str]:
    p = _payload_str(payload)

    if action == "cmd_start":
        return ("Команда", "/start")
    if action == "click_start":
        return ("Клік", "Старт")
    if action == "consent_choice":
        if p == "yes":
            return ("Згода", "Згоден")
        if p == "no":
            return ("Згода", "Не згоден")
        return ("Згода", p)
    if action == "onboarding_disability":
        try:
            return ("Онбординг - інвалідність", _disability_to_label(store, int(p)))
        except Exception:
            return ("Онбординг - інвалідність", p)
    if action == "onboarding_status":
        return ("Онбординг - статус", _status_code_to_label(store, p))
    if action == "open_node":
        return ("Меню", _node_label_safe(store, p))
    if action == "nav_back":
        return ("Навігація", "Назад")
    if action == "nav_home":
        return ("Навігація", "На початок")
    if action == "listen":
        node_id = p.split(":", 1)[1] if ":" in p else p
        return ("Озвучення", _node_label_safe(store, node_id))
    if action == "send_test":
        return ("Адмін", "Тестова відправка звіту")
    if action == "message":
        return ("Повідомлення", p)

    return (action, p)

def _excel_actions_sheet_max_rows() -> int:
    excel_limit = 1_048_576
    header_rows = 1
    default_safe = 1_000_000
    raw_value = getattr(
        config,
        "REPORT_MAX_ROWS_PER_SHEET",
        getattr(config, "REPORT_MAX_ROWS_PER_FILE", default_safe),
    )

    try:
        max_rows = int(raw_value)
    except Exception:
        max_rows = default_safe

    max_rows = max(1, max_rows)
    return min(max_rows, excel_limit - header_rows)


def build_report_files(db: AppDB, store: ContentStore, out_dir: Path) -> Tuple[List[Path], int, int, int, int]:
    print("[REPORT] build files start")

    df_actions = db.fetch_actions_df()
    total_rows = len(df_actions)
    if total_rows == 0:
        print("[REPORT] no actions to export")
        return [], 0, 0, 0, 0

    df_profiles = db.fetch_profiles_df()
    profiles = df_profiles.copy()
    profiles["disability_human"] = profiles["disability"].apply(
        lambda x: _disability_to_label(store, None if pd.isna(x) else int(x))
    )
    profiles["status_human"] = profiles["status"].apply(
        lambda x: _status_code_to_label(store, "" if pd.isna(x) else str(x))
    )

    df = df_actions.copy()

    df["created_at_utc"] = df["created_at"].apply(
        lambda s: datetime.fromtimestamp(int(s), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    )
    df["created_at_kyiv"] = df["created_at"].apply(
        lambda s: to_kyiv_str(datetime.fromtimestamp(int(s), tz=timezone.utc))
    )

    ev_type: List[str] = []
    ev_value: List[str] = []
    for _, r in df.iterrows():
        t, v = _human_event(store, str(r["action"]), r.get("payload"))
        ev_type.append(t)
        ev_value.append(v)
    df["event_type"] = ev_type
    df["event_value"] = ev_value

    df = df.merge(
        profiles[["user_id", "disability_human", "status_human"]],
        on="user_id",
        how="left",
    )

    df_out = df[[
        "id",
        "user_id",
        "disability_human",
        "status_human",
        "event_type",
        "event_value",
        "created_at_utc",
        "created_at_kyiv",
    ]].rename(columns={
        "id": "row_id",
        "user_id": "tg_user_id",
        "disability_human": "інвалідність",
        "status_human": "статус",
        "event_type": "тип_події",
        "event_value": "значення",
        "created_at_utc": "час_utc",
        "created_at_kyiv": "час_київ",
    })

    users = profiles.copy()
    users["created_at_utc"] = users["created_at"].apply(
        lambda s: datetime.fromtimestamp(int(s), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    )
    users["updated_at_utc"] = users["updated_at"].apply(
        lambda s: datetime.fromtimestamp(int(s), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    )
    users_out = users[[
        "user_id",
        "disability_human",
        "status_human",
        "created_at_utc",
        "updated_at_utc",
    ]].rename(columns={
        "user_id": "tg_user_id",
        "disability_human": "інвалідність",
        "status_human": "статус",
        "created_at_utc": "created_utc",
        "updated_at_utc": "updated_utc",
    }).sort_values("tg_user_id")

    max_rows_per_sheet = _excel_actions_sheet_max_rows()
    chunks = chunk_dataframe(df_out, max_rows=max_rows_per_sheet)

    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    report_path = out_dir / f"user_actions_report_{ts}.xlsx"

    exported_rows = 0
    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
        summary = pd.DataFrame([{
            "total_action_rows": total_rows,
            "actions_sheets": len(chunks),
            "max_rows_per_actions_sheet": max_rows_per_sheet,
            "generated_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        }])
        summary.to_excel(writer, index=False, sheet_name="summary")
        users_out.to_excel(writer, index=False, sheet_name="users")

        for idx, part in enumerate(chunks, 1):
            sheet_name = f"actions_{idx}"
            part.to_excel(writer, index=False, sheet_name=sheet_name)
            exported_rows += len(part)
            print(f"[REPORT] wrote {sheet_name}: {len(part)} rows")

    max_id = int(df_actions["id"].max())
    print(
        f"[REPORT] build files done: file={report_path}, "
        f"exported_rows={exported_rows}, total_rows={total_rows}, sheets={len(chunks)}"
    )
    return [report_path], exported_rows, total_rows, max_id, len(chunks)

def send_email_smtp(subject: str, body: str, attachments: List[Path]) -> None:
    import socket

    host = getattr(config, "SMTP_HOST", "")
    port = int(getattr(config, "SMTP_PORT", 587))
    user = getattr(config, "SMTP_USER", "")
    password = getattr(config, "SMTP_PASSWORD", "")
    use_ssl = bool(getattr(config, "SMTP_USE_SSL", False))
    timeout_sec = int(getattr(config, "SMTP_TIMEOUT_SEC", 30))

    mail_from = getattr(config, "SMTP_FROM", user)
    to_list = getattr(config, "REPORT_TO", [])
    if isinstance(to_list, str):
        to_list = [x.strip() for x in to_list.split(",") if x.strip()]

    if not host or not user or not password or not to_list:
        raise RuntimeError("SMTP settings are incomplete (host/user/password/REPORT_TO).")

    msg = EmailMessage()
    msg["From"] = mail_from
    msg["To"] = ", ".join(to_list)
    msg["Date"] = formatdate(localtime=True)
    msg["Subject"] = subject
    msg.set_content(body)

    for p in attachments:
        data = p.read_bytes()
        msg.add_attachment(
            data,
            maintype="application",
            subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=p.name,
        )

    print(
        f"[EMAIL] SMTP connect IPv4 only: host={host}, port={port}, "
        f"ssl={use_ssl}, timeout={timeout_sec}",
        flush=True
    )

    original_getaddrinfo = socket.getaddrinfo

    def getaddrinfo_ipv4_only(*args, **kwargs):
        return original_getaddrinfo(
            args[0],
            args[1],
            socket.AF_INET,
            socket.SOCK_STREAM
        )

    socket.getaddrinfo = getaddrinfo_ipv4_only

    try:
        if use_ssl:
            with smtplib.SMTP_SSL(host, port, timeout=timeout_sec) as s:
                print("[EMAIL] SMTP SSL login start", flush=True)
                s.login(user, password)
                print("[EMAIL] SMTP SSL send start", flush=True)
                s.send_message(msg)
        else:
            with smtplib.SMTP(host, port, timeout=timeout_sec) as s:
                print("[EMAIL] SMTP EHLO", flush=True)
                s.ehlo()

                print("[EMAIL] SMTP STARTTLS", flush=True)
                s.starttls()

                print("[EMAIL] SMTP EHLO after STARTTLS", flush=True)
                s.ehlo()

                print("[EMAIL] SMTP login start", flush=True)
                s.login(user, password)

                print("[EMAIL] SMTP send start", flush=True)
                s.send_message(msg)

        print("[EMAIL] SMTP sent", flush=True)

    finally:
        socket.getaddrinfo = original_getaddrinfo

def send_email_sendgrid(subject: str, body: str, attachments: List[Path]) -> None:
    if requests is None:
        raise RuntimeError("requests is not installed. Install it or use SMTP.")

    api_key = getattr(config, "SENDGRID_API_KEY", "")
    email_from = getattr(config, "EMAIL_FROM", "")
    to_list = getattr(config, "REPORT_TO", [])

    if isinstance(to_list, str):
        to_list = [x.strip() for x in to_list.split(",") if x.strip()]

    if not api_key or not email_from or not to_list:
        raise RuntimeError("SendGrid settings are incomplete (SENDGRID_API_KEY/EMAIL_FROM/REPORT_TO).")

    sg_url = "https://api.sendgrid.com/v3/mail/send"

    def _b64(p: Path) -> str:
        import base64
        return base64.b64encode(p.read_bytes()).decode("utf-8")

    payload = {
        "personalizations": [{"to": [{"email": x} for x in to_list]}],
        "from": {"email": email_from},
        "subject": subject,
        "content": [{"type": "text/plain", "value": body}],
        "attachments": [
            {
                "content": _b64(p),
                "type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "filename": p.name,
                "disposition": "attachment",
            }
            for p in attachments
        ],
    }

    r = requests.post(
        sg_url,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        data=json.dumps(payload),
        timeout=30,
    )
    if r.status_code not in (200, 202):
        raise RuntimeError(f"SendGrid HTTP {r.status_code}: {r.text[:300]}")

def send_email_with_attachments(subject: str, body: str, attachments: List[Path]) -> str:

    api_key = getattr(config, "SENDGRID_API_KEY", "")
    if api_key:
        send_email_sendgrid(subject, body, attachments)
        return "sendgrid"
    send_email_smtp(subject, body, attachments)
    return "smtp"

async def send_report_files_to_admins(bot: Bot, files: List[Path], body: str) -> None:
    admin_ids = getattr(config, "ADMIN_USER_IDS", [])

    if isinstance(admin_ids, int):
        admin_ids = [admin_ids]

    admin_ids = [int(x) for x in admin_ids if str(x).strip()]

    if not admin_ids:
        raise RuntimeError("ADMIN_USER_IDS is empty. Cannot send report to Telegram.")

    for uid in admin_ids:
        for p in files:
            if not p.exists():
                continue

            await bot.send_document(
                chat_id=uid,
                document=FSInputFile(str(p)),
                caption=body[:1000],
            )

async def run_weekly_report(db: AppDB, bot: Bot, store: ContentStore, reason: str = "scheduled") -> Tuple[str, int, int]:
    out_dir = BASE_DIR / getattr(config, "REPORT_OUT_DIR", "reports")

    files, exported_rows, total_rows, max_id, sheets_count = await asyncio.to_thread(
        build_report_files,
        db,
        store,
        out_dir,
    )

    if total_rows == 0:
        return ("", 0, 0)

    if exported_rows != total_rows:
        raise RuntimeError(f"Export mismatch: exported_rows={exported_rows}, total_rows={total_rows}")

    total_size_mb = sum(p.stat().st_size for p in files if p.exists()) / 1024 / 1024

    body = (
        "Звіт дій користувачів.\n\n"
        f"Причина запуску: {reason}\n"
        f"Рядків: {total_rows}\n"
        f"Excel-файлів: {len(files)}\n"
        f"Листів actions у Excel: {sheets_count}\n"
        f"Розмір вкладення: {total_size_mb:.2f} MB\n"
        f"Час UTC: {datetime.utcnow().isoformat()}\n"
    )

    delivery = getattr(config, "REPORT_DELIVERY", "telegram").strip().lower()

    if delivery == "telegram":
        print("[REPORT] telegram send start", flush=True)
        await send_report_files_to_admins(bot, files, body)
        print("[REPORT] telegram send done", flush=True)
        channel = "telegram"
    else:
        subject = getattr(config, "REPORT_SUBJECT", "Weekly bot report")
        subject = f"{subject} ({reason})"

        print("[REPORT] email send start", flush=True)
        channel = await asyncio.to_thread(
            send_email_with_attachments,
            subject,
            body,
            files,
        )
        print(f"[REPORT] email send done: channel={channel}", flush=True)

    db.clear_actions_upto_id(int(max_id))
    print(f"[REPORT] cleared actions up to id={max_id}", flush=True)

    return (channel, int(total_rows), int(sheets_count))

async def report_scheduler(db: AppDB, bot: Bot, store: ContentStore) -> None:
    while True:
        now_utc = datetime.now(tz=timezone.utc)
        target_utc = next_monday_0800_kyiv(now_utc)
        sleep_sec = max(5.0, (target_utc - now_utc).total_seconds())
        await asyncio.sleep(sleep_sec)
        try:
            await run_weekly_report(db, bot, store, reason="scheduled")
        except Exception as e:
            admin_ids = getattr(config, "ADMIN_USER_IDS", [])
            if isinstance(admin_ids, int):
                admin_ids = [admin_ids]
            text = f"⚠️ Report failed: {type(e).__name__}: {e}"
            for uid in admin_ids or []:
                try:
                    await bot.send_message(chat_id=int(uid), text=text)
                except Exception:
                    pass



_xlsx = getattr(config, "XLSX_PATH", "1.xlsx")
_xlsx_p = Path(_xlsx)
if not _xlsx_p.is_absolute():
    _xlsx_p = BASE_DIR / _xlsx_p
store = ContentStore(str(_xlsx_p))

_db_path = getattr(config, "DB_PATH", "bot.sqlite3")
_db_p = Path(_db_path)
if not _db_p.is_absolute():
    _db_p = BASE_DIR / _db_p
db = AppDB(str(_db_p))

router = Router()



async def maybe_start_onboarding(bot: Bot, chat_id: int, state: FSMContext, user_id: int) -> bool:
    if not db.get_agreed(user_id):
        return False
    if db.is_onboarding_complete(user_id):
        return False
    p = db.get_profile(user_id)
    if p["disability"] is None:
        await render_onboarding_disability(bot, chat_id, state, store)
        return True
    if p["status"] is None or str(p["status"]).strip() == "":
        await render_onboarding_status(bot, chat_id, state, store)
        return True
    return False



@router.message(CommandStart())
async def start_cmd(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    db.log_action(user_id, "cmd_start", "")
    if db.get_agreed(user_id):
        if await maybe_start_onboarding(message.bot, message.chat.id, state, user_id):
            return
        await render_home(message.bot, message.chat.id, state, store)
    else:
        await render_welcome(message.bot, message.chat.id, state, store)

@router.message()
async def any_message(message: Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    text = (message.text or "").strip()
    if text.lower() == "send_test":
        db.log_action(user_id, "send_test", "")
        admin_ids = getattr(config, "ADMIN_USER_IDS", [])
        if isinstance(admin_ids, int):
            admin_ids = [admin_ids]
        if admin_ids and (user_id not in [int(x) for x in admin_ids]):
            await message.answer("⛔ Доступ заборонено.")
            return

        await message.answer("📨 Формую та відправляю тестовий звіт…")
        try:
            channel, rows, sheets_count = await run_weekly_report(db, message.bot, store, reason="send_test")
            if rows == 0:
                await message.answer("ℹ️ Даних для звіту немає (таблиця user_actions порожня).")
            else:
                if channel == "telegram":
                    await message.answer(
                        f"✅ Звіт відправлено адміну в Telegram. Рядків у звіті: {rows}. Листів actions у Excel: {sheets_count}."
                    )
                elif channel == "sendgrid":
                    await message.answer(
                        f"✅ SendGrid прийняв лист. Рядків у звіті: {rows}. Листів actions у Excel: {sheets_count}. Перевір Inbox/Spam."
                    )
                else:
                    await message.answer(
                        f"✅ SMTP відправлено. Рядків у звіті: {rows}. Листів actions у Excel: {sheets_count}. Перевір Inbox/Spam."
                    )
        except Exception as e:
            await message.answer(f"⚠️ Помилка відправки: {type(e).__name__}: {e}")
        return

    db.log_action(user_id, "message", (text[:200] if text else ""))

    if db.get_agreed(user_id):
        if await maybe_start_onboarding(message.bot, message.chat.id, state, user_id):
            return
        await render_home(message.bot, message.chat.id, state, store)
    else:
        await render_welcome(message.bot, message.chat.id, state, store)

@router.callback_query(F.data == "start")
async def cb_start(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    if cb.message:
        await state.update_data(last_message_id=cb.message.message_id)

    user_id = cb.from_user.id
    db.log_action(user_id, "click_start", "")

    if db.get_agreed(user_id):
        if await maybe_start_onboarding(cb.bot, cb.message.chat.id, state, user_id):
            return
        await render_home(cb.bot, cb.message.chat.id, state, store)
    else:
        await render_consent(cb.bot, cb.message.chat.id, state, store)

@router.callback_query(F.data.startswith("consent:"))
async def cb_consent(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    if cb.message:
        await state.update_data(last_message_id=cb.message.message_id)

    user_id = cb.from_user.id
    chat_id = cb.message.chat.id

    choice = cb.data.split(":", 1)[1].strip().lower()
    db.log_action(user_id, "consent_choice", choice)

    if choice == "yes":
        db.set_agreed(user_id, True)
        db.ensure_profile_row(user_id)
        await cleanup_transient(cb.bot, chat_id, state, delete_pdf=True, delete_voice=True)
        await render_onboarding_disability(cb.bot, chat_id, state, store)
    else:
        db.set_agreed(user_id, False)
        await cleanup_transient(cb.bot, chat_id, state, delete_pdf=True, delete_voice=True)
        await render_welcome(cb.bot, chat_id, state, store)

@router.callback_query(F.data.startswith("onb:dis:"))
async def cb_onb_dis(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    if cb.message:
        await state.update_data(last_message_id=cb.message.message_id)

    user_id = cb.from_user.id
    chat_id = cb.message.chat.id
    val = cb.data.split(":", 2)[2].strip()
    disability = 1 if val == "1" else 0
    db.set_disability(user_id, disability)
    db.log_action(user_id, "onboarding_disability", str(disability))

    await render_onboarding_status(cb.bot, chat_id, state, store)

@router.callback_query(F.data.startswith("onb:status:"))
async def cb_onb_status(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    if cb.message:
        await state.update_data(last_message_id=cb.message.message_id)

    user_id = cb.from_user.id
    chat_id = cb.message.chat.id
    code = cb.data.split(":", 2)[2].strip()

    valid_codes = {c for c, _ in onb_status_options(store)}
    if valid_codes and code not in valid_codes:
        await cb.answer("Некоректний вибір", show_alert=True)
        return

    db.set_status(user_id, code)
    db.log_action(user_id, "onboarding_status", code)

    await render_home(cb.bot, chat_id, state, store)

@router.callback_query(F.data.startswith("open:"))
async def cb_open(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    user_id = cb.from_user.id
    chat_id = cb.message.chat.id

    if cb.message:
        await state.update_data(last_message_id=cb.message.message_id)

    node_id = cb.data.split(":", 1)[1].strip()

    db.log_action(user_id, "open_node", node_id)

    if node_id != "NOT_FOUND" and not db.get_agreed(user_id):
        await render_welcome(cb.bot, chat_id, state, store)
        return

    if db.get_agreed(user_id) and (not db.is_onboarding_complete(user_id)):
        if await maybe_start_onboarding(cb.bot, chat_id, state, user_id):
            return

    try:
        store.get(node_id)
    except Exception:
        if db.get_agreed(user_id):
            await render_home(cb.bot, chat_id, state, store)
        else:
            await render_welcome(cb.bot, chat_id, state, store)
        return

    data = await state.get_data()
    cur = data.get("current")
    stack = data.get("stack") or []
    stack = stack_push(stack, cur, node_id)
    await state.update_data(stack=stack)

    await render_node(cb.bot, chat_id, state, store, node_id)

@router.callback_query(F.data == "nav:home")
async def cb_home(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    user_id = cb.from_user.id
    chat_id = cb.message.chat.id

    if cb.message:
        await state.update_data(last_message_id=cb.message.message_id)

    db.log_action(user_id, "nav_home", "")

    if not db.get_agreed(user_id):
        await render_welcome(cb.bot, chat_id, state, store)
        return

    if await maybe_start_onboarding(cb.bot, chat_id, state, user_id):
        return

    await state.update_data(stack=[], current="ROOT_MENU")
    await render_home(cb.bot, chat_id, state, store)

@router.callback_query(F.data == "nav:back")
async def cb_back(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    user_id = cb.from_user.id
    chat_id = cb.message.chat.id

    if cb.message:
        await state.update_data(last_message_id=cb.message.message_id)

    db.log_action(user_id, "nav_back", "")

    if not db.get_agreed(user_id):
        await render_welcome(cb.bot, chat_id, state, store)
        return

    if await maybe_start_onboarding(cb.bot, chat_id, state, user_id):
        return

    data = await state.get_data()
    stack = data.get("stack") or []
    stack, prev = stack_pop(stack)
    await state.update_data(stack=stack)

    if not prev:
        await render_home(cb.bot, chat_id, state, store)
        return

    try:
        store.get(prev)
    except Exception:
        await render_home(cb.bot, chat_id, state, store)
        return

    await render_node(cb.bot, chat_id, state, store, prev)

@router.callback_query(F.data.startswith("listen:"))
async def cb_listen(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer("🔊 Озвучую…", cache_time=1)

    user_id = cb.from_user.id
    chat_id = cb.message.chat.id

    if cb.message:
        await state.update_data(last_message_id=cb.message.message_id)

    db.log_action(user_id, "listen", cb.data)

    if not db.get_agreed(user_id):
        await render_welcome(cb.bot, chat_id, state, store)
        return

    if await maybe_start_onboarding(cb.bot, chat_id, state, user_id):
        return

    node_id = cb.data.split(":", 1)[1].strip()
    try:
        node = store.get(node_id)
    except Exception:
        await render_home(cb.bot, chat_id, state, store)
        return

    tts_text = (node.audio_text or "").strip() or (node.message_text or "").strip()
    if not tts_text:
        await cb.answer("Немає тексту для озвучення", show_alert=True)
        return

    await cleanup_transient(cb.bot, chat_id, state, delete_pdf=False, delete_voice=True)

    try:
        try:
            await cb.bot.send_chat_action(chat_id=chat_id, action="record_voice")
        except Exception:
            pass

        p = await ensure_audio(node_id, render_text_with_cfg(store, tts_text))
        voice_msg = await cb.bot.send_voice(chat_id=chat_id, voice=FSInputFile(str(p)))
        await state.update_data(last_voice_msg_id=voice_msg.message_id)

    except TimeoutError:
        await cb.bot.send_message(
            chat_id=chat_id,
            text="⏳ Озвучення зараз займає занадто багато часу. Спробуйте ще раз через хвилину.",
        )
    except Exception as e:
        await cb.bot.send_message(
            chat_id=chat_id,
            text=f"⚠️ Не вдалося озвучити. Помилка: {type(e).__name__}: {e}",
        )

async def main() -> None:
    if not getattr(config, "BOT_TOKEN", ""):
        raise RuntimeError("BOT_TOKEN is empty. Set BOT_TOKEN in config.py or env.")

    store.load()

    if bool(getattr(config, "PREGENERATE_AUDIO", False)):
        await pregenerate_all_audio(store)

    bot = Bot(
        token=config.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    if bool(getattr(config, "REPORT_ENABLE", True)):
        asyncio.create_task(report_scheduler(db, bot, store))

    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])

if __name__ == "__main__":
    asyncio.run(main())

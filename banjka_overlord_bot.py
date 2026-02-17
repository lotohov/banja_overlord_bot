import os
import logging
import asyncio
import sqlite3
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from telegram import Update, Bot, ReplyKeyboardMarkup, KeyboardButton, ChatMember
from telegram.error import InvalidToken
from telegram.request import HTTPXRequest
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    MessageHandler,
    filters,
)

load_dotenv()

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Настройка логгера
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

log_path = os.getenv("LOG_PATH", "bot.log")
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
try:
    handler = RotatingFileHandler(
        log_path, maxBytes=1024*1024, backupCount=5, encoding='utf-8'
    )
except OSError:
    fallback_path = "/tmp/banjka_overlord_bot.log"
    handler = RotatingFileHandler(
        fallback_path, maxBytes=1024*1024, backupCount=5, encoding='utf-8'
    )
handler.setFormatter(formatter)
logger.addHandler(handler)

console = logging.StreamHandler()
console.setFormatter(formatter)
logger.addHandler(console)

# Слой хранения: инициализация SQLite и миграции схемы.
class Database:
    def __init__(self):
        db_path = os.getenv("DATABASE_PATH", "banja.db")
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self._init_tables()
    
    # Создаёт базовые таблицы, если они ещё не существуют.
    def _init_tables(self):
        tables = [
            '''CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY,
                next_date DATETIME,
                interval INTEGER,
                is_active BOOLEAN,
                reg_start DATETIME)''',
            '''CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                mention_enabled BOOLEAN DEFAULT 1)''',
            '''CREATE TABLE IF NOT EXISTS registrations (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                event_id INTEGER,
                reg_time DATETIME,
                FOREIGN KEY(user_id) REFERENCES users(user_id),
                FOREIGN KEY(event_id) REFERENCES events(id))''',
            '''CREATE TABLE IF NOT EXISTS golden_stats (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                event_id INTEGER,
                event_date DATETIME,
                assigned_at DATETIME,
                FOREIGN KEY(user_id) REFERENCES users(user_id),
                FOREIGN KEY(event_id) REFERENCES events(id))''',
            '''CREATE TABLE IF NOT EXISTS notification_settings (
                user_id INTEGER PRIMARY KEY,
                notify_enabled BOOLEAN DEFAULT 1,
                FOREIGN KEY(user_id) REFERENCES users(user_id))'''
        ]
        for table in tables:
            self.cursor.execute(table)
        self._migrate_golden_stats()
        self.conn.commit()

    # Мягкая миграция legacy-схемы golden_stats без потери данных.
    def _migrate_golden_stats(self):
        self.cursor.execute("PRAGMA table_info(golden_stats)")
        columns = {row[1] for row in self.cursor.fetchall()}
        if "event_date" not in columns:
            self.cursor.execute("ALTER TABLE golden_stats ADD COLUMN event_date DATETIME")
        if "assigned_at" not in columns:
            self.cursor.execute("ALTER TABLE golden_stats ADD COLUMN assigned_at DATETIME")
    
    def close(self):
        self.conn.close()

# Основное приложение бота: конфигурация, хендлеры и бизнес-логика.
class BotApp:
    RUS_DAYS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    KEYBOARD = ReplyKeyboardMarkup(
        [[KeyboardButton("✨ Записаться"), KeyboardButton("➖ Отменить запись")],
         [KeyboardButton("📊 Статистика"), KeyboardButton("🔔 Управление уведомлениями")],
         [KeyboardButton("📜 Правила")]],
        resize_keyboard=True
    )

    # Инициализация зависимостей, окружения и Telegram-приложения.
    def __init__(self):
        self.db = Database()
        self.BOT_TIMEZONE = os.getenv("BOT_TIMEZONE", "Europe/Moscow")
        # Все плановые уведомления считаются в этой timezone, чтобы не было сдвига из-за UTC контейнера.
        self.scheduler = AsyncIOScheduler(timezone=ZoneInfo(self.BOT_TIMEZONE))
        proxy_url = os.getenv("PROXY_URL")
        request = HTTPXRequest(
            connect_timeout=10,
            read_timeout=30,
            write_timeout=30,
            pool_timeout=10,
            proxy_url=proxy_url,
        )
        self.application = (
            ApplicationBuilder()
            .token(os.getenv("TELEGRAM_TOKEN"))
            .request(request)
            .build()
        )
        self.bot = Bot(os.getenv("TELEGRAM_TOKEN"), request=request)
        self.CHANNEL_ID = os.getenv("CHANNEL_ID")
        self.MAX_PARTICIPANTS = int(os.getenv("MAX_PARTICIPANTS", 20))
        self.CLOSE_AT = int(os.getenv("CLOSE_AT", 25))
        self.CANCEL_BEFORE_HOURS = int(os.getenv("CANCEL_BEFORE_HOURS", 24))
        allowed_users_raw = os.getenv("SETDATE_ALLOWED_USERS", "")
        self.SETDATE_ALLOWED_USERS = {
            int(user_id.strip())
            for user_id in allowed_users_raw.split(",")
            if user_id.strip().isdigit()
        }
        self._register_handlers()
        logger.info("Bot initialized and handlers registered")

    # Управление жизненным циклом событий и плановыми уведомлениями.
    class EventManager:
        def __init__(self, outer):
            self.outer = outer

        # Создаёт новое активное событие, выключая предыдущее.
        async def create_event(self, start_date: datetime, interval: int):
            self.outer.db.cursor.execute('UPDATE events SET is_active = 0')
            self.outer.db.cursor.execute(
                '''INSERT INTO events (next_date, interval, is_active, reg_start)
                VALUES (?, ?, 1, CURRENT_TIMESTAMP)''',
                (start_date, interval)
            )
            self.outer.db.conn.commit()
            event_id = self.outer.db.cursor.lastrowid
            await self._schedule_notifications(start_date, event_id, interval)
            await self._update_channel_name(start_date)

        # Планирует все уведомления и автосоздание следующего события.
        async def _schedule_notifications(self, event_date: datetime, event_id: int, interval: int):
            notifications = [
                (event_date - timedelta(days=3), "Напоминание: Мероприятие через 3 дня!"),
                (event_date - timedelta(days=1), "Напоминание: Мероприятие завтра!"),
                (event_date - timedelta(days=2), "LOW_FILL_2_DAYS"),
                (event_date - timedelta(days=1), "LOW_FILL_1_DAY"),
                (event_date - timedelta(hours=1), "Мероприятие через 1 час! Участники:"),
                (event_date - timedelta(minutes=1), "Старт через 1 минуту! Золотые участники:"),
            ]

            for notify_time, message in notifications:
                self.outer.scheduler.add_job(
                    self.send_notification,
                    DateTrigger(run_date=notify_time),
                    args=(message, event_id)
                )

            self.outer.scheduler.add_job(
                self._create_next_event,
                DateTrigger(run_date=event_date),
                args=(event_id, interval)
            )

        # По завершении текущего события создаёт следующее по интервалу.
        async def _create_next_event(self, event_id: int, interval: int):
            self.outer.db.cursor.execute('SELECT next_date FROM events WHERE id = ?', (event_id,))
            result = self.outer.db.cursor.fetchone()
            if result:
                next_date = datetime.strptime(result[0].split('.')[0], "%Y-%m-%d %H:%M:%S")
                new_date = next_date + timedelta(days=interval)
                await self.create_event(new_date, interval)

        # Обновляет название чата/канала под дату ближайшего события.
        async def _update_channel_name(self, event_date: datetime):
            day_of_week = self.outer.RUS_DAYS[event_date.weekday()]
            new_name = f"Банька {event_date.strftime('%d.%m')} {day_of_week} {event_date.strftime('%H:%M')}"
            try:
                await self.outer.bot.set_chat_title(
                    chat_id=self.outer.CHANNEL_ID,
                    title=new_name
                )
            except Exception as e:
                logger.error(f"Ошибка обновления названия: {e}")

        # Персональные ЛС-напоминания за 1 час для участников с включённым notify_enabled.
        async def _send_hour_personal_notifications(self, event_id: int):
            self.outer.db.cursor.execute(
                'SELECT next_date FROM events WHERE id = ?',
                (event_id,)
            )
            row = self.outer.db.cursor.fetchone()
            event_time_text = "скоро"
            if row and row[0]:
                try:
                    event_time = datetime.strptime(row[0].split('.')[0], DATE_FORMAT)
                    event_time_text = event_time.strftime("%d.%m.%Y %H:%M")
                except (ValueError, IndexError, TypeError):
                    pass

            self.outer.db.cursor.execute('''
                SELECT DISTINCT r.user_id
                FROM registrations r
                LEFT JOIN notification_settings ns ON ns.user_id = r.user_id
                WHERE r.event_id = ? AND COALESCE(ns.notify_enabled, 1) = 1
            ''', (event_id,))
            recipients = [row[0] for row in self.outer.db.cursor.fetchall()]

            text = (
                "Напоминание: мероприятие через 1 час.\n"
                f"Начало: {event_time_text}"
            )
            for user_id in recipients:
                try:
                    await self.outer.bot.send_message(chat_id=user_id, text=text)
                except Exception as e:
                    logger.warning("Не удалось отправить ЛС user_id=%s: %s", user_id, e)

        # Унифицированная отправка канал-уведомлений, включая динамические payload.
        async def send_notification(self, message: str, event_id: int):
            if message in ("LOW_FILL_2_DAYS", "LOW_FILL_1_DAY"):
                self.outer.db.cursor.execute(
                    'SELECT COUNT(*) FROM registrations WHERE event_id = ?',
                    (event_id,)
                )
                registered_count = self.outer.db.cursor.fetchone()[0]
                if registered_count >= self.outer.MAX_PARTICIPANTS:
                    return

                self.outer.db.cursor.execute('''
                    SELECT u.full_name
                    FROM registrations r
                    JOIN users u ON r.user_id = u.user_id
                    WHERE r.event_id = ?
                    ORDER BY r.reg_time ASC
                ''', (event_id,))
                participants = [row[0] for row in self.outer.db.cursor.fetchall()]
                free_places = self.outer.MAX_PARTICIPANTS - registered_count
                days_text = "2 дня" if message == "LOW_FILL_2_DAYS" else "1 день"
                participants_text = "\n".join(participants) if participants else "Пока нет записавшихся"
                text = (
                    f"Напоминание: до мероприятия {days_text}.\n"
                    f"Свободных мест: {free_places}\n"
                    "Участники:\n"
                    f"{participants_text}"
                )
            if "1 минуту" in message:
                self.outer.db.cursor.execute('''
                    SELECT u.full_name FROM golden_stats g
                    JOIN users u ON g.user_id = u.user_id
                    WHERE g.event_id = ?''', (event_id,))
                golden_users = [row[0] for row in self.outer.db.cursor.fetchall()]
                text = f"{message}\n" + "\n".join(golden_users)
            elif message not in ("LOW_FILL_2_DAYS", "LOW_FILL_1_DAY"):
                text = message

            if "1 час" in message:
                await self._send_hour_personal_notifications(event_id)
            
            await self.outer.bot.send_message(self.outer.CHANNEL_ID, text)

    # Регистрации, отмены, статистика и пользовательские настройки уведомлений.
    class RegistrationManager:
        def __init__(self, outer):
            self.outer = outer

        # Точка входа регистрации: проверки доступа/лимитов и итоговое сообщение пользователю.
        async def register_user(self, user_id: int, username: str, full_name: str) -> Tuple[bool, str, bool]:
            event = self._get_current_event()
            if not event:
                return False, self._build_closed_message(None), False
	
            count = self._get_registration_count(event[0])
            if count >= self.outer.CLOSE_AT:
                return False, self._build_closed_message(event), False

            if self._is_already_registered(user_id, event[0]):
                return False, "Вы уже зарегистрированы", False

            is_golden = await self._process_registration(user_id, username, full_name, event, count)
            if is_golden is None:
                return False, "Ошибка регистрации", False
            return True, self._build_success_message(is_golden, count), is_golden

        # Возвращает текущее активное событие.
        def _get_current_event(self):
            self.outer.db.cursor.execute('SELECT * FROM events WHERE is_active = 1')
            return self.outer.db.cursor.fetchone()

        # Считает количество регистраций для конкретного события.
        def _get_registration_count(self, event_id: int) -> int:
            self.outer.db.cursor.execute(
                'SELECT COUNT(*) FROM registrations WHERE event_id = ?',
                (event_id,)
            )
            return self.outer.db.cursor.fetchone()[0]

        # Проверяет, есть ли у пользователя запись на это событие.
        def _is_already_registered(self, user_id: int, event_id: int) -> bool:
            self.outer.db.cursor.execute(
                'SELECT 1 FROM registrations WHERE user_id = ? AND event_id = ?',
                (user_id, event_id)
            )
            return bool(self.outer.db.cursor.fetchone())

        # Выполняет запись в БД и определяет золотой статус по текущим правилам.
        async def _process_registration(self, user_id: int, username: str, full_name: str, event: tuple, count: int) -> Optional[bool]:
            try:
                event_date = datetime.strptime(event[1].split('.')[0], DATE_FORMAT)
            except ValueError as e:
                logger.error(f"Error parsing event date: {e}")
                return None

            # Золотой статус выдаётся по заполненности обычных мест или по дедлайну в часах.
            current_time = datetime.now()
            golden_by_capacity = count >= self.outer.MAX_PARTICIPANTS
            golden_by_time = current_time >= (event_date - timedelta(hours=self.outer.CANCEL_BEFORE_HOURS))
            is_golden = golden_by_capacity or golden_by_time

            try:
                if is_golden:
                    self.outer.db.cursor.execute(
                        '''INSERT INTO golden_stats (user_id, event_id, event_date, assigned_at)
                        VALUES (?, ?, ?, CURRENT_TIMESTAMP)''',
                        (user_id, event[0], event[1])
                    )

                self.outer.db.cursor.execute(
                    'INSERT OR IGNORE INTO users (user_id, username, full_name) VALUES (?, ?, ?)',
                    (user_id, username, full_name)
                )
                self.outer.db.cursor.execute(
                    'INSERT INTO golden_stats (user_id, event_id) VALUES (?, ?)',
                    (user_id, event[0])
                )
                self.outer.db.conn.commit()
            except sqlite3.DatabaseError as e:
                logger.error(f"Database error during registration: {e}")
                return None

            return is_golden

        # Формирует человекочитаемое подтверждение успешной регистрации.
        def _build_success_message(self, is_golden: bool, count: int) -> str:
            message = "Регистрация успешна!"
            if is_golden:
                message += " 🌟 Золотая регистрация!"
            if count + 1 >= self.outer.MAX_PARTICIPANTS:
                message += "\nВнимание: достигнут лимит участников!"
            return message

        # Формирует ответ, когда регистрация закрыта, с подсказкой по открытию.
        def _build_closed_message(self, event: Optional[tuple]) -> str:
            open_cmd = "/setdate ДД.ММ.ГГГГ ЧЧ:ММ ИНТЕРВАЛ"
            if not event:
                return (
                    "Регистрация закрыта.\n"
                    "Когда откроется: после создания нового мероприятия.\n"
                    f"Команда открытия: {open_cmd}"
                )

            try:
                event_date = datetime.strptime(event[1].split('.')[0], DATE_FORMAT)
                open_time = event_date.strftime("%d.%m.%Y %H:%M")
            except (ValueError, IndexError, TypeError):
                open_time = "время не определено"

            return (
                "Регистрация закрыта.\n"
                f"Когда откроется: {open_time} (автоматически для следующего мероприятия).\n"
                f"Команда открытия: {open_cmd}"
            )

        # Сводная статистика: посещения и количество золотых регистраций по пользователям.
        async def get_stats(self) -> List[Dict]:
            self.outer.db.cursor.execute('''
                SELECT u.full_name, COUNT(r.id), COUNT(g.id), u.user_id
                FROM users u
                LEFT JOIN registrations r ON u.user_id = r.user_id
                LEFT JOIN golden_stats g ON u.user_id = g.user_id
                GROUP BY u.user_id
                ORDER BY COUNT(r.id) DESC
            ''')
            return [{
                'name': row[0],
                'visits': row[1],
                'golden': row[2],
                'user_id': row[3]
            } for row in self.outer.db.cursor.fetchall()]

        # Данные о ближайшем событии и списке участников для блока статистики.
        async def get_next_event_info(self) -> Dict:
            event = self._get_current_event()
            if not event:
                return {"event_time": None, "participants": []}

            try:
                event_time = datetime.strptime(event[1].split('.')[0], DATE_FORMAT).strftime("%d.%m.%Y %H:%M")
            except (ValueError, IndexError, TypeError):
                event_time = str(event[1])

            self.outer.db.cursor.execute('''
                SELECT u.full_name
                FROM registrations r
                JOIN users u ON r.user_id = u.user_id
                WHERE r.event_id = ?
                ORDER BY r.reg_time ASC
            ''', (event[0],))
            participants = [row[0] for row in self.outer.db.cursor.fetchall()]

            return {"event_time": event_time, "participants": participants}

        # Отмена записи с проверкой дедлайна и возвратом деталей для канал-уведомления.
        async def unregister_user(self, user_id: int) -> Tuple[bool, str, Optional[Dict]]:
            event = self._get_current_event()
            if not event:
                return False, "Нет активного мероприятия для отмены.", None

            try:
                event_date = datetime.strptime(event[1].split('.')[0], DATE_FORMAT)
            except (ValueError, IndexError, TypeError):
                return False, "Не удалось определить время мероприятия.", None

            cancel_deadline = event_date - timedelta(hours=self.outer.CANCEL_BEFORE_HOURS)
            if datetime.now() >= cancel_deadline:
                return (
                    False,
                    f"Отмена закрыта. Доступно до {cancel_deadline.strftime('%d.%m.%Y %H:%M')}",
                    None
                )

            if not self._is_already_registered(user_id, event[0]):
                return False, "Вы не зарегистрированы на текущее мероприятие.", None

            try:
                self.outer.db.cursor.execute(
                    'DELETE FROM registrations WHERE user_id = ? AND event_id = ?',
                    (user_id, event[0])
                )
                self.outer.db.cursor.execute(
                    'DELETE FROM golden_stats WHERE user_id = ? AND event_id = ?',
                    (user_id, event[0])
                )
                self.outer.db.conn.commit()
            except sqlite3.DatabaseError as e:
                logger.error(f"Database error during unregister: {e}")
                return False, "Ошибка отмены регистрации.", None

            count = self._get_registration_count(event[0])
            free_places = max(0, self.outer.MAX_PARTICIPANTS - count)
            details = {
                "event_time": event_date.strftime("%d.%m.%Y %H:%M"),
                "cancel_time": datetime.now().strftime("%d.%m.%Y %H:%M"),
                "free_places": free_places,
            }
            return True, "Регистрация отменена.", details

        # Переключает персональный флаг уведомлений и возвращает новое состояние.
        async def toggle_notifications(self, user_id: int) -> bool:
            self.outer.db.cursor.execute('''
                INSERT OR REPLACE INTO notification_settings (user_id, notify_enabled)
                VALUES (?, NOT COALESCE(
                    (SELECT notify_enabled FROM notification_settings WHERE user_id = ?),
                    1
                ))''', (user_id, user_id))
            self.outer.db.conn.commit()
            self.outer.db.cursor.execute(
                'SELECT notify_enabled FROM notification_settings WHERE user_id = ?',
                (user_id,)
            )
            return self.outer.db.cursor.fetchone()[0] == 1

    # Регистрирует команды и обработчик текстовых сообщений.
    def _register_handlers(self):
        self.application.add_handler(CommandHandler("start", self._handle_start))
        self.application.add_handler(CommandHandler("rules", self._handle_rules))
        self.application.add_handler(CommandHandler("setdate", self._handle_set_date))
        self.application.add_handler(CommandHandler("cancel", self._handle_cancel))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._handle_message))

    # Приветствие и подсказки по базовым действиям.
    async def _handle_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger.info("Received /start from user_id=%s chat_id=%s", update.effective_user.id, update.effective_chat.id)
        await update.message.reply_text(
            "Добро пожаловать в банный клуб!\n"
            "Используйте + или кнопку ✨ Записаться для записи.\n"
            "Используйте - или кнопку ➖ Отменить запись для отмены.",
            reply_markup=self.KEYBOARD
        )

    # Выводит свод правил пользования банным чатом.
    async def _handle_rules(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        rules_text = (
            "‼️5 простых правила настоящих банщиков:\n"
            "1. Каждый вторник-среду накануне бани опрос. Все желающие ставят +. Подсчет заканчивается в ЧЕТВЕРГ вечером.\n"
            "2. Поставил + и не пришел - скидывешься со всеми\n"
            "3. Не поставил + во время и пришел - скинулся на баню по стандарту и +500р в общак.\n"
            "Исключения - праздники, которые можем отменять заранее. Общак - резерв для оплаты бани.\n"
            "4. Поставил + и не можешь пойти - ищи замену, тот кто готов заменить идет по стандарту, а тебе не нужно скидываться. Ростовщичество не поощряется :)\n"
            "5. ⁠Количество мест - максимум 8 человек"
        )
        await update.message.reply_text(rules_text, reply_markup=self.KEYBOARD)

    # Админ/whitelist-команда создания следующего события.
    async def _handle_set_date(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._can_set_date(update):
            return

        try:
            date_str, time_str, interval = context.args
            event_date = datetime.strptime(
                f"{date_str} {time_str}:00", "%d.%m.%Y %H:%M:%S"
            )
            event_manager = self.EventManager(self)
            await event_manager.create_event(event_date, int(interval))
            await update.message.reply_text(
                f"✅ Мероприятие на {event_date}\nРегистрация до {event_date - timedelta(days=1)}",
                reply_markup=self.KEYBOARD
            )
            await self.bot.send_message(
                self.CHANNEL_ID,
                f"Новое мероприятие {event_date.strftime('%d.%m %H:%M')}!\nРегистрация открыта!"
            )
        except Exception as e:
            logger.error(f"Ошибка установки даты: {e}")
            await update.message.reply_text(
                "❌ Формат: /setdate ДД.ММ.ГГГГ ЧЧ:ММ ИНТЕРВАЛ",
                reply_markup=self.KEYBOARD
            )

    # Полная отмена текущего активного события и очистка scheduler jobs.
    async def _handle_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if await self._check_admin(update):
            self.db.cursor.execute('UPDATE events SET is_active = 0')
            self.db.conn.commit()
            self.scheduler.remove_all_jobs()
            await update.message.reply_text("Мероприятие отменено", reply_markup=self.KEYBOARD)
            await self.bot.send_message(self.CHANNEL_ID, "❌ Мероприятие отменено")

    # Роутер текстовых команд с клавиатуры (+, -, статистика, уведомления, правила).
    async def _handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Игнорируем нетекстовые и сервисные update, чтобы не падать на None.
        if not update.message or not update.message.text:
            return
        text = update.message.text.strip()
        logger.info("Received message '%s' from user_id=%s chat_id=%s", text, update.effective_user.id, update.effective_chat.id)
        if text in ("+", "✨ Записаться"):
            await self._handle_register(update)
        elif text in ("-", "➖ Отменить запись"):
            await self._handle_unregister(update)
        elif text == "📊 Статистика":
            await self._handle_stats(update)
        elif text == "🔔 Управление уведомлениями":
            await self._handle_notifications(update)
        elif text in ("Правила", "📜 Правила"):
            await self._handle_rules(update, context)

    # Пользовательская регистрация и уведомление в канал о новом участнике.
    async def _handle_register(self, update: Update):
        user = update.effective_user
        reg_manager = self.RegistrationManager(self)
        success, message, is_golden = await reg_manager.register_user(
            user.id, user.username, user.full_name
        )

        if success:
            await self.bot.send_message(
                self.CHANNEL_ID,
                f"🎉 {user.full_name} зарегистрирован(а)! {'🌟' if is_golden else ''}"
            )

        await update.message.reply_text(
            f"✅ {message}" if success else f"❌ {message}",
            reply_markup=self.KEYBOARD
        )

    # Пользовательская отмена и отправка деталей отмены в канал.
    async def _handle_unregister(self, update: Update):
        user = update.effective_user
        reg_manager = self.RegistrationManager(self)
        success, message, details = await reg_manager.unregister_user(user.id)

        if success:
            event_time = details["event_time"] if details else "неизвестно"
            cancel_time = details["cancel_time"] if details else datetime.now().strftime("%d.%m.%Y %H:%M")
            free_places = details["free_places"] if details else "неизвестно"
            await self.bot.send_message(
                self.CHANNEL_ID,
                (
                    f"➖ {user.full_name} отменил(а) регистрацию.\n"
                    f"Когда отменил(а): {cancel_time}\n"
                    f"Мероприятие: {event_time}\n"
                    f"Свободных мест: {free_places}"
                )
            )

        await update.message.reply_text(
            f"✅ {message}" if success else f"❌ {message}",
            reply_markup=self.KEYBOARD
        )

    # Расширенный отчёт: персональная статистика + ближайшее событие и участники.
    async def _handle_stats(self, update: Update):
        reg_manager = self.RegistrationManager(self)
        stats_data = await reg_manager.get_stats()
        response = [
            f"{stat['name']}: посещений - {stat['visits']}, золотых - {stat['golden']}"
            for stat in stats_data
        ]
        event_info = await reg_manager.get_next_event_info()
        if event_info["event_time"] is None:
            next_event_block = "\n\nСледующее событие: не назначено"
        else:
            participants = event_info["participants"]
            participants_block = "\n".join(participants) if participants else "Пока нет записавшихся"
            participants_count = len(participants)
            next_event_block = (
                f"\n\nСледующее событие: {event_info['event_time']}\n"
                f"Количество записанных: {participants_count}/{self.MAX_PARTICIPANTS}\n"
                "Записаны:\n"
                f"{participants_block}"
            )
        await update.message.reply_text(
            "📊 Статистика:\n" + ("\n".join(response) if response else "Нет данных") + next_event_block,
            reply_markup=self.KEYBOARD
        )

    # Переключение персональных ЛС-уведомлений.
    async def _handle_notifications(self, update: Update):
        user = update.effective_user
        reg_manager = self.RegistrationManager(self)
        new_state = await reg_manager.toggle_notifications(user.id)
        state_text = "включены" if new_state else "выключены"
        await update.message.reply_text(
            f"🔔 Уведомления {state_text}!",
            reply_markup=self.KEYBOARD
        )

    # Явная проверка админ-прав с ответом пользователю при отказе.
    async def _check_admin(self, update: Update) -> bool:
        user = await self.bot.get_chat_member(
            update.effective_chat.id,
            update.effective_user.id
        )
        if user.status not in [ChatMember.ADMINISTRATOR, ChatMember.OWNER]:
            await update.message.reply_text("❌ Только для администраторов", reply_markup=self.KEYBOARD)
            return False
        return True

    # Доступ к /setdate: админ или пользователь из whitelist.
    async def _can_set_date(self, update: Update) -> bool:
        if await self._check_admin_silent(update):
            return True
        if update.effective_user.id in self.SETDATE_ALLOWED_USERS:
            return True
        await update.message.reply_text(
            "❌ Нет доступа к /setdate (нужен админ или пользователь из списка SETDATE_ALLOWED_USERS).",
            reply_markup=self.KEYBOARD
        )
        return False

    # Тихая проверка админ-прав без пользовательского сообщения.
    async def _check_admin_silent(self, update: Update) -> bool:
        user = await self.bot.get_chat_member(
            update.effective_chat.id,
            update.effective_user.id
        )
        return user.status in [ChatMember.ADMINISTRATOR, ChatMember.OWNER]

    # Запускает scheduler и long-polling Telegram API.
    async def run(self):
        self.scheduler.start()
        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling()
        logger.info("Polling started")

        while True:
            await asyncio.sleep(3600)

    # Корректно останавливает polling, scheduler и соединение с БД.
    async def shutdown(self):
        try:
            await self.application.updater.stop()
        except RuntimeError:
            pass
        try:
            await self.application.stop()
        except RuntimeError:
            pass
        try:
            await self.application.shutdown()
        except RuntimeError:
            pass
        try:
            self.scheduler.shutdown()
        except Exception:
            pass
        try:
            self.db.close()
        except Exception:
            pass

# Основной цикл: автоперезапуск при непредвиденных ошибках.
async def main():
    while True:
        bot = BotApp()
        try:
            await bot.run()
            return
        except (KeyboardInterrupt, SystemExit):
            await bot.shutdown()
            return
        except Exception as e:
            if isinstance(e, InvalidToken):
                logger.error("Критическая ошибка: неверный TELEGRAM_TOKEN")
            else:
                logger.error(f"Критическая ошибка: {e}", exc_info=True)
            await bot.shutdown()
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(main())

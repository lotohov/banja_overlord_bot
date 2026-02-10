import os
import logging
import asyncio
import sqlite3
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
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

class Database:
    def __init__(self):
        db_path = os.getenv("DATABASE_PATH", "banja.db")
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self._init_tables()
    
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
                FOREIGN KEY(user_id) REFERENCES users(user_id),
                FOREIGN KEY(event_id) REFERENCES events(id))''',
            '''CREATE TABLE IF NOT EXISTS notification_settings (
                user_id INTEGER PRIMARY KEY,
                notify_enabled BOOLEAN DEFAULT 1,
                FOREIGN KEY(user_id) REFERENCES users(user_id))'''
        ]
        for table in tables:
            self.cursor.execute(table)
        self.conn.commit()
    
    def close(self):
        self.conn.close()

class BotApp:
    RUS_DAYS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    KEYBOARD = ReplyKeyboardMarkup(
        [[KeyboardButton("✨ Записаться"), KeyboardButton("📊 Статистика")],
         [KeyboardButton("🔔 Управление уведомлениями")]],
        resize_keyboard=True
    )

    def __init__(self):
        self.db = Database()
        self.scheduler = AsyncIOScheduler()
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
        self._register_handlers()
        logger.info("Bot initialized and handlers registered")

    class EventManager:
        def __init__(self, outer):
            self.outer = outer

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

        async def _schedule_notifications(self, event_date: datetime, event_id: int, interval: int):
            notifications = [
                (event_date - timedelta(days=3), "Напоминание: Мероприятие через 3 дня!"),
                (event_date - timedelta(days=1), "Напоминание: Мероприятие завтра!"),
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

        async def _create_next_event(self, event_id: int, interval: int):
            self.outer.db.cursor.execute('SELECT next_date FROM events WHERE id = ?', (event_id,))
            result = self.outer.db.cursor.fetchone()
            if result:
                next_date = datetime.strptime(result[0].split('.')[0], "%Y-%m-%d %H:%M:%S")
                new_date = next_date + timedelta(days=interval)
                await self.create_event(new_date, interval)

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

        async def send_notification(self, message: str, event_id: int):
            if "1 минуту" in message:
                self.outer.db.cursor.execute('''
                    SELECT u.full_name FROM golden_stats g
                    JOIN users u ON g.user_id = u.user_id
                    WHERE g.event_id = ?''', (event_id,))
                golden_users = [row[0] for row in self.outer.db.cursor.fetchall()]
                text = f"{message}\n" + "\n".join(golden_users)
            else:
                text = message
            
            await self.outer.bot.send_message(self.outer.CHANNEL_ID, text)

    class RegistrationManager:
        def __init__(self, outer):
            self.outer = outer

        async def register_user(self, user_id: int, username: str, full_name: str) -> Tuple[bool, str, bool]:
            event = self._get_current_event()
            if not event:
                return False, "Регистрация закрыта", False

            count = self._get_registration_count(event[0])
            if count >= self.outer.CLOSE_AT:
                return False, "Регистрация закрыта", False

            if self._is_already_registered(user_id, event[0]):
                return False, "Вы уже зарегистрированы", False

            is_golden = await self._process_registration(user_id, username, full_name, event, count)
            if is_golden is None:
                return False, "Ошибка регистрации", False
            return True, self._build_success_message(is_golden, count), is_golden

        def _get_current_event(self):
            self.outer.db.cursor.execute('SELECT * FROM events WHERE is_active = 1')
            return self.outer.db.cursor.fetchone()

        def _get_registration_count(self, event_id: int) -> int:
            self.outer.db.cursor.execute(
                'SELECT COUNT(*) FROM registrations WHERE event_id = ?',
                (event_id,)
            )
            return self.outer.db.cursor.fetchone()[0]

        def _is_already_registered(self, user_id: int, event_id: int) -> bool:
            self.outer.db.cursor.execute(
                'SELECT 1 FROM registrations WHERE user_id = ? AND event_id = ?',
                (user_id, event_id)
            )
            return bool(self.outer.db.cursor.fetchone())

        async def _process_registration(self, user_id: int, username: str, full_name: str, event: tuple, count: int) -> Optional[bool]:
            try:
                event_date = datetime.strptime(event[1].split('.')[0], DATE_FORMAT)
            except ValueError as e:
                logger.error(f"Error parsing event date: {e}")
                return None

            current_time = datetime.now()
            is_golden = count >= self.outer.MAX_PARTICIPANTS or current_time >= (event_date - timedelta(days=3))

            try:
                if is_golden:
                    self.outer.db.cursor.execute(
                        'INSERT INTO golden_stats (user_id, event_id) VALUES (?, ?)',
                        (user_id, event[0])
                    )

                self.outer.db.cursor.execute(
                    'INSERT OR IGNORE INTO users (user_id, username, full_name) VALUES (?, ?, ?)',
                    (user_id, username, full_name)
                )
                self.outer.db.cursor.execute(
                    '''INSERT INTO registrations (user_id, event_id, reg_time)
                    VALUES (?, ?, CURRENT_TIMESTAMP)''',
                    (user_id, event[0])
                )
                self.outer.db.conn.commit()
            except sqlite3.DatabaseError as e:
                logger.error(f"Database error during registration: {e}")
                return None

            return is_golden

        def _build_success_message(self, is_golden: bool, count: int) -> str:
            message = "Регистрация успешна!"
            if is_golden:
                message += " 🌟 Золотая регистрация!"
            if count + 1 >= self.outer.MAX_PARTICIPANTS:
                message += "\nВнимание: достигнут лимит участников!"
            return message

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

    def _register_handlers(self):
        self.application.add_handler(CommandHandler("start", self._handle_start))
        self.application.add_handler(CommandHandler("setdate", self._handle_set_date))
        self.application.add_handler(CommandHandler("cancel", self._handle_cancel))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._handle_message))

    async def _handle_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger.info("Received /start from user_id=%s chat_id=%s", update.effective_user.id, update.effective_chat.id)
        await update.message.reply_text(
            "Добро пожаловать в банный клуб!\nИспользуйте + или кнопку ✨ Записаться",
            reply_markup=self.KEYBOARD
        )

    async def _handle_set_date(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._check_admin(update):
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

    async def _handle_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if await self._check_admin(update):
            self.db.cursor.execute('UPDATE events SET is_active = 0')
            self.db.conn.commit()
            self.scheduler.remove_all_jobs()
            await update.message.reply_text("Мероприятие отменено", reply_markup=self.KEYBOARD)
            await self.bot.send_message(self.CHANNEL_ID, "❌ Мероприятие отменено")

    async def _handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = update.message.text.strip()
        logger.info("Received message '%s' from user_id=%s chat_id=%s", text, update.effective_user.id, update.effective_chat.id)
        if text in ("+", "✨ Записаться"):
            await self._handle_register(update)
        elif text == "📊 Статистика":
            await self._handle_stats(update)
        elif text == "🔔 Управление уведомлениями":
            await self._handle_notifications(update)

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

    async def _handle_stats(self, update: Update):
        reg_manager = self.RegistrationManager(self)
        stats_data = await reg_manager.get_stats()
        response = [
            f"{stat['name']}: посещений - {stat['visits']}, золотых - {stat['golden']}"
            for stat in stats_data
        ]
        await update.message.reply_text(
            "📊 Статистика:\n" + "\n".join(response),
            reply_markup=self.KEYBOARD
        )

    async def _handle_notifications(self, update: Update):
        user = update.effective_user
        reg_manager = self.RegistrationManager(self)
        new_state = await reg_manager.toggle_notifications(user.id)
        state_text = "включены" if new_state else "выключены"
        await update.message.reply_text(
            f"🔔 Уведомления {state_text}!",
            reply_markup=self.KEYBOARD
        )

    async def _check_admin(self, update: Update) -> bool:
        user = await self.bot.get_chat_member(
            update.effective_chat.id,
            update.effective_user.id
        )
        if user.status not in [ChatMember.ADMINISTRATOR, ChatMember.OWNER]:
            await update.message.reply_text("❌ Только для администраторов", reply_markup=self.KEYBOARD)
            return False
        return True

    async def run(self):
        self.scheduler.start()
        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling()
        logger.info("Polling started")

        while True:
            await asyncio.sleep(3600)

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

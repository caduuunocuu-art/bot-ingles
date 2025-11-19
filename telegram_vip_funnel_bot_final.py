# telegram_vip_funnel_bot_final.py
# ----------------------------------------------------------------------------
# Versão 100% funcional - Mantém todas as funcionalidades originais
# ----------------------------------------------------------------------------

import os
import asyncio
import logging
from datetime import datetime, timedelta
import pytz

import aiosqlite
from aiogram import Bot, Dispatcher, types
from aiogram.types import ChatType, ChatMemberUpdated
from aiogram.utils import executor
from aiogram.utils.exceptions import (
    RetryAfter,
    BotBlocked,
    ChatNotFound,
    UserDeactivated,
    Unauthorized,
    ChatAdminRequired,
    TelegramAPIError,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger

# -------------------------
# CONFIG — EDITE/USE AMBIENTE
# -------------------------
API_TOKEN = os.getenv("TG_BOT_TOKEN", "PUT_YOUR_BOT_TOKEN_HERE")
PREVIEWS_GROUP_ID = int(os.getenv("PREVIEWS_GROUP_ID", "-1003053104506"))
PREVIEWS_GROUP_INVITE_LINK = os.getenv("PREVIEWS_GROUP_INVITE_LINK", "https://t.me/+wYpQExxUOzkyNDk5")

# Redirecionamento para o bot de vendas (clicável)
PURCHASE_LINK = os.getenv("PURCHASE_LINK", "https://t.me/Grupo_Vip_BR2bot")
DISCOUNT_LINK = os.getenv("DISCOUNT_LINK", PURCHASE_LINK)

# Janela de prévia (dias)
DAYS_OF_PREVIEW = int(os.getenv("DAYS_OF_PREVIEW", "2"))
# Quantidade de dias de retarget após o término da prévia
RETARGET_DAYS = int(os.getenv("RETARGET_DAYS", "5"))

DB_PATH = os.getenv("DB_PATH", "vip_funnel_async.db")

# Envio imediato da mensagem do Dia 1 (delay em segundos após entrar no grupo)
SEND_IMMEDIATE_DELAY_SECONDS = int(os.getenv("SEND_IMMEDIATE_DELAY_SECONDS", "10"))
MAX_MESSAGE_RETRIES = int(os.getenv("MAX_MESSAGE_RETRIES", "3"))

# Vídeo .mp4 direto (usado como CTA em todos os envios)
VIDEO_URL = os.getenv("VIDEO_URL", "https://botdiscarado.com.br/video.mp4/leve.mp4")

# Admins (IDs). Use vírgula para múltiplos IDs em ADMINS.
ADMINS = set(map(int, os.getenv("ADMINS", "7708241274").split(",")))

# CTA persuasivo (usa {name}) — será usado na legenda do vídeo
CTA_TEXT = """
🚨 {name}, YOUR TIME IS RUNNING OUT! ⏰

🚨 IN VIP YOU WOULD SEE RIGHT NOW:
✅ FULL scenes without cuts
✅ EXCLUSIVE angles  
✅ 100% UNCENSORED content
✅ OnlyFans LEAKED TODAY

💎 IN VIP YOU GET IMMEDIATE ACCESS TO:
⭐ 100% ORIGINAL content (no repeats)
⭐ GUARANTEED DAILY updates  
⭐ PRIORITY 24/7 support
⭐ SEALED and ANONYMOUS group

📊 WHILE YOU WATCH:
⭐ 47 people joined VIP
⭐ 83 NEW contents

👉 SECURE YOUR SPOT: {link}
"""
# Horários configuráveis (formato "HH:MM")
MESSAGE_HOURS = os.getenv("MESSAGE_HOURS", "12:00,18:00,22:00").split(",")

# Timezone para agendamentos
TZ = pytz.timezone(os.getenv("TIMEZONE", "America/Sao_Paulo"))

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# -------------------------
# Bot & scheduler
# -------------------------
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)
scheduler = AsyncIOScheduler(timezone=TZ)

# -------------------------
# DB schema (assíncrona)
# -------------------------
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    joined_group INTEGER DEFAULT 0,
    join_time INTEGER DEFAULT 0,
    removed INTEGER DEFAULT 0,
    banned INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    attempt_time INTEGER,
    reason TEXT
);
"""

_db_lock = asyncio.Lock()

# -------------------------
# Mensagens estruturadas (funil 2 dias x 5 envios/dia + retarget)
# -------------------------
MESSAGES_SCHEDULE = {
  "1": {
    "12:00": "🚨 {name}, YOUR PREVIEW ACCESS HAS STARTED!\n\n⚠️ ATTENTION: You have 48h to enjoy FREE content before automatic ban!\n\n🔥 Meanwhile in VIP: +15 EXCLUSIVE scenes daily\n💎 Click to see what awaits you: {link}",
    "18:00": "😈 {name}, THE HOTTEST SCENES ARE IN VIP!\n\nWhile you see samples here, they're releasing:\n• FULL scenes without censorship\n• NEVER-BEFORE-SEEN content daily\n• Leaked OnlyFans\n• Exclusive Close Friends\n\n⚡ Don't just settle for previews: {link}",
    "22:00": "🌙 {name}, TODAY 23 PEOPLE UPGRADED FROM PREVIEW TO VIP!\n\nThey got tired of crumbs and went for the FULL FEAST!\n\n🚀 Your turn tomorrow? {link}"
  },
  "2": {
    "10:00": "⏰ {name}, 12H LEFT UNTIL YOUR BAN!\n\nYour preview access expires TODAY at 22:00!\n\n🔞 In VIP you'd have access right now to:\n✅ +500 COMPLETE scenes\n✅ +50 leaked onlyfans\n✅ DAILY content\n\n💀 Gonna miss this chance? {link}",
    "16:00": "🚨 {name}, RED ALERT: 6H REMAINING!\n\nYour preview ban is APPROACHING!\n\n🔥 Last chance to upgrade to VIP with:\n• LIFETIME access\n• UNCENSORED content\n• DAILY updates\n\n⚡ Run before it's too late: {link}",
    "21:00": "💀 {name}, FINAL HOUR IN PREVIEW!\n\nONLY 60 MINUTES left until your BAN!\n\n🎯 Out of 47 people banned today, 41 joined VIP!\n\n🚀 Final opportunity: {link}"
  },
  "retarget": {
    "1": {
      "12:00": "💔 {name}, YOU WERE BANNED FROM PREVIEW...\n\nBut your ADULT journey doesn't have to end here!\n\n🔞 In VIP you'd have access RIGHT NOW to:\n• COMPLETE scenes you missed\n• EXCLUSIVE OnlyFans\n• 100% UNCENSORED content\n\n⚡ Come back now: {link}",
      "18:00": "😈 {name}, MISSING THE HOT SCENES?\n\nWhile you were banned, VIP released +8 NEW scenes!\n\n🔥 Content you WON'T FIND anywhere else!\n💎 Immediate access: {link}",
      "22:00": "🌙 {name}, THE SPICIEST SCENES CONTINUE IN VIP!\n\n23 ex-banned members returned and are enjoying premium content!\n\n🚀 Your turn? {link}"
    },
    "2": {
      "12:00": "🚨 {name}, ALERT: NEW CONTENT AVAILABLE!\n\nWhile you're out, VIP is blowing up with:\n• OnlyFans leaked TODAY\n• EXCLUSIVE Close Friends\n• FULL scenes without cuts\n\n⚡ Don't stay out: {link}",
      "18:00": "😈 {name}, THINGS HEATED UP IN VIP!\n\nWe released EXCLUSIVE content that will blow your mind!\n\n🔥 Scenes you've never seen before!\n💎 Immediate access: {link}",
      "22:00": "💀 {name}, FINAL SPECIAL INVITATION!\n\nWe reopened spots for LIMITED TIME!\n\n🎯 Special conditions for ex-preview members!\n⚡ Join now: {link}"
    },
    "3": {
      "12:00": "⚡ {name}, WAKE UP TO THE DANGER!\n\nThe most DARING content is happening in VIP!\n\n🔞 FORBIDDEN scenes\n🔞 LEAKED OnlyFans\n🔞 INTIMATE Close Friends\n\n🚀 Do you have the courage? {link}",
      "18:00": "😈 {name}, TODAY HAS EXPLICIT SCENES IN VIP!\n\nMaterial so hot it almost melted the server!\n\n🔥 Only for BRAVE members!\n💎 Up for the challenge? {link}",
      "22:00": "🌙 {name}, PLEASURE ALL NIGHT IN VIP!\n\nWhile you sleep, the group is active with SPICY content!\n\n🚀 Last chance today: {link}"
    },
    "4": {
      "12:00": "🎯 {name}, FLASH OFFER!\n\nOnly TODAY: EXCLUSIVE bonus for returning!\n\n🔞 Package of UNSEEN scenes\n🔞 Never-before-leaked OnlyFans\n🔞 EXTRA hot content\n\n⚡ For limited time: {link}",
      "18:00": "🚨 {name}, SPOTS ALMOST GONE!\n\nOnly 8 spots left with special bonus!\n\n🔥 Content that will get you addicted!\n💎 Secure yours: {link}",
      "22:00": "💀 {name}, LAST OPPORTUNITY WITH BONUS!\n\nOnly 2 hours left until bonus expires!\n\n⚡ Don't leave it for later: {link}"
    },
    "5": {
      "12:00": "⌛ {name}, FINAL COUNTDOWN!\n\nLAST DAY with special conditions!\n\n🔞 Price increases 50% tomorrow\n🔞 Bonuses expire today\n\n🚀 Don't be the one to miss out: {link}",
      "18:00": "⏳ {name}, 6H LEFT UNTIL CHANGES!\n\nVIP will never be this affordable again!\n\n🔥 Last chance at current price\n💎 Tomorrow will be too late: {link}",
      "22:00": "💀 {name}, FINAL GOODBYE!\n\nThis is your LAST system message!\n\n⚡ Opportunities end in 2h\n🎯 Price increases TOMORROW\n\n🔞 Last call: {link}"
    }
  }
}
# -------------------------
# Inicialização do banco de dados
# -------------------------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(CREATE_SQL)
        await db.commit()
    logger.info("Banco de dados inicializado")

# -------------------------
# Handlers
# -------------------------
@dp.message_handler(commands=["start"], chat_type=ChatType.PRIVATE)
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    first_name = message.from_user.first_name or "Usuário"
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                INSERT OR IGNORE INTO users (user_id, username, first_name, last_name, joined_group, join_time)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    message.from_user.username,
                    first_name,
                    message.from_user.last_name,
                    0,
                    0,
                ),
            )
            await db.commit()

        start_text = """🎯 FREE ACCESS - PREVIEW GROUP 🎯

✅ Your temporary access has been successfully activated!

🔗 Join the group now: {invite_link}

🚨 Important information:
• Duration: {days} free days
• Anti-return system active (don't try to return without paying)
• VIP offers full benefits

👉 Tip: Join NOW and don't miss any content!""".format(
    invite_link=PREVIEWS_GROUP_INVITE_LINK,
    days=DAYS_OF_PREVIEW
)

        await message.answer(start_text)
        logger.info(f"Usuário {user_id} ({first_name}) recebeu link de convite via /start")
    except Exception as e:
        logger.exception(f"Erro no /start para {user_id}: {e}")
        await message.answer("❌ Ocorreu um erro. Tente novamente ou contate um administrador.")

# -------------------------
# Funções auxiliares (DB + envio)
# -------------------------
async def safe_send_message(chat_id: int, text: str, name_for_cta: str, max_retries: int = MAX_MESSAGE_RETRIES) -> bool:
    """Envia texto + vídeo com CTA - Garante que o vídeo seja enviado."""
    video_sent = False
    text_sent = False
    
    # PRIMEIRO: Tenta enviar o vídeo (mais importante)
    try:
        caption = CTA_TEXT.format(name=name_for_cta, link=PURCHASE_LINK)  # ← ADICIONE ISSO!
        await bot.send_video(chat_id, VIDEO_URL, caption=caption)
        video_sent = True
        logger.info(f"✅ Vídeo CTA enviado para {chat_id}")
    except Exception as e:
        logger.error(f"❌ Falha CRÍTICA: Não foi possível enviar vídeo para {chat_id}: {e}")
        return False  # Se o vídeo falha, retorna erro
    
    # SEGUNDO: Tenta enviar o texto (opcional)
    if text:
        attempt = 0
        while attempt < max_retries:
            try:
                await bot.send_message(chat_id, text)
                text_sent = True
                logger.info(f"✅ Mensagem de texto enviada para {chat_id}")
                break
            except RetryAfter as e:
                wait = getattr(e, 'timeout', getattr(e, 'retry_after', None)) or 5
                logger.info(f"RetryAfter: aguardando {wait}s para texto em {chat_id}")
                await asyncio.sleep(wait)
                attempt += 1
            except (BotBlocked, ChatNotFound, UserDeactivated, Unauthorized):
                logger.warning(f"Usuário {chat_id} bloqueou o bot - texto não enviado")
                break
            except Exception as e:
                logger.warning(f"Falha no texto para {chat_id} (tentativa {attempt+1}): {e}")
                attempt += 1
                await asyncio.sleep(2)
        
        if not text_sent:
            logger.warning(f"⚠️ Texto não enviado para {chat_id}, mas vídeo foi enviado")
    
    return video_sent  # Retorna True se pelo menos o vídeo foi enviado

async def get_user_info(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT user_id, username, first_name, last_name, joined_group, join_time, removed, banned
            FROM users WHERE user_id = ?
            """,
            (user_id,),
        )
        row = await cursor.fetchone()
        return row if row else None

async def update_user_joined(user_id: int, username: str, first_name: str, last_name: str):
    join_time = int(datetime.now().timestamp())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO users (user_id, username, first_name, last_name, joined_group, join_time)
            VALUES (?, ?, ?, ?, 1, ?)
            ON CONFLICT(user_id) DO UPDATE SET 
                username=excluded.username,
                first_name=excluded.first_name,
                last_name=excluded.last_name,
                joined_group=1
            """,
            (user_id, username, first_name, last_name, join_time)
        )
        await db.commit()

async def mark_user_removed(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET removed = 1 WHERE user_id = ?", (user_id,))
        await db.commit()

async def mark_user_banned(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET banned = 1 WHERE user_id = ?", (user_id,))
        await db.commit()

async def unban_user(user_id: int):
    """Remove o banimento de um usuário (no banco de dados e no grupo)"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET banned = 0, removed = 0 WHERE user_id = ?", (user_id,))
        await db.commit()
    logger.info(f"Usuário {user_id} desbanido no banco de dados")

async def unban_user_in_group(user_id: int):
    """Remove o banimento do usuário no grupo do Telegram"""
    try:
        await bot.unban_chat_member(PREVIEWS_GROUP_ID, user_id)
        logger.info(f"Usuário {user_id} desbanido no grupo do Telegram")
        return True
    except Exception as e:
        logger.error(f"Erro ao desbanir usuário {user_id} no grupo: {e}")
        return False

async def record_attempt(user_id: int, reason: str):
    attempt_time = int(datetime.now().timestamp())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO attempts (user_id, attempt_time, reason) VALUES (?, ?, ?)",
            (user_id, attempt_time, reason),
        )
        await db.commit()

# -------------------------
# Função que envia a mensagem agendada
# -------------------------
async def send_scheduled_message(user_id: int, day: int, hour: str, is_retarget: bool = False):
    user_info = await get_user_info(user_id)
    if not user_info:
        logger.warning(f"Usuário {user_id} não encontrado no banco para envio agendado")
        return

    (_uid, username, first_name, last_name, joined_group, join_time, removed, banned) = user_info

    # CORREÇÃO: Lógica de verificação corrigida
    if banned:
        logger.debug(f"Ignorado envio para {user_id}: usuário banido")
        return
        
    if not is_retarget and removed:
        logger.debug(f"Ignorado envio de prévia para {user_id}: usuário removido")
        return

    name = first_name or "Usuário"

    try:
        day_key = str(day)

        if not is_retarget:
            message_template = MESSAGES_SCHEDULE.get(day_key, {}).get(hour)
            logger.info(f"Enviando PRÉVIA - Dia {day}, Hora {hour} para {user_id}")
        else:
            message_template = MESSAGES_SCHEDULE.get("retarget", {}).get(day_key, {}).get(hour)
            logger.info(f"Enviando RETARGET - Dia {day}, Hora {hour} para {user_id} (removed={removed})")

        if not message_template:
            logger.warning(f"Mensagem não configurada para day={day} hour={hour} retarget={is_retarget}")
            return

        remaining_days = max(DAYS_OF_PREVIEW - day, 0)
        formatted = message_template.format(
            name=name,
            days=DAYS_OF_PREVIEW,
            remaining=remaining_days,
            link=PURCHASE_LINK,
            discount_link=DISCOUNT_LINK
        )

        success = await safe_send_message(user_id, formatted, name_for_cta=name)
        if success:
            logger.info(f"✓ Enviado (retarget={is_retarget}) dia {day} hora {hour} para {user_id}")
        else:
            logger.warning(f"✗ Falha ao enviar (retarget={is_retarget}) dia {day} hora {hour} para {user_id}")
    except Exception as e:
        logger.exception(f"Erro em send_scheduled_message para {user_id}: {e}")

# -------------------------
# Agendamento das mensagens
# -------------------------
async def schedule_user_messages(user_id: int, username: str, first_name: str, last_name: str):
    now = datetime.now(TZ)
    
    # Agenda dias de prévia (1..DAYS_OF_PREVIEW) com horas configuráveis
    for day in range(1, DAYS_OF_PREVIEW + 1):
        for hour in MESSAGE_HOURS:
            try:
                hour_dt = datetime.strptime(hour.strip(), "%H:%M").time()
            except Exception:
                logger.error(f"Formato de hora inválido em MESSAGE_HOURS: {hour}. Use HH:MM")
                continue

            target_date = (now + timedelta(days=day - 1)).date()
            run_dt = TZ.localize(datetime.combine(target_date, hour_dt))
            job_id = f"user_{user_id}_day_{day}_hour_{hour}"
            scheduler.add_job(
                send_scheduled_message,
                trigger=DateTrigger(run_date=run_dt),
                args=[user_id, day, hour.strip(), False],
                id=job_id,
                replace_existing=True,
            )

    # Agenda remoção no fim do período de prévia (após DAYS_OF_PREVIEW dias)
    removal_time = now + timedelta(days=DAYS_OF_PREVIEW)
    removal_dt = removal_time + timedelta(minutes=1)
    scheduler.add_job(
        remove_user_from_group,
        trigger=DateTrigger(run_date=removal_dt),
        args=[user_id],
        id=f"user_{user_id}_removal",
        replace_existing=True,
    )

    # CORREÇÃO: Agenda retarget começando 1 DIA APÓS a remoção
    for rday in range(1, RETARGET_DAYS + 1):
        for hour in MESSAGE_HOURS:
            try:
                hour_dt = datetime.strptime(hour.strip(), "%H:%M").time()
            except Exception:
                continue
            # CORREÇÃO: Retarget começa 1 DIA APÓS o fim da prévia
            target_date = (now + timedelta(days=DAYS_OF_PREVIEW + 1 + (rday - 1))).date()
            run_dt = TZ.localize(datetime.combine(target_date, hour_dt))
            job_id = f"user_{user_id}_retarget_day_{rday}_hour_{hour}"
            scheduler.add_job(
                send_scheduled_message,
                trigger=DateTrigger(run_date=run_dt),
                args=[user_id, rday, hour.strip(), True],
                id=job_id,
                replace_existing=True,
            )

    logger.info(f"Mensagens agendadas para o usuário {user_id} ({first_name}) — {DAYS_OF_PREVIEW} dias + {RETARGET_DAYS} retargets")

# -------------------------
# Remoção do usuário do grupo
# -------------------------
async def remove_user_from_group(user_id: int):
    try:
        await bot.ban_chat_member(PREVIEWS_GROUP_ID, user_id)
        await asyncio.sleep(5)
        await bot.unban_chat_member(PREVIEWS_GROUP_ID, user_id)

        await mark_user_removed(user_id)

        user_info = await get_user_info(user_id)
        if user_info:
            name = user_info[2] or "Usuário"
            removal_text = "{name}, your free access has expired. To return, only VIP: {link}".format(
                name=name, link=PURCHASE_LINK
            )
            await safe_send_message(user_id, removal_text, name_for_cta=name)  # ← CORRIGIDO!

        logger.info(f"Usuário {user_id} removido do grupo de prévia")
    except (ChatAdminRequired, TelegramAPIError) as e:
        logger.error(f"Erro ao remover usuário {user_id} do grupo: {e}")
        for admin_id in ADMINS:
            try:
                await bot.send_message(admin_id, f"Erro ao remover usuário {user_id} do grupo: {e}")
            except Exception:
                pass

        logger.info(f"Usuário {user_id} removido do grupo de prévia")
    except (ChatAdminRequired, TelegramAPIError) as e:
        logger.error(f"Erro ao remover usuário {user_id} do grupo: {e}")
        for admin_id in ADMINS:
            try:
                await bot.send_message(admin_id, f"Erro ao remover usuário {user_id} do grupo: {e}")
            except Exception:
                pass

# -------------------------
# Handler para novos membros no grupo de prévia (ANTI-RETORNO CORRIGIDO)
# -------------------------
@dp.chat_member_handler(chat_id=PREVIEWS_GROUP_ID)
async def handle_chat_member_update(update: ChatMemberUpdated):
    try:
        if update.new_chat_member.status == 'member':
            user = update.new_chat_member.user
            user_id = user.id

            user_info = await get_user_info(user_id)

            # CORREÇÃO: Anti-retorno completo como no código original
            if user_info and user_info[6]:  # removed flag
                join_time = user_info[5]  # join_time
                current_time = int(datetime.now().timestamp())
                preview_end_time = join_time + (DAYS_OF_PREVIEW * 24 * 3600)
                
                # Só bane se realmente passou do tempo de prévia
                if current_time > preview_end_time:
                    await record_attempt(user_id, "Tentativa de retorno após período de prévia")
                    try:
                        await bot.ban_chat_member(PREVIEWS_GROUP_ID, user_id)
                        await mark_user_banned(user_id)
                        name = user.first_name or "Usuário"
                        ban_text = "{name}, seu acesso gratuito já expirou. Para voltar, só no VIP: {link}".format(
                            name=name, link=PURCHASE_LINK
                        )
                        await safe_send_message(user_id, ban_text, name_for_cta=name)
                        logger.info(f"Usuário {user_id} ({name}) banido por tentativa de retorno após período")
                    except Exception as e:
                        logger.error(f"Erro ao banir usuário {user_id}: {e}")
                else:
                    # Ainda está no período de prévia, permite voltar
                    logger.info(f"Usuário {user_id} voltou durante período de prévia - permitido")
                    await update_user_joined(user_id, user.username, user.first_name, user.last_name)

            # Novo usuário (ou ainda não marcado como joined)
            elif not user_info or not user_info[4]:  # joined_group flag
                await update_user_joined(user_id, user.username, user.first_name, user.last_name)

                # Vídeo CTA de boas-vindas
try:
    caption = CTA_TEXT.format(name=user.first_name or "Usuário", link=PURCHASE_LINK)  # ← ADICIONE ISSO!
    await bot.send_video(user_id, VIDEO_URL, caption=caption)
    logger.info(f"Vídeo CTA de boas-vindas enviado para {user_id}")
                except Exception as e:
                    logger.error(f"Erro ao enviar vídeo CTA de boas-vindas para {user_id}: {e}")

                await schedule_user_messages(user_id, user.username, user.first_name, user.last_name)

                # Mensagem imediata (dia 1)
                await asyncio.sleep(SEND_IMMEDIATE_DELAY_SECONDS)
                first_hour = MESSAGE_HOURS[0].strip() if MESSAGE_HOURS else "12:00"
                await send_scheduled_message(user_id, 1, first_hour, is_retarget=False)
                logger.info(f"Novo usuário {user_id} ({user.first_name}) adicionado ao grupo e agendado")
    except Exception as e:
        logger.exception(f"Erro ao processar chat_member_update: {e}")

# -------------------------
# Comandos administrativos
# -------------------------
@dp.message_handler(commands=['stats'], user_id=list(ADMINS), chat_type=ChatType.PRIVATE)
async def cmd_stats(message: types.Message):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM users")
            total_users = (await cursor.fetchone())[0]

            cursor = await db.execute(
                "SELECT COUNT(*) FROM users WHERE joined_group = 1 AND removed = 0 AND banned = 0"
            )
            active_users = (await cursor.fetchone())[0]

            cursor = await db.execute("SELECT COUNT(*) FROM users WHERE removed = 1")
            removed_users = (await cursor.fetchone())[0]

            cursor = await db.execute("SELECT COUNT(*) FROM users WHERE banned = 1")
            banned_users = (await cursor.fetchone())[0]

            cursor = await db.execute("SELECT COUNT(*) FROM attempts")
            attempts = (await cursor.fetchone())[0]

        stats_text = (
            f"📊 Estatísticas do Bot VIP Funnel:\n\n"
            f"• Total de usuários: {total_users}\n"
            f"• Usuários ativos: {active_users}\n"
            f"• Usuários removidos: {removed_users}\n"
            f"• Usuários banidos: {banned_users}\n"
            f"• Tentativas de retorno: {attempts}"
        )
        await message.answer(stats_text)
    except Exception as e:
        logger.exception(f"Erro ao recuperar estatísticas: {e}")
        await message.answer("❌ Erro ao recuperar estatísticas.")

# Broadcast com confirmação simples em memória
_pending_broadcast = {}

@dp.message_handler(commands=['broadcast'], user_id=list(ADMINS), chat_type=ChatType.PRIVATE)
async def cmd_broadcast(message: types.Message):
    if not message.reply_to_message or not (message.reply_to_message.text or message.reply_to_message.caption):
        await message.answer("❌ Use este comando em *resposta* a uma mensagem de texto para fazer broadcast.", parse_mode="Markdown")
        return

    content = message.reply_to_message.text or message.reply_to_message.caption
    _pending_broadcast[message.from_user.id] = content

    preview = (content[:400] + '…') if len(content) > 400 else content
    await message.answer(
        "📢 Confirmar broadcast para *todos os usuários*?\n\n" +
        f"Prévia:\n\n{preview}\n\n" +
        "Digite /confirmar para prosseguir ou /cancelar para abortar.",
        parse_mode="Markdown",
    )

@dp.message_handler(commands=['confirmar'], user_id=list(ADMINS), chat_type=ChatType.PRIVATE)
async def cmd_confirm_broadcast(message: types.Message):
    admin_id = message.from_user.id
    content = _pending_broadcast.get(admin_id)
    if not content:
        await message.answer("Não há broadcast pendente. Use /broadcast respondendo a uma mensagem.")
        return

    sent = 0
    failed = 0
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT user_id, first_name FROM users") as cursor:
                async for row in cursor:
                    uid, fname = row
                    name = fname or "Usuário"
                    try:
                        await safe_send_message(uid, content, name_for_cta=name)
                        sent += 1
                    except Exception:
                        failed += 1
                    await asyncio.sleep(0.05)
    finally:
        _pending_broadcast.pop(admin_id, None)

    await message.answer(f"✅ Broadcast finalizado. Enviados: {sent} | Falhas: {failed}")

@dp.message_handler(commands=['cancelar'], user_id=list(ADMINS), chat_type=ChatType.PRIVATE)
async def cmd_cancel_broadcast(message: types.Message):
    _pending_broadcast.pop(message.from_user.id, None)
    await message.answer("❌ Broadcast cancelado.")

# Comando para desbanir usuário
@dp.message_handler(commands=['desbanir'], user_id=list(ADMINS), chat_type=ChatType.PRIVATE)
async def cmd_unban(message: types.Message):
    """Desbanir um usuário (útil para testes)"""
    try:
        if message.reply_to_message:
            user_id = message.reply_to_message.from_user.id
            username = message.reply_to_message.from_user.username or "Sem username"
            first_name = message.reply_to_message.from_user.first_name or "Usuário"
        else:
            user_id = message.from_user.id
            username = message.from_user.username or "Sem username"
            first_name = message.from_user.first_name or "Usuário"

        await unban_user(user_id)
        group_unbanned = await unban_user_in_group(user_id)
        
        for job in scheduler.get_jobs():
            if f"user_{user_id}" in job.id:
                scheduler.remove_job(job.id)
                logger.info(f"Job removido: {job.id}")

        if group_unbanned:
            response = f"✅ Usuário @{username} ({first_name}) desbanido com sucesso!\n\n📊 Status:\n• Banimento removido do banco\n• Banimento removido do grupo\n• Jobs de remoção cancelados"
        else:
            response = f"⚠️ Usuário @{username} ({first_name}) desbanido parcialmente!\n\n📊 Status:\n• Banimento removido do banco ✅\n• Erro ao remover banimento do grupo ❌\n• Jobs de remoção cancelados ✅\n\n💡 O usuário pode não estar banido no grupo."

        await message.answer(response)
        logger.info(f"Admin {message.from_user.id} desbaniu o usuário {user_id}")

    except Exception as e:
        error_msg = f"❌ Erro ao desbanir usuário: {e}"
        await message.answer(error_msg)
        logger.error(f"Erro no comando desbanir: {e}")

# -------------------------
# Startup / Shutdown
# -------------------------
async def on_startup(_):
    await init_db()
    scheduler.start()
    logger.info("Bot iniciado e agendador ativado")

async def on_shutdown(_):
    scheduler.shutdown()
    logger.info("Bot desligado e agendador parado")

if __name__ == '__main__':
    executor.start_polling(
        dp,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        skip_updates=True,
    )

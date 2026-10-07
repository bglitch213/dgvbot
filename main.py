import os
import time
import asyncio
import random
import threading
from threading import Thread
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta

import discord
from discord import app_commands
from discord.ext import commands, tasks
from flask import Flask
import psycopg2
from psycopg2 import pool as pg_pool
from psycopg2.extras import execute_values

# ==========================================
# 0. 렌더(Render) 24시간 유지용 Flask 웹서버 (가장 먼저 실행)
# ==========================================
app = Flask(__name__)


@app.route('/')
def home():
    return "Bot is running!"


def run():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)


def keep_alive():
    t = Thread(target=run)
    t.daemon = True
    t.start()


keep_alive()

VOICE_REWARD_INTERVAL_MINUTES = 30
KST = timezone(timedelta(hours=9))
DATABASE_URL = os.getenv("DATABASE_URL")

# ==========================================
# 1. DB (커넥션 풀 + 스레드 실행)
#    - 매 명령마다 새로 접속하던 방식은 느려서 3초 응답 제한에 걸릴 수 있었습니다.
#    - 풀을 쓰고, DB 작업은 별도 스레드에서 실행해 봇이 멈추지 않게 합니다.
# ==========================================
POOL_MAX = 10
if not DATABASE_URL:
    raise RuntimeError("❌ 에러: DATABASE_URL 환경 변수가 설정되지 않았습니다!")

_pool = pg_pool.ThreadedConnectionPool(
    1, POOL_MAX, DATABASE_URL,
    sslmode='require',
    keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5,
    connect_timeout=10,
)
_pool_sem = threading.BoundedSemaphore(POOL_MAX)


@contextmanager
def db_cursor():
    """풀에서 커넥션을 빌려 커서를 제공. 정상 종료 시 commit, 예외 시 rollback."""
    _pool_sem.acquire()
    conn = None
    broken = False
    try:
        conn = _pool.getconn()
        cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            broken = True
            raise
        except Exception:
            try:
                conn.rollback()
            except Exception:
                broken = True
            raise
        finally:
            try:
                cur.close()
            except Exception:
                pass
    finally:
        if conn is not None:
            try:
                _pool.putconn(conn, close=(broken or bool(conn.closed)))
            except Exception:
                pass
        _pool_sem.release()


def _run(fn, *args):
    """동기 DB 함수 실행. 끊어진 연결이면 1회 재시도."""
    last = None
    for _ in range(2):
        try:
            with db_cursor() as cur:
                return fn(cur, *args)
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            last = e
    raise last


async def run_db(fn, *args):
    return await asyncio.to_thread(_run, fn, *args)


def init_db():
    def _init(cur):
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                guild_id BIGINT,
                user_id BIGINT,
                username TEXT DEFAULT NULL,
                coins INTEGER DEFAULT 0,
                voice_minutes INTEGER DEFAULT 0,
                referred_by BIGINT DEFAULT NULL,
                warnings INTEGER DEFAULT 0,
                defense_tickets INTEGER DEFAULT 0,
                PRIMARY KEY (guild_id, user_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS voice_sessions (
                guild_id BIGINT,
                user_id BIGINT,
                join_time DOUBLE PRECISION,
                counting_since DOUBLE PRECISION DEFAULT NULL,
                accumulated_seconds DOUBLE PRECISION DEFAULT 0,
                PRIMARY KEY (guild_id, user_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id BIGINT PRIMARY KEY,
                voice_reward_rate INTEGER DEFAULT 1,
                referral_reward INTEGER NOT NULL DEFAULT 30,
                log_channel_id BIGINT DEFAULT NULL,
                slot_rtp INTEGER DEFAULT 85
            )
        """)
        # 기존 테이블을 쓰는 경우에도 필요한 컬럼을 자동 보완
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT DEFAULT NULL")
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_by BIGINT DEFAULT NULL")
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS warnings INTEGER DEFAULT 0")
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS defense_tickets INTEGER DEFAULT 0")
        cur.execute("ALTER TABLE voice_sessions ADD COLUMN IF NOT EXISTS counting_since DOUBLE PRECISION DEFAULT NULL")
        cur.execute("ALTER TABLE voice_sessions ADD COLUMN IF NOT EXISTS accumulated_seconds DOUBLE PRECISION DEFAULT 0")
        cur.execute("ALTER TABLE guild_settings ADD COLUMN IF NOT EXISTS slot_rtp INTEGER DEFAULT 85")

    _run(_init)


init_db()


def ensure_user(cur, guild_id, user_id, username=None):
    cur.execute(
        """
        INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
        VALUES (%s, %s, %s, 0, 0, 0, 0)
        ON CONFLICT (guild_id, user_id) DO UPDATE
            SET username = COALESCE(EXCLUDED.username, users.username)
        """,
        (guild_id, user_id, username),
    )


# ==========================================
# 2. 음성 시간 / 코인 적립 로직 (이벤트·1분 루프 공용)
# ==========================================
def award_voice(cur, guild_id, user_id, username, minutes):
    if minutes <= 0:
        return
    cur.execute("SELECT voice_reward_rate FROM guild_settings WHERE guild_id=%s", (guild_id,))
    rr = cur.fetchone()
    rate = rr[0] if rr and rr[0] is not None else 1
    cur.execute("SELECT COALESCE(voice_minutes, 0) FROM users WHERE guild_id=%s AND user_id=%s", (guild_id, user_id))
    row = cur.fetchone()
    previous = row[0] if row else 0
    new_minutes = previous + minutes
    crossed = new_minutes // VOICE_REWARD_INTERVAL_MINUTES - previous // VOICE_REWARD_INTERVAL_MINUTES
    cur.execute(
        """
        UPDATE users
        SET voice_minutes = COALESCE(voice_minutes, 0) + %s,
            coins = COALESCE(coins, 0) + %s,
            username = COALESCE(%s, username)
        WHERE guild_id=%s AND user_id=%s
        """,
        (minutes, crossed * rate, username, guild_id, user_id),
    )


def process_voice(cur, guild_id, user_id, username, in_voice, muted, now, fresh_join=False):
    """
    음성 세션 상태를 갱신하고 쌓인 시간을 정산한다.
    - in_voice=False : 남은 시간을 정산하고 세션 삭제
    - fresh_join=True: 새로 입장 (기존 잔여 세션 초기화)
    - 그 외          : 지금까지 카운트된 시간을 정산하고 뮤트 상태에 맞춰 카운트 재개/중지
    """
    ensure_user(cur, guild_id, user_id, username)

    if fresh_join:
        cur.execute(
            """
            INSERT INTO voice_sessions (guild_id, user_id, join_time, counting_since, accumulated_seconds)
            VALUES (%s, %s, %s, %s, 0)
            ON CONFLICT (guild_id, user_id) DO UPDATE SET
                join_time = EXCLUDED.join_time,
                counting_since = EXCLUDED.counting_since,
                accumulated_seconds = 0
            """,
            (guild_id, user_id, now, None if muted else now),
        )
        return

    cur.execute(
        """
        SELECT counting_since, COALESCE(accumulated_seconds, 0)
        FROM voice_sessions WHERE guild_id=%s AND user_id=%s FOR UPDATE
        """,
        (guild_id, user_id),
    )
    row = cur.fetchone()

    if not in_voice:
        if row:
            counting_since, acc = row
            acc = float(acc)
            if counting_since is not None:
                acc += max(0, now - float(counting_since))
            award_voice(cur, guild_id, user_id, username, int(acc // 60))
            cur.execute("DELETE FROM voice_sessions WHERE guild_id=%s AND user_id=%s", (guild_id, user_id))
        return

    if row is None:
        cur.execute(
            """
            INSERT INTO voice_sessions (guild_id, user_id, join_time, counting_since, accumulated_seconds)
            VALUES (%s, %s, %s, %s, 0)
            ON CONFLICT (guild_id, user_id) DO NOTHING
            """,
            (guild_id, user_id, now, None if muted else now),
        )
        return

    counting_since, acc = row
    acc = float(acc)
    if counting_since is not None:
        acc += max(0, now - float(counting_since))
    minutes = int(acc // 60)
    acc -= minutes * 60
    award_voice(cur, guild_id, user_id, username, minutes)
    cur.execute(
        """
        UPDATE voice_sessions SET counting_since=%s, accumulated_seconds=%s
        WHERE guild_id=%s AND user_id=%s
        """,
        (None if muted else now, acc, guild_id, user_id),
    )


def fetch_sessions(cur):
    cur.execute("SELECT guild_id, user_id FROM voice_sessions")
    return cur.fetchall()


def voice_batch(items, now):
    for guild_id, user_id, username, in_voice, muted in items:
        try:
            _run(process_voice, guild_id, user_id, username, in_voice, muted, now)
        except Exception as e:
            print(f"[VOICE DEBUG] 사용자 처리 오류 ({guild_id}/{user_id}): {e}")


# ==========================================
# 3. 봇 설정
# ==========================================
intents = discord.Intents.default()
intents.guilds = True
intents.voice_states = True
intents.members = True  # 개발자 포털에서 SERVER MEMBERS INTENT 를 반드시 켜야 합니다.

bot = commands.Bot(command_prefix="!", intents=intents)
commands_synced = False


def voice_is_muted(voice_state) -> bool:
    return bool(voice_state and (voice_state.self_mute or voice_state.self_deaf))


def resync_voice_sessions_sync(entries, now):
    """봇 (재)시작 시 이미 음성 채널에 있는 유저의 세션을 만들어 줌."""
    for guild_id, user_id, username, muted in entries:
        try:
            def _f(cur):
                ensure_user(cur, guild_id, user_id, username)
                cur.execute(
                    """
                    INSERT INTO voice_sessions (guild_id, user_id, join_time, counting_since, accumulated_seconds)
                    VALUES (%s, %s, %s, %s, 0)
                    ON CONFLICT (guild_id, user_id) DO NOTHING
                    """,
                    (guild_id, user_id, now, None if muted else now),
                )
            _run(_f)
        except Exception as e:
            print(f"[VOICE DEBUG] 세션 복구 오류: {e}")


@bot.event
async def on_ready():
    global commands_synced
    print(f"로그인 완료: {bot.user}")

    for guild in bot.guilds:
        try:
            await guild.chunk(cache=True)
        except Exception:
            pass

    # 재연결로 on_ready 가 다시 불려도 매번 동기화해서 명령어 목록이 항상 최신이 되게 함
    # (서버별로 따로 try/except → 한 서버가 실패해도 다른 서버는 계속 진행)
    all_ok = True
    for guild in bot.guilds:
        try:
            bot.tree.copy_global_to(guild=guild)
            synced = await bot.tree.sync(guild=guild)
            names = ", ".join(c.name for c in synced)
            print(f"[COMMAND SYNC] {guild.name} ({guild.id}) -> {len(synced)}개: {names}")
        except Exception as e:
            all_ok = False
            print(f"[COMMAND SYNC] {guild.name} ({guild.id}) 동기화 오류: {e}")
    commands_synced = all_ok

    # 이미 음성 채널에 들어와 있는 유저 세션 복구
    try:
        entries = []
        for guild in bot.guilds:
            for vc in guild.voice_channels + list(guild.stage_channels):
                for m in vc.members:
                    if not m.bot:
                        entries.append((guild.id, m.id, m.name, voice_is_muted(m.voice)))
        if entries:
            await asyncio.to_thread(resync_voice_sessions_sync, entries, time.time())
    except Exception as e:
        print(f"[VOICE DEBUG] 세션 복구 실패: {e}")

    if not check_voice_time.is_running():
        check_voice_time.start()


@bot.event
async def on_guild_join(guild: discord.Guild):
    try:
        await guild.chunk(cache=True)
    except Exception:
        pass
    try:
        bot.tree.copy_global_to(guild=guild)
        synced = await bot.tree.sync(guild=guild)
        print(f"[COMMAND SYNC] 새 서버 {guild.name} ({guild.id}) -> {len(synced)}개")
    except Exception as e:
        print(f"[COMMAND SYNC] 새 서버 동기화 오류: {e}")


# 모든 슬래시 명령어의 공통 오류 처리 (응답 없음 방지)
@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        msg = "❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다."
    elif isinstance(error, app_commands.NoPrivateMessage):
        msg = "❌ 서버 안에서만 사용할 수 있는 명령어입니다."
    else:
        original = getattr(error, "original", error)
        print(f"[명령어 오류] {original!r}")
        msg = f"❌ 오류가 발생했습니다: {str(original)[:300]}"
    try:
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)
    except Exception:
        pass


# ==========================================
# 4. 백그라운드: 1분마다 음성 시간 정산
# ==========================================
@tasks.loop(minutes=1)
async def check_voice_time():
    try:
        sessions = await run_db(fetch_sessions)
        items = []
        for guild_id, user_id in sessions:
            guild = bot.get_guild(int(guild_id))
            if not guild:
                continue
            member = guild.get_member(int(user_id))
            voice = member.voice if member else None
            in_voice = bool(voice and voice.channel)
            muted = voice_is_muted(voice)
            username = member.name if member else None
            items.append((int(guild_id), int(user_id), username, in_voice, muted))
        if items:
            await asyncio.to_thread(voice_batch, items, time.time())
    except Exception as e:
        print(f"[VOICE DEBUG] 전체 음성 체크 오류: {e}")


@check_voice_time.before_loop
async def before_check_voice_time():
    await bot.wait_until_ready()


@bot.event
async def on_voice_state_update(member, before, after):
    if member.bot:
        return

    was_connected = before.channel is not None
    is_connected = after.channel is not None
    muted_after = voice_is_muted(after)

    try:
        await run_db(
            process_voice,
            member.guild.id, member.id, member.name,
            is_connected, muted_after, time.time(),
            (is_connected and not was_connected),
        )
    except Exception as e:
        print(f"[음성 상태 업데이트 오류] {e}")


# ==========================================
# 5. 🧹 채팅 청소 명령어
# ==========================================
@bot.tree.command(name="채팅청소", description="지정한 수량만큼 채팅을 삭제합니다. (선택적으로 특정 유저만 삭제 가능)")
@app_commands.describe(limit="삭제할 메시지 수 (1~100)", member="청소할 대상 유저 (선택하지 않으면 전체 최근 메시지)")
@app_commands.guild_only()
@app_commands.checks.has_permissions(administrator=True)
async def clear_user_chat(interaction: discord.Interaction, limit: int = 20, member: discord.Member = None):
    if limit < 1 or limit > 100:
        await interaction.response.send_message("⚠️ 삭제 수량은 1부터 100 사이로 입력해 주세요.", ephemeral=True)
        return

    channel = interaction.channel
    if channel is None or not hasattr(channel, "history") or not hasattr(channel, "delete_messages"):
        await interaction.response.send_message("⚠️ 이 채널에서는 청소할 수 없습니다.", ephemeral=True)
        return

    perms = channel.permissions_for(interaction.guild.me)
    if not (perms.manage_messages and perms.read_message_history):
        await interaction.response.send_message(
            "❌ 봇에게 이 채널의 **메시지 관리** 및 **메시지 기록 보기** 권한이 필요합니다.", ephemeral=True
        )
        return

    await interaction.response.defer(thinking=True, ephemeral=True)

    try:
        messages_to_delete = []
        if member:
            async for message in channel.history(limit=500):
                if message.author.id == member.id:
                    messages_to_delete.append(message)
                    if len(messages_to_delete) >= limit:
                        break
            target_name = f"**{member.display_name}**님의"
        else:
            async for message in channel.history(limit=limit):
                messages_to_delete.append(message)
            target_name = "최근"

        if not messages_to_delete:
            await interaction.followup.send("⚠️ 삭제할 대화 내역을 찾지 못했습니다.", ephemeral=True)
            return

        now = datetime.now(timezone.utc)
        recent_messages = []
        old_messages = []
        for m in messages_to_delete:
            if (now - m.created_at) < timedelta(days=13, hours=23):
                recent_messages.append(m)
            else:
                old_messages.append(m)

        deleted_count = 0
        for i in range(0, len(recent_messages), 100):
            chunk = recent_messages[i:i + 100]
            try:
                if len(chunk) > 1:
                    await channel.delete_messages(chunk)
                else:
                    await chunk[0].delete()
                deleted_count += len(chunk)
            except discord.HTTPException as e:
                print(f"[채팅청소 오류] {e}")

        for m in old_messages:
            try:
                await m.delete()
                deleted_count += 1
                await asyncio.sleep(0.5)
            except Exception:
                pass

        await interaction.followup.send(
            f"🧹 {target_name} 메시지 **{deleted_count}개**를 성공적으로 청소했습니다!",
            ephemeral=True,
        )
    except Exception as e:
        await interaction.followup.send(f"❌ 메시지 청소 중 오류가 발생했습니다: {e}", ephemeral=True)


# ==========================================
# 6. 🔄 닉네임 일괄 동기화
# ==========================================
def sync_usernames_db(cur, guild_id, rows):
    execute_values(
        cur,
        """
        INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
        VALUES %s
        ON CONFLICT (guild_id, user_id) DO UPDATE SET username = EXCLUDED.username
        """,
        [(guild_id, uid, name, 0, 0, 0, 0) for uid, name in rows],
        page_size=500,
    )


@bot.tree.command(name="닉네임동기화", description="서버 내 모든 멤버의 닉네임을 DB에 강제로 일괄 동기화합니다.")
@app_commands.guild_only()
@app_commands.checks.has_permissions(administrator=True)
async def force_sync_usernames(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True, ephemeral=True)
    guild = interaction.guild
    rows = [(m.id, m.name) for m in guild.members if not m.bot]
    if not rows:
        await interaction.followup.send("⚠️ 동기화할 멤버가 없습니다. (멤버 인텐트 설정을 확인하세요)", ephemeral=True)
        return
    try:
        await run_db(sync_usernames_db, guild.id, rows)
        await interaction.followup.send(f"✅ 성공적으로 서버 멤버 {len(rows)}명의 닉네임을 DB에 동기화했습니다!", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ 동기화 중 오류 발생: {e}", ephemeral=True)


# ==========================================
# 7. 🚨 관리자 명령어 (경고, 코인, 방어권)
# ==========================================
def add_warning_db(cur, guild_id, user_id, username):
    ensure_user(cur, guild_id, user_id, username)
    cur.execute(
        """
        UPDATE users SET defense_tickets = defense_tickets - 1
        WHERE guild_id=%s AND user_id=%s AND COALESCE(defense_tickets, 0) > 0
        RETURNING defense_tickets
        """,
        (guild_id, user_id),
    )
    row = cur.fetchone()
    if row is not None:
        return ("defense", int(row[0]))
    cur.execute(
        """
        UPDATE users SET warnings = COALESCE(warnings, 0) + 1, username = %s
        WHERE guild_id=%s AND user_id=%s
        RETURNING warnings
        """,
        (username, guild_id, user_id),
    )
    return ("warn", int(cur.fetchone()[0]))


@bot.tree.command(name="경고지급", description="특정 유저에게 경고를 1회 지급합니다. (관리자 전용)")
@app_commands.describe(member="경고를 받을 유저", reason="경고 사유")
@app_commands.guild_only()
@app_commands.checks.has_permissions(administrator=True)
async def add_warning(interaction: discord.Interaction, member: discord.Member, reason: str = "사유 없음"):
    await handle_add_warning(interaction, member, reason)


# /경고부여 도 같은 기능으로 동작 (이전 이름 호환)
@bot.tree.command(name="경고부여", description="특정 유저에게 경고를 1회 부여합니다. (관리자 전용)")
@app_commands.describe(member="경고를 받을 유저", reason="경고 사유")
@app_commands.guild_only()
@app_commands.checks.has_permissions(administrator=True)
async def add_warning_alias(interaction: discord.Interaction, member: discord.Member, reason: str = "사유 없음"):
    await handle_add_warning(interaction, member, reason)


async def handle_add_warning(interaction: discord.Interaction, member: discord.Member, reason: str):
    if member.bot:
        await interaction.response.send_message("❌ 봇에게는 경고를 부여할 수 없습니다.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    try:
        kind, value = await run_db(add_warning_db, interaction.guild_id, member.id, member.name)
    except Exception as e:
        print(f"[경고 DB 오류] {e}")
        await interaction.followup.send(f"❌ 경고 지급 중 오류가 발생했습니다.\n```{str(e)[:1500]}```", ephemeral=True)
        return

    if kind == "defense":
        await interaction.followup.send(
            f"🛡️ **{member.display_name}**님은 방어권을 사용해 경고를 방어했습니다!\n"
            f"남은 방어권: **{value}개**\n사유: {reason}",
            ephemeral=True,
        )
        return

    kick_msg = ""
    if value >= 3:
        try:
            await member.kick(reason=f"경고 {value}회 누적: {reason}")
            kick_msg = "\n🚪 경고 3회 이상 누적으로 서버에서 추방했습니다."
        except discord.Forbidden:
            kick_msg = "\n⚠️ 경고는 지급됐지만 봇에게 추방 권한이 없어 추방하지 못했습니다."
        except discord.HTTPException as kick_error:
            print(f"[경고 추방 오류] {kick_error}")
            kick_msg = "\n⚠️ 경고는 지급됐지만 추방 처리 중 오류가 발생했습니다."

    await interaction.followup.send(
        f"⚠️ **{member.display_name}**님에게 경고 **1회**를 부여했습니다.\n"
        f"현재 경고: **{value}회**\n사유: {reason}{kick_msg}",
        ephemeral=True,
    )


def remove_warning_db(cur, guild_id, user_id):
    cur.execute(
        "SELECT COALESCE(warnings, 0) FROM users WHERE guild_id=%s AND user_id=%s FOR UPDATE",
        (guild_id, user_id),
    )
    row = cur.fetchone()
    current = row[0] if row else 0
    if current <= 0:
        return None
    cur.execute("UPDATE users SET warnings = warnings - 1 WHERE guild_id=%s AND user_id=%s", (guild_id, user_id))
    return current - 1


@bot.tree.command(name="경고차감", description="특정 유저의 경고를 1회 차감합니다. (관리자 전용)")
@app_commands.describe(member="경고를 차감할 유저")
@app_commands.guild_only()
@app_commands.checks.has_permissions(administrator=True)
async def remove_warning(interaction: discord.Interaction, member: discord.Member):
    await interaction.response.defer(ephemeral=True)
    try:
        result = await run_db(remove_warning_db, interaction.guild_id, member.id)
    except Exception as e:
        await interaction.followup.send(f"❌ 오류 발생: {e}", ephemeral=True)
        return
    if result is None:
        await interaction.followup.send(f"❌ **{member.display_name}**님의 경고는 이미 0회입니다.", ephemeral=True)
    else:
        await interaction.followup.send(
            f"✅ **{member.display_name}**님의 경고를 1회 차감했습니다. (현재 경고: {result}회)", ephemeral=True
        )


def give_coins_db(cur, guild_id, user_id, username, amount):
    cur.execute(
        """
        INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
        VALUES (%s, %s, %s, %s, 0, 0, 0)
        ON CONFLICT (guild_id, user_id) DO UPDATE
            SET coins = COALESCE(users.coins, 0) + EXCLUDED.coins, username = EXCLUDED.username
        RETURNING coins
        """,
        (guild_id, user_id, username, amount),
    )
    return int(cur.fetchone()[0])


@bot.tree.command(name="코인지급", description="특정 유저에게 코인을 지급합니다. (관리자 전용)")
@app_commands.describe(member="코인을 받을 유저", amount="지급할 코인 수량")
@app_commands.guild_only()
@app_commands.checks.has_permissions(administrator=True)
async def give_coins(interaction: discord.Interaction, member: discord.Member, amount: int):
    if amount <= 0:
        await interaction.response.send_message("❌ 1 이상을 입력해 주세요.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    try:
        balance = await run_db(give_coins_db, interaction.guild_id, member.id, member.name, amount)
        await interaction.followup.send(
            f"✅ **{member.display_name}**님에게 **{amount:,}코인**을 지급했습니다! (잔액: {balance:,}코인)", ephemeral=True
        )
    except Exception as e:
        await interaction.followup.send(f"❌ 오류: {e}", ephemeral=True)


def take_coins_db(cur, guild_id, user_id, amount):
    cur.execute(
        "SELECT COALESCE(coins, 0) FROM users WHERE guild_id=%s AND user_id=%s FOR UPDATE",
        (guild_id, user_id),
    )
    row = cur.fetchone()
    if not row:
        return None
    removed = min(int(row[0]), amount)
    cur.execute(
        "UPDATE users SET coins = COALESCE(coins, 0) - %s WHERE guild_id=%s AND user_id=%s",
        (removed, guild_id, user_id),
    )
    return removed, int(row[0]) - removed


@bot.tree.command(name="코인회수", description="특정 유저의 코인을 회수합니다. (관리자 전용)")
@app_commands.describe(member="코인을 회수할 유저", amount="회수할 코인 수량")
@app_commands.guild_only()
@app_commands.checks.has_permissions(administrator=True)
async def take_coins(interaction: discord.Interaction, member: discord.Member, amount: int):
    if amount <= 0:
        await interaction.response.send_message("❌ 1 이상을 입력해 주세요.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    try:
        result = await run_db(take_coins_db, interaction.guild_id, member.id, amount)
    except Exception as e:
        await interaction.followup.send(f"❌ 오류: {e}", ephemeral=True)
        return
    if result is None:
        await interaction.followup.send("❌ 유저 데이터를 찾을 수 없습니다.", ephemeral=True)
    else:
        removed, balance = result
        await interaction.followup.send(
            f"✅ **{member.display_name}**님의 코인 **{removed:,}개** 회수 완료 (잔액: {balance:,}코인)", ephemeral=True
        )


def give_defense_db(cur, guild_id, user_id, username, amount):
    cur.execute(
        """
        INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
        VALUES (%s, %s, %s, 0, 0, 0, %s)
        ON CONFLICT (guild_id, user_id) DO UPDATE
            SET defense_tickets = COALESCE(users.defense_tickets, 0) + EXCLUDED.defense_tickets,
                username = EXCLUDED.username
        RETURNING defense_tickets
        """,
        (guild_id, user_id, username, amount),
    )
    return int(cur.fetchone()[0])


@bot.tree.command(name="방어권지급", description="특정 유저에게 방어권을 지급합니다. (관리자 전용)")
@app_commands.describe(member="방어권을 받을 유저", amount="지급할 수량")
@app_commands.guild_only()
@app_commands.checks.has_permissions(administrator=True)
async def give_defense_ticket(interaction: discord.Interaction, member: discord.Member, amount: int = 1):
    if amount <= 0:
        await interaction.response.send_message("❌ 1 이상을 입력해 주세요.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    try:
        total = await run_db(give_defense_db, interaction.guild_id, member.id, member.name, amount)
        await interaction.followup.send(
            f"🛡️ **{member.display_name}**님에게 방어권 **{amount}개**를 지급했습니다! (보유: {total}개)", ephemeral=True
        )
    except Exception as e:
        await interaction.followup.send(f"❌ 오류: {e}", ephemeral=True)


# ==========================================
# 8. 🎰 슬롯머신
# ==========================================
SLOT_ICONS = ["🍒", "🍋", "🍊", "🔔", "⭐", "💎", "7️⃣"]
SLOT_OUTCOMES = (("jackpot", 3.0), ("double", 1.5), ("pair", 0.5), ("lose", 0.0))
active_slot_spins = set()
slot_semaphore = asyncio.Semaphore(50)


def roll_slot_result(rtp_percent: int):
    rtp = max(0, min(150, int(rtp_percent))) / 100.0
    winning_average_multiplier = sum(multiplier for _, multiplier in SLOT_OUTCOMES[:3]) / 3.0
    total_win_probability = min(1.0, rtp / winning_average_multiplier)

    if random.random() < total_win_probability:
        outcome_name, multiplier = random.choice(SLOT_OUTCOMES[:3])
        icon = random.choice(SLOT_ICONS)
        if outcome_name == "jackpot":
            result_icons = [icon, icon, icon]
        else:
            other = random.choice([i for i in SLOT_ICONS if i != icon])
            result_icons = [icon, icon, other]
            random.shuffle(result_icons)
        return result_icons, multiplier

    return random.sample(SLOT_ICONS, 3), 0.0


def play_slot_db(cur, guild_id, user_id, bet):
    """베팅 차감 → 결과 → 지급을 하나의 트랜잭션으로 처리 (중간 오류 시 코인 손실 없음)."""
    ensure_user(cur, guild_id, user_id)
    cur.execute(
        """
        UPDATE users SET coins = coins - %s
        WHERE guild_id=%s AND user_id=%s AND COALESCE(coins, 0) >= %s
        RETURNING coins
        """,
        (bet, guild_id, user_id, bet),
    )
    row = cur.fetchone()
    if row is None:
        cur.execute("SELECT COALESCE(coins, 0) FROM users WHERE guild_id=%s AND user_id=%s", (guild_id, user_id))
        r = cur.fetchone()
        return {"ok": False, "coins": int(r[0]) if r else 0}

    cur.execute("SELECT slot_rtp FROM guild_settings WHERE guild_id=%s", (guild_id,))
    r = cur.fetchone()
    rtp = int(r[0]) if r and r[0] is not None else 85

    icons, multiplier = roll_slot_result(rtp)
    payout = int(bet * multiplier)
    balance = int(row[0])
    if payout > 0:
        cur.execute(
            "UPDATE users SET coins = coins + %s WHERE guild_id=%s AND user_id=%s RETURNING coins",
            (payout, guild_id, user_id),
        )
        balance = int(cur.fetchone()[0])
    return {"ok": True, "icons": icons, "multiplier": multiplier, "payout": payout, "coins": balance}


def build_slot_text(res, bet):
    icons = res["icons"]
    payout = res["payout"]
    net = payout - bet
    if res["multiplier"] >= 3.0:
        result_text = f"🎉 **[잭팟 당첨!]** **{payout:,}코인** 획득! (순이익 {net:+,})"
    elif payout > bet:
        result_text = f"✨ **[당첨!]** **{payout:,}코인** 획득! (순이익 {net:+,})"
    elif payout > 0:
        result_text = f"😅 **[부분 당첨]** {payout:,}코인 돌려받았습니다. (손익 {net:+,})"
    else:
        result_text = "😢 **[꽝]** 아쉽게도 꽝입니다."
    return (
        f"🎰 **[슬롯머신 결과]**\n` {icons[0]} | {icons[1]} | {icons[2]} `\n\n"
        f"{result_text}\n💰 현재 잔액: **{res['coins']:,}코인**"
    )


class SlotMachineView(discord.ui.View):
    def __init__(self, author_id: int, bet_amount: int):
        super().__init__(timeout=30)
        self.author_id = author_id
        self.bet_amount = bet_amount
        self.message = None

    async def on_timeout(self):
        if self.message is not None:
            try:
                await self.message.edit(view=None)
            except Exception:
                pass

    @discord.ui.button(label="🎰 다시 돌리기", style=discord.ButtonStyle.success)
    async def spin_again(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("본인이 실행한 슬롯머신만 다시 돌릴 수 있습니다.", ephemeral=True)
            return
        if interaction.guild_id is None:
            return

        guild_id = interaction.guild_id
        user_id = interaction.user.id
        spin_key = (guild_id, user_id)
        if spin_key in active_slot_spins:
            await interaction.response.send_message("⏳ 이미 슬롯머신이 돌아가고 있습니다.", ephemeral=True)
            return

        active_slot_spins.add(spin_key)
        self.stop()  # 이전 뷰의 타임아웃이 새 뷰를 지우지 않도록 중지
        msg = interaction.message
        try:
            await interaction.response.edit_message(content="🎰 **슬롯머신이 돌아가는 중입니다...**\n` 🔄 | 🔄 | 🔄 `", view=None)

            async with slot_semaphore:
                res = await run_db(play_slot_db, guild_id, user_id, self.bet_amount)
                if not res["ok"]:
                    await msg.edit(content=f"❌ 코인이 부족합니다! (현재 잔액: {res['coins']:,}코인)", view=None)
                    return
                await asyncio.sleep(0.6)
                new_view = SlotMachineView(self.author_id, self.bet_amount)
                new_view.message = msg
                await msg.edit(content=build_slot_text(res, self.bet_amount), view=new_view)
        except Exception as e:
            print(f"[슬롯 오류] {e}")
            try:
                await msg.edit(content=f"❌ 슬롯머신 처리 중 오류가 발생했습니다: {str(e)[:200]}", view=None)
            except Exception:
                pass
        finally:
            active_slot_spins.discard(spin_key)


@bot.tree.command(name="슬롯머신", description="코인을 걸고 슬롯머신을 돌립니다. (1~500코인)")
@app_commands.describe(bet="배팅할 코인 수량")
@app_commands.guild_only()
async def slot_machine(interaction: discord.Interaction, bet: int):
    if bet < 1 or bet > 500:
        await interaction.response.send_message("❌ 배팅 수량은 1 ~ 500코인 사이여야 합니다.", ephemeral=True)
        return

    user_id = interaction.user.id
    guild_id = interaction.guild_id
    spin_key = (guild_id, user_id)

    if spin_key in active_slot_spins:
        await interaction.response.send_message("⏳ 이미 슬롯머신이 돌아가고 있습니다.", ephemeral=True)
        return

    active_slot_spins.add(spin_key)
    msg = None
    try:
        await interaction.response.send_message("🎰 **슬롯머신이 돌아가는 중입니다...**\n` 🔄 | 🔄 | 🔄 `")
        msg = await interaction.original_response()

        async with slot_semaphore:
            res = await run_db(play_slot_db, guild_id, user_id, bet)
            if not res["ok"]:
                await msg.edit(content=f"❌ 코인이 부족합니다! (현재 잔액: {res['coins']:,}코인)")
                return
            await asyncio.sleep(0.6)
            view = SlotMachineView(user_id, bet)
            view.message = msg
            await msg.edit(content=build_slot_text(res, bet), view=view)
    except Exception as e:
        print(f"[슬롯 오류] {e}")
        try:
            if msg is not None:
                await msg.edit(content=f"❌ 슬롯머신 처리 중 오류가 발생했습니다: {str(e)[:200]}", view=None)
        except Exception:
            pass
    finally:
        active_slot_spins.discard(spin_key)


# ==========================================
# 9. 일반 명령어
# ==========================================
def get_info_db(cur, guild_id, user_id):
    cur.execute(
        """
        SELECT COALESCE(coins, 0), COALESCE(voice_minutes, 0), COALESCE(warnings, 0), COALESCE(defense_tickets, 0)
        FROM users WHERE guild_id=%s AND user_id=%s
        """,
        (guild_id, user_id),
    )
    row = cur.fetchone()
    return tuple(row) if row else (0, 0, 0, 0)


@bot.tree.command(name="정보", description="사용자의 코인, 음성 시간, 경고, 방어권을 확인합니다.")
@app_commands.describe(member="조회할 사용자 (선택하지 않으면 본인)")
@app_commands.guild_only()
async def my_info(interaction: discord.Interaction, member: discord.Member = None):
    target = member or interaction.user
    await interaction.response.defer(ephemeral=True)
    try:
        coins, minutes, warnings, tickets = await run_db(get_info_db, interaction.guild_id, target.id)
    except Exception as e:
        await interaction.followup.send(f"❌ 오류: {e}", ephemeral=True)
        return
    await interaction.followup.send(
        f"**{target.mention}**님의 정보:\n- 🪙 코인: **{coins:,}개**\n- ⌛ 음성 접속: **{minutes}분**\n"
        f"- ⚠️ 경고: **{warnings}회**\n- 🛡 방어권: **{tickets}개**",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )


def register_referral_db(cur, guild_id, user_id, username, referrer_id, referrer_name):
    ensure_user(cur, guild_id, user_id, username)
    cur.execute(
        "SELECT COALESCE(voice_minutes, 0), referred_by FROM users WHERE guild_id=%s AND user_id=%s FOR UPDATE",
        (guild_id, user_id),
    )
    minutes, referred_by = cur.fetchone()
    if referred_by is not None:
        return ("already", 0)
    if minutes < 30:
        return ("short", minutes)

    cur.execute("SELECT referral_reward FROM guild_settings WHERE guild_id=%s", (guild_id,))
    setting = cur.fetchone()
    reward = int(setting[0]) if setting and setting[0] is not None else 30

    cur.execute("UPDATE users SET referred_by=%s WHERE guild_id=%s AND user_id=%s", (referrer_id, guild_id, user_id))
    cur.execute(
        """
        INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
        VALUES (%s, %s, %s, %s, 0, 0, 0)
        ON CONFLICT (guild_id, user_id) DO UPDATE
            SET coins = COALESCE(users.coins, 0) + EXCLUDED.coins,
                username = COALESCE(EXCLUDED.username, users.username)
        """,
        (guild_id, referrer_id, referrer_name, reward),
    )
    return ("ok", reward)


@bot.tree.command(name="추천인", description="추천인을 등록합니다. (음성 30분 이상 시 가능)")
@app_commands.describe(referrer="추천할 유저")
@app_commands.guild_only()
async def register_referral(interaction: discord.Interaction, referrer: discord.Member):
    if referrer.id == interaction.user.id or referrer.bot:
        await interaction.response.send_message("올바르지 않은 추천인 대상입니다.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    try:
        status, value = await run_db(
            register_referral_db, interaction.guild_id, interaction.user.id,
            interaction.user.name, referrer.id, referrer.name,
        )
    except Exception as e:
        await interaction.followup.send(f"❌ 오류: {e}", ephemeral=True)
        return

    if status == "already":
        await interaction.followup.send("이미 추천인을 등록하셨습니다.", ephemeral=True)
    elif status == "short":
        await interaction.followup.send(
            f"❌ 음성 접속 시간 30분 이상일 때만 가능합니다. (현재 {value}분)", ephemeral=True
        )
    else:
        await interaction.followup.send(
            f"✅ 성공적으로 추천인을 등록했습니다! ({referrer.mention}님에게 {value}코인 지급)",
            ephemeral=True,
            allowed_mentions=discord.AllowedMentions.none(),
        )


def ranking_db(cur, guild_id):
    cur.execute(
        "SELECT user_id, coins FROM users WHERE guild_id=%s AND coins > 0 ORDER BY coins DESC LIMIT 10",
        (guild_id,),
    )
    return cur.fetchall()


@bot.tree.command(name="코인순위", description="서버 코인 상위 10명을 확인합니다.")
@app_commands.guild_only()
async def coin_ranking(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    try:
        rows = await run_db(ranking_db, interaction.guild_id)
    except Exception as e:
        await interaction.followup.send(f"❌ 오류: {e}", ephemeral=True)
        return

    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    lines = [
        f"{medals.get(i, f'{i}.')} <@{uid}> — **{c:,}코인**"
        for i, (uid, c) in enumerate(rows, start=1)
    ] if rows else ["아직 코인 보유자가 없습니다."]

    embed = discord.Embed(title=f"🏆 {interaction.guild.name} 코인 순위", description="\n".join(lines), color=discord.Color.gold())
    await interaction.followup.send(embed=embed, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())


@bot.tree.command(name="명령어", description="봇 명령어 목록을 확인합니다.")
async def show_commands(interaction: discord.Interaction):
    embed = discord.Embed(title="🤖 봇 명령어 안내", color=discord.Color.blue())
    embed.add_field(
        name="👤 일반 명령어",
        value="• `/정보 [유저]`\n• `/추천인 [유저]`\n• `/슬롯머신 [배팅액]`\n• `/코인순위`\n• `/명령어`",
        inline=False,
    )
    embed.add_field(
        name="🛡 관리자 전용",
        value="• `/채팅청소 [수량] [유저]` (수량 입력 후 유저는 선택사항)\n• `/경고지급` (또는 `/경고부여`)\n• `/경고차감`\n• `/코인지급`\n• `/코인회수`\n• `/방어권지급`\n• `/닉네임동기화`",
        inline=False,
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


if __name__ == "__main__":
    token = os.getenv("DISCORD_TOKEN") or os.getenv("DISCORD_BOT_TOKEN")
    if not token:
        print("❌ 에러: DISCORD_TOKEN 환경 변수가 설정되지 않았습니다!")
        raise SystemExit(1)
    bot.run(token)

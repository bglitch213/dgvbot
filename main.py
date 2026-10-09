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
        cur.execute("ALTER TABLE guild_settings ADD COLUMN IF NOT EXISTS log_channel_id BIGINT DEFAULT NULL")

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


_last_guild_sync = {}  # guild_id -> 마지막 동기화 시각 (너무 잦은 동기화 방지)


async def sync_guild_commands(guild: discord.Guild, min_interval: float = 0.0) -> bool:
    """이 서버의 슬래시 명령어를 현재 코드 기준으로 디스코드에 덮어써서 맞춘다."""
    now = time.time()
    if now - _last_guild_sync.get(guild.id, 0) < min_interval:
        return False
    _last_guild_sync[guild.id] = now
    try:
        bot.tree.copy_global_to(guild=guild)
        synced = await bot.tree.sync(guild=guild)
        names = ", ".join(c.name for c in synced)
        print(f"[COMMAND SYNC] {guild.name} ({guild.id}) -> {len(synced)}개: {names}")
        for c in synced:
            if c.name == "경고지급":
                print(f"[COMMAND SYNC] 경고지급 입력칸: {[o.name for o in c.options]}")
        return True
    except Exception as e:
        print(f"[COMMAND SYNC] {guild.name} ({guild.id}) 동기화 오류: {e}")
        return False


@bot.event
async def on_ready():
    global commands_synced
    print(f"로그인 완료: {bot.user}")

    for guild in bot.guilds:
        try:
            await guild.chunk(cache=True)
        except Exception:
            pass

    # 예전 버전이 '전역'으로 등록해 둔 낡은 명령어가 서버 명령어와 겹쳐 보이는 것을 막기 위해
    # 전역 등록분을 비움 (서버별 등록은 아래에서 따로 함)
    try:
        await bot.http.bulk_upsert_global_commands(bot.application_id, [])
    except Exception as e:
        print(f"[COMMAND SYNC] 전역 명령어 정리 실패(무시 가능): {e}")

    # 재연결로 on_ready 가 다시 불려도 매번 동기화 (서버별로 따로 처리 → 한 서버가 실패해도 계속 진행)
    results = [await sync_guild_commands(guild) for guild in bot.guilds]
    commands_synced = all(results)

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
    await sync_guild_commands(guild)


# 모든 슬래시 명령어의 공통 오류 처리 (응답 없음 방지)
@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CommandSignatureMismatch):
        # 디스코드에 등록된 명령어 모양이 코드와 다름 → 이 서버 명령어를 바로 다시 동기화(자동 복구)
        print(f"[명령어 오류] 시그니처 불일치: {getattr(error.command, 'name', '?')} → 자동 재동기화 시도")
        fixed = False
        if interaction.guild is not None:
            fixed = await sync_guild_commands(interaction.guild, min_interval=30)
        if fixed:
            msg = ("🔄 명령어 정보가 오래돼서 방금 자동으로 갱신했습니다.\n"
                   "**디스코드 앱을 새로고침(Ctrl+R, 모바일은 앱 완전 종료 후 재실행)** 한 뒤 다시 입력해 주세요.")
        else:
            msg = ("🔄 명령어 정보를 갱신 중입니다. 잠시 후 **디스코드 앱을 새로고침(Ctrl+R)** 하고 다시 입력해 주세요.")
    elif isinstance(error, app_commands.MissingPermissions):
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
@app_commands.describe(limit="삭제할 메시지 수 (1~100, 필수)", member="청소할 대상 유저 (선택, 비우면 전체 최근 메시지)")
@app_commands.rename(limit="수량", member="유저")
@app_commands.guild_only()
@app_commands.checks.has_permissions(administrator=True)
async def clear_user_chat(interaction: discord.Interaction, limit: app_commands.Range[int, 1, 100], member: discord.Member = None):
    if limit < 1 or limit > 100:
        await interaction.response.send_message("⚠️ 삭제 수량은 1부터 100 사이로 입력해 주세요.", ephemeral=True)
        return

    channel = interaction.channel
    # 캐시에 없는 채널(PartialMessageable 등)이면 실제 채널 객체로 교체
    if channel is None or not hasattr(channel, "delete_messages"):
        try:
            channel = interaction.guild.get_channel_or_thread(interaction.channel_id) \
                or await interaction.guild.fetch_channel(interaction.channel_id)
        except Exception:
            channel = None
    if channel is None or not hasattr(channel, "history") or not hasattr(channel, "delete_messages"):
        await interaction.response.send_message("⚠️ 이 채널에서는 청소할 수 없습니다.", ephemeral=True)
        return

    perms = channel.permissions_for(interaction.guild.me)
    if not (perms.view_channel and perms.manage_messages and perms.read_message_history):
        await interaction.response.send_message(
            "❌ 봇에게 이 채널의 **채널 보기 / 메시지 관리 / 메시지 기록 보기** 권한이 필요합니다.", ephemeral=True
        )
        return

    await interaction.response.defer(thinking=True, ephemeral=True)

    try:
        messages_to_delete = []
        searched_limit = 500
        if member:
            async for message in channel.history(limit=searched_limit):
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
        failed_count = 0
        fail_reasons = set()

        def _reason(e: Exception) -> str:
            if isinstance(e, discord.Forbidden):
                return "봇 권한 부족(메시지 관리)"
            status = getattr(e, "status", "")
            text = str(getattr(e, "text", "") or e)[:60]
            return f"{status} {text}".strip()

        async def _delete_one(m) -> bool:
            try:
                await m.delete()
                return True
            except discord.NotFound:
                return True  # 이미 삭제된 메시지
            except Exception as e:
                fail_reasons.add(_reason(e))
                return False

        for i in range(0, len(recent_messages), 100):
            chunk = recent_messages[i:i + 100]
            try:
                if len(chunk) > 1:
                    await channel.delete_messages(chunk)
                else:
                    await chunk[0].delete()
                deleted_count += len(chunk)
            except discord.Forbidden as e:
                failed_count += len(chunk)
                fail_reasons.add(_reason(e))
            except discord.HTTPException as e:
                # 일괄 삭제 실패 시 한 개씩 다시 시도
                print(f"[채팅청소 일괄삭제 오류 → 개별 삭제로 재시도] {e}")
                for m in chunk:
                    if await _delete_one(m):
                        deleted_count += 1
                    else:
                        failed_count += 1
                    await asyncio.sleep(0.3)

        for m in old_messages:
            if await _delete_one(m):
                deleted_count += 1
            else:
                failed_count += 1
            await asyncio.sleep(0.5)

        if deleted_count == 0 and failed_count > 0:
            await interaction.followup.send(
                f"❌ 메시지를 삭제하지 못했습니다. (실패 {failed_count}개)\n사유: {', '.join(fail_reasons) or '알 수 없음'}",
                ephemeral=True,
            )
            return

        result = f"🧹 {target_name} 메시지 **{deleted_count}개**를 청소했습니다!"
        detail = f"대상: {member.mention if member else '채널 전체 최근 메시지'} / 요청: {limit}개 / 삭제: {deleted_count}개"
        if member and len(messages_to_delete) < limit:
            result += f"\nℹ️ 최근 {searched_limit}개 메시지 안에서 {len(messages_to_delete)}개만 찾았습니다."
        if failed_count:
            reasons = ", ".join(fail_reasons) or "알 수 없음"
            result += f"\n⚠️ {failed_count}개는 삭제하지 못했습니다. (사유: {reasons})"
            detail += f" / 실패: {failed_count}개"

        await finish_admin_command(interaction, "채팅청소", result, detail)
    except Exception as e:
        print(f"[채팅청소 오류] {e}")
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
        await finish_admin_command(
            interaction, "닉네임동기화",
            f"✅ 성공적으로 서버 멤버 {len(rows)}명의 닉네임을 DB에 동기화했습니다!",
            f"멤버 {len(rows)}명 동기화",
        )
    except Exception as e:
        await interaction.followup.send(f"❌ 동기화 중 오류 발생: {e}", ephemeral=True)


# ==========================================
# 7. 🚨 관리자 명령어 (경고, 코인, 방어권)
# ==========================================
# ---------- 관리자 명령어 사용 기록 (로그 채널 저장 + 채팅창 공개 알림) ----------
NO_MENTIONS = discord.AllowedMentions.none()


def set_log_channel_db(cur, guild_id, channel_id):
    cur.execute(
        """
        INSERT INTO guild_settings (guild_id, log_channel_id) VALUES (%s, %s)
        ON CONFLICT (guild_id) DO UPDATE SET log_channel_id = EXCLUDED.log_channel_id
        """,
        (guild_id, channel_id),
    )


def get_log_channel_db(cur, guild_id):
    cur.execute("SELECT log_channel_id FROM guild_settings WHERE guild_id=%s", (guild_id,))
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def build_admin_embed(interaction: discord.Interaction, command_name: str, detail: str) -> discord.Embed:
    user = interaction.user
    channel_text = getattr(interaction.channel, "mention", None) or "알 수 없음"
    embed = discord.Embed(
        title="🛡️ 관리자 명령어 사용",
        description=f"{user.mention} 님이 `/{command_name}` 명령어를 사용했습니다.",
        color=discord.Color.orange(),
        timestamp=discord.utils.utcnow(),
    )
    embed.add_field(name="👤 사용자", value=f"{user.mention}\n`{user}` (`{user.id}`)", inline=True)
    embed.add_field(name="⌨️ 명령어", value=f"`/{command_name}`", inline=True)
    embed.add_field(name="📍 사용 채널", value=channel_text, inline=True)
    embed.add_field(name="📝 내용", value=(detail or "-")[:1000], inline=False)
    embed.set_footer(text=datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST"))
    return embed


async def send_to_log_channel(guild: discord.Guild, embed: discord.Embed):
    """
    /로그 로 설정한 채널에 기록을 남긴다.
    반환: (True, 채널멘션) 성공 / (None, 사유) 미설정 / (False, 사유) 실패
    """
    try:
        channel_id = await run_db(get_log_channel_db, guild.id)
    except Exception as e:
        return False, f"로그 채널 설정 조회 오류: {str(e)[:200]}"
    if not channel_id:
        return None, "로그 채널이 설정되지 않았습니다."

    channel = guild.get_channel(channel_id)
    if channel is None:
        try:
            channel = await guild.fetch_channel(channel_id)
        except discord.NotFound:
            return False, "설정된 로그 채널이 삭제되었습니다. `/로그` 로 다시 설정해 주세요."
        except discord.Forbidden:
            return False, "봇이 설정된 로그 채널을 볼 수 없습니다. (채널 보기 권한 필요)"
        except discord.HTTPException as e:
            return False, f"로그 채널 조회 오류: {e}"

    try:
        await channel.send(embed=embed, allowed_mentions=NO_MENTIONS)
    except discord.Forbidden:
        return False, f"봇에게 {channel.mention} 의 **메시지 보내기 / 링크 첨부** 권한이 없습니다."
    except discord.HTTPException as e:
        return False, f"로그 전송 오류: {e}"
    return True, channel.mention


async def finish_admin_command(interaction: discord.Interaction, command_name: str, result_text: str, detail: str):
    """
    관리자 명령어가 성공했을 때 마지막에 호출 (반드시 interaction.response.defer() 이후):
      1) /로그 로 설정한 채널에 '누가 어떤 명령어를 썼는지' 공개 기록 (모두에게 보이는 일반 메시지)
      2) 실행자에게만 결과 + 기록 성공/실패 안내
      3) 로그 채널이 미설정/오류일 때만, 기록이 사라지지 않도록 현재 채팅창에 대신 공개
    """
    embed = build_admin_embed(interaction, command_name, detail)
    ok, info = await send_to_log_channel(interaction.guild, embed)

    if ok is True:
        status = f"\n\n📝 사용 기록을 {info} 에 공개로 남겼습니다."
    else:
        if ok is None:
            status = "\n\n⚠️ 로그 채널이 설정되지 않았습니다. `/로그` 로 채널을 설정해 주세요. (이번 기록은 이 채팅창에 대신 공개했습니다)"
        else:
            status = f"\n\n⚠️ 사용 기록 저장 실패: {info}\n(이번 기록은 이 채팅창에 대신 공개했습니다)"
            print(f"[로그 오류] {interaction.guild_id} /{command_name}: {info}")
        try:
            await interaction.followup.send(embed=embed, ephemeral=False, allowed_mentions=NO_MENTIONS)
        except Exception as e:
            print(f"[공개 기록 대체 전송 오류] /{command_name}: {e}")

    await interaction.followup.send(result_text + status, ephemeral=True, allowed_mentions=NO_MENTIONS)


@bot.tree.command(name="로그", description="이 채널을 관리자 명령어 사용 기록 채널로 지정합니다. (관리자 전용)")
@app_commands.guild_only()
@app_commands.checks.has_permissions(administrator=True)
async def set_log_channel(interaction: discord.Interaction):
    channel = interaction.channel
    guild = interaction.guild
    if channel is None or not hasattr(channel, "send"):
        await interaction.response.send_message("⚠️ 이 채널은 로그 채널로 지정할 수 없습니다.", ephemeral=True)
        return

    perms = channel.permissions_for(guild.me)
    missing = [
        label for label, ok in (
            ("채널 보기", perms.view_channel),
            ("메시지 보내기", perms.send_messages),
            ("링크 첨부(임베드)", perms.embed_links),
        ) if not ok
    ]
    if missing:
        await interaction.response.send_message(
            f"❌ 봇에게 이 채널의 **{', '.join(missing)}** 권한이 없어 지정하지 못했습니다.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)
    try:
        await run_db(set_log_channel_db, guild.id, channel.id)
        await finish_admin_command(
            interaction, "로그", f"✅ 이 채널({channel.mention})을 로그 채널로 지정했습니다.",
            f"로그 채널 지정: {channel.mention}",
        )
    except Exception as e:
        print(f"[로그 설정 오류] {e}")
        await interaction.followup.send(f"❌ 오류: {e}", ephemeral=True)


def compute_warning_change(warnings, tickets, amount):
    """
    경고 증감 계산 (DB와 무관한 순수 함수).
    - amount > 0 : 경고 amount회 지급. 방어권이 있으면 방어권을 먼저 1개씩 소모해 방어하고,
                   방어권이 모자란 나머지 횟수만 경고로 쌓임.
    - amount < 0 : 경고를 |amount|회 차감 (0 밑으로는 내려가지 않음). 방어권은 절대 건드리지 않음.
    반환: (새 경고, 새 방어권, 소모된 방어권, 실제 추가된 경고, 실제 차감된 경고)
    """
    warnings = max(0, int(warnings or 0))
    tickets = max(0, int(tickets or 0))
    if amount > 0:
        used = min(tickets, amount)
        added = amount - used
        return warnings + added, tickets - used, used, added, 0
    if amount < 0:
        removed = min(warnings, -amount)
        return warnings - removed, tickets, 0, 0, removed
    return warnings, tickets, 0, 0, 0


def adjust_warning_db(cur, guild_id, user_id, username, amount):
    ensure_user(cur, guild_id, user_id, username)
    cur.execute(
        """
        SELECT COALESCE(warnings, 0), COALESCE(defense_tickets, 0)
        FROM users WHERE guild_id=%s AND user_id=%s FOR UPDATE
        """,
        (guild_id, user_id),
    )
    cur_warnings, cur_tickets = cur.fetchone()
    new_w, new_t, used, added, removed = compute_warning_change(cur_warnings, cur_tickets, amount)
    cur.execute(
        """
        UPDATE users SET warnings=%s, defense_tickets=%s, username=%s
        WHERE guild_id=%s AND user_id=%s
        """,
        (new_w, new_t, username, guild_id, user_id),
    )
    return {"warnings": new_w, "tickets": new_t, "used": used, "added": added, "removed": removed}


@bot.tree.command(name="경고지급", description="경고를 지급/차감합니다. 수량이 양수면 지급, 음수면 차감 (관리자 전용)")
@app_commands.describe(member="대상 유저", amount="경고 수량 (양수 = 지급, 음수 = 차감)")
@app_commands.rename(member="닉네임", amount="수량")
@app_commands.guild_only()
@app_commands.checks.has_permissions(administrator=True)
async def add_warning(interaction: discord.Interaction, member: discord.Member, amount: int):
    await handle_add_warning(interaction, member, amount)


async def handle_add_warning(interaction: discord.Interaction, member: discord.Member, amount: int):
    if member.bot:
        await interaction.response.send_message("❌ 봇에게는 경고를 부여할 수 없습니다.", ephemeral=True)
        return
    if amount == 0:
        await interaction.response.send_message("❌ 수량은 0이 될 수 없습니다. (양수 = 지급, 음수 = 차감)", ephemeral=True)
        return
    if abs(amount) > 100:
        await interaction.response.send_message("❌ 수량은 -100 ~ 100 사이로 입력해 주세요.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    try:
        r = await run_db(adjust_warning_db, interaction.guild_id, member.id, member.name, amount)
    except Exception as e:
        print(f"[경고 DB 오류] {e}")
        await interaction.followup.send(f"❌ 경고 처리 중 오류가 발생했습니다.\n```{str(e)[:1500]}```", ephemeral=True)
        return

    name = member.display_name

    # ---- 차감 (음수) ----
    if amount < 0:
        if r["removed"] == 0:
            await finish_admin_command(
                interaction, "경고지급",
                f"ℹ️ **{name}**님의 경고는 이미 0회라 차감할 경고가 없습니다.\n"
                f"현재 경고: **0회** / 방어권: **{r['tickets']}개** (방어권은 변동 없음)",
                f"대상: {member.mention} / 수량: {amount} / 결과: 이미 경고 0회라 차감 없음",
            )
        else:
            await finish_admin_command(
                interaction, "경고지급",
                f"✅ **{name}**님의 경고를 **{r['removed']}회** 차감했습니다.\n"
                f"현재 경고: **{r['warnings']}회** / 방어권: **{r['tickets']}개**",
                f"대상: {member.mention} / 수량: {amount} / 결과: 경고 {r['removed']}회 차감 (현재 경고 {r['warnings']}회)",
            )
        return

    # ---- 지급 (양수): 방어권 먼저 소모 ----
    lines = []
    if r["used"] > 0:
        lines.append(f"🛡️ 방어권 **{r['used']}개**를 사용해 경고 {r['used']}회를 방어했습니다.")
    if r["added"] > 0:
        lines.append(f"⚠️ 경고 **{r['added']}회**를 부여했습니다.")

    kick_msg = ""
    if r["added"] > 0 and r["warnings"] >= 3:
        try:
            await member.kick(reason=f"경고 {r['warnings']}회 누적")
            kick_msg = "\n🚪 경고 3회 이상 누적으로 서버에서 추방했습니다."
        except discord.Forbidden:
            kick_msg = "\n⚠️ 경고는 지급됐지만 봇에게 추방 권한이 없어 추방하지 못했습니다."
        except discord.HTTPException as kick_error:
            print(f"[경고 추방 오류] {kick_error}")
            kick_msg = "\n⚠️ 경고는 지급됐지만 추방 처리 중 오류가 발생했습니다."

    detail = (
        f"대상: {member.mention} / 수량: +{amount} / 방어권 사용 {r['used']}개 · 경고 부여 {r['added']}회 "
        f"(현재 경고 {r['warnings']}회, 방어권 {r['tickets']}개)"
    )
    if kick_msg:
        detail += f"\n{kick_msg.strip()}"
    await finish_admin_command(
        interaction, "경고지급",
        f"**{name}**님\n" + "\n".join(lines) + "\n"
        f"현재 경고: **{r['warnings']}회** / 남은 방어권: **{r['tickets']}개**"
        f"{kick_msg}",
        detail,
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
        await finish_admin_command(
            interaction, "코인지급",
            f"✅ **{member.display_name}**님에게 **{amount:,}코인**을 지급했습니다! (잔액: {balance:,}코인)",
            f"대상: {member.mention} / 지급: {amount:,}코인 / 잔액: {balance:,}코인",
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
        await finish_admin_command(
            interaction, "코인회수",
            f"✅ **{member.display_name}**님의 코인 **{removed:,}개** 회수 완료 (잔액: {balance:,}코인)",
            f"대상: {member.mention} / 요청: {amount:,}코인 / 회수: {removed:,}코인 / 잔액: {balance:,}코인",
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
        await finish_admin_command(
            interaction, "방어권지급",
            f"🛡️ **{member.display_name}**님에게 방어권 **{amount}개**를 지급했습니다! (보유: {total}개)",
            f"대상: {member.mention} / 지급: {amount}개 / 보유: {total}개",
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


# ---------- 슬롯머신 환수율 (숨김 명령어 /슬롯머신설정: /명령어 목록에 표시하지 않음) ----------
def set_slot_rtp_db(cur, guild_id, rtp):
    cur.execute(
        """
        INSERT INTO guild_settings (guild_id, slot_rtp) VALUES (%s, %s)
        ON CONFLICT (guild_id) DO UPDATE SET slot_rtp = EXCLUDED.slot_rtp
        """,
        (guild_id, rtp),
    )


def get_slot_rtp_db(cur, guild_id):
    cur.execute("SELECT slot_rtp FROM guild_settings WHERE guild_id=%s", (guild_id,))
    r = cur.fetchone()
    return int(r[0]) if r and r[0] is not None else 85


@bot.tree.command(name="슬롯머신설정", description="슬롯머신 환수율(%)을 설정합니다. (관리자 전용)")
@app_commands.describe(percent="환수율 % (0~150)")
@app_commands.rename(percent="환수율")
@app_commands.guild_only()
@app_commands.default_permissions(administrator=True)
@app_commands.checks.has_permissions(administrator=True)
async def set_slot_rtp(interaction: discord.Interaction, percent: app_commands.Range[int, 0, 150]):
    await interaction.response.defer(ephemeral=True)
    try:
        old = await run_db(get_slot_rtp_db, interaction.guild.id)
        await run_db(set_slot_rtp_db, interaction.guild.id, percent)
        # 비공개 명령어이므로 로그 채널/채팅창에 공개 기록을 남기지 않고 본인에게만 응답합니다.
        await interaction.followup.send(f"🎰 슬롯머신 환수율을 **{old}% → {percent}%** 로 변경했습니다.", ephemeral=True)
    except Exception as e:
        print(f"[슬롯머신설정 오류] {e}")
        await interaction.followup.send(f"❌ 오류: {e}", ephemeral=True)


@bot.tree.command(name="명령어", description="봇 명령어 목록을 확인합니다.")
async def show_commands(interaction: discord.Interaction):
    embed = discord.Embed(title="🤖 봇 명령어 안내", color=discord.Color.blue())
    embed.add_field(
        name="👤 일반 명령어",
        value="• `/정보 [유저]`\n• `/추천인 [유저]`\n• `/슬롯머신 [배팅액]`\n• `/코인순위`\n• `/명령어`",
        inline=False,
    )

    # 관리자에게만 관리자 명령어 목록을 보여줍니다. (일반 유저에게는 숨김)
    perms = getattr(interaction.user, "guild_permissions", None)
    if perms is not None and perms.administrator:
        embed.add_field(
            name="🛡 관리자 전용",
            value="• `/채팅청소 [수량] [유저]` (수량 입력 후 유저는 선택사항)\n• `/경고지급 [유저] [횟수] [사유]` (횟수에 음수 입력 시 경고 차감)\n• `/코인지급`\n• `/코인회수`\n• `/방어권지급`\n• `/닉네임동기화`\n• `/로그` (명령어를 입력한 채널을 사용 기록 채널로 지정)",
            inline=False,
        )
    await interaction.response.send_message(embed=embed, ephemeral=True)


if __name__ == "__main__":
    token = os.getenv("DISCORD_TOKEN") or os.getenv("DISCORD_BOT_TOKEN")
    if not token:
        print("❌ 에러: DISCORD_TOKEN 환경 변수가 설정되지 않았습니다!")
        raise SystemExit(1)
    bot.run(token)

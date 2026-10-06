import os
import time
import asyncio
from threading import Thread
from datetime import datetime, timezone, timedelta
import random
import discord
from discord import app_commands
from discord.ext import commands, tasks
from flask import Flask
import psycopg2

# 0. 렌더(Render) 24시간 유지용 Flask 웹서버 설정 (가장 먼저 실행)
app = Flask('')

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

# 봇이 켜지기 전에 웹서버와 포트를 확실히 먼저 엽니다.
keep_alive()


VOICE_REWARD_INTERVAL_MINUTES = 30
KST = timezone(timedelta(hours=9)) # 한국 표준시 (UTC+9)

# 외부 클라우드 DB 연결 주소
DATABASE_URL = os.getenv("DATABASE_URL")


# 1. 데이터베이스 연결 함수
def get_db():
    if not DATABASE_URL:
        raise RuntimeError("❌ 에러: DATABASE_URL 환경 변수가 설정되지 않았습니다!")
    return psycopg2.connect(DATABASE_URL, sslmode='require')


# 2. 데이터베이스 초기화 및 테이블 생성 함수
def init_db():
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("""
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
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS voice_sessions (
            guild_id BIGINT,
            user_id BIGINT,
            join_time DOUBLE PRECISION,
            counting_since DOUBLE PRECISION DEFAULT NULL,
            accumulated_seconds DOUBLE PRECISION DEFAULT 0,
            PRIMARY KEY (guild_id, user_id)
        )
    """)
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT DEFAULT NULL")
    cursor.execute("ALTER TABLE voice_sessions ADD COLUMN IF NOT EXISTS counting_since DOUBLE PRECISION DEFAULT NULL")
    cursor.execute("ALTER TABLE voice_sessions ADD COLUMN IF NOT EXISTS accumulated_seconds DOUBLE PRECISION DEFAULT 0")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS guild_settings (
            guild_id BIGINT PRIMARY KEY,
            voice_reward_rate INTEGER DEFAULT 1,
            referral_reward INTEGER NOT NULL DEFAULT 30,
            log_channel_id BIGINT DEFAULT NULL,
            slot_rtp INTEGER DEFAULT 85
        )
    """)

    cursor.execute("""
        ALTER TABLE guild_settings
        ADD COLUMN IF NOT EXISTS slot_rtp INTEGER DEFAULT 85
    """)

    conn.commit()
    cursor.close()
    conn.close()


init_db()

# 인텐트 설정 강화 (Server Members Intent 포함)
intents = discord.Intents.default()
intents.guilds = True
intents.voice_states = True
intents.members = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)
commands_synced = False


@bot.event
async def on_ready():
    global commands_synced
    print(f"로그인 완료: {bot.user}")

    for guild in bot.guilds:
        try:
            await guild.chunk(cache=True)
            print(f"[{guild.name}] 서버 멤버 캐싱 완료")
        except Exception as e:
            print(f"[{guild.name}] 멤버 캐싱 중 오류 발생: {e}")

    if not commands_synced:
        try:
            for guild in bot.guilds:
                bot.tree.clear_commands(guild=guild)
                bot.tree.copy_global_to(guild=guild)
                synced = await bot.tree.sync(guild=guild)
                print(f"[{guild.name}] 서버 명령어 동기화 완료: {len(synced)}개")
            commands_synced = True
        except Exception as e:
            print(f"명령어 동기화 중 오류 발생: {e}")

    # 봇 시작 시점 음성채널 강제 동기화 및 username 즉시 업데이트
    conn = get_db()
    cursor = conn.cursor()
    try:
        for guild in bot.guilds:
            active_voice_user_ids = set()
            for channel in guild.voice_channels:
                for member in channel.members:
                    if member.bot:
                        continue
                    active_voice_user_ids.add(member.id)
                    username = member.name

                    cursor.execute(
                        """
                        INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
                        VALUES (%s, %s, %s, 0, 0, 0, 0)
                        ON CONFLICT (guild_id, user_id) DO UPDATE SET username = EXCLUDED.username
                        """,
                        (guild.id, member.id, username),
                    )

                    now = time.time()
                    is_muted = member.voice.self_mute or member.voice.self_deaf
                    
                    cursor.execute(
                        """
                        INSERT INTO voice_sessions
                            (guild_id, user_id, join_time, counting_since, accumulated_seconds)
                        VALUES (%s, %s, %s, %s, 0)
                        ON CONFLICT (guild_id, user_id) DO NOTHING
                        """,
                        (guild.id, member.id, now, None if is_muted else now),
                    )
            
            cursor.execute(
                "SELECT user_id FROM voice_sessions WHERE guild_id = %s",
                (guild.id,)
            )
            db_sessions = cursor.fetchall()
            for (db_uid,) in db_sessions:
                if db_uid not in active_voice_user_ids:
                    cursor.execute(
                        "DELETE FROM voice_sessions WHERE guild_id = %s AND user_id = %s",
                        (guild.id, db_uid)
                    )

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[음성 세션 복구 오류] {e}")
    finally:
        cursor.close()
        conn.close()

    if not check_voice_time.is_running():
        check_voice_time.start()


@bot.event
async def on_guild_join(guild: discord.Guild):
    try:
        await guild.chunk(cache=True)
    except Exception:
        pass
    bot.tree.clear_commands(guild=guild)
    bot.tree.copy_global_to(guild=guild)
    synced = await bot.tree.sync(guild=guild)
    print(f"[{guild.name}] 서버 명령어 동기화 완료: {len(synced)}개")


# ==========================================
# 🚀 음성 시간 체크 루프 (username 실시간 동기화 포함)
# ==========================================
@tasks.loop(minutes=1)
async def check_voice_time():
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT guild_id, user_id, join_time, counting_since, accumulated_seconds FROM voice_sessions")
        sessions = cur.fetchall()

        for guild_id, user_id, join_time, counting_since, accumulated_seconds in sessions:
            try:
                guild = bot.get_guild(int(guild_id))
                if not guild:
                    continue
                member = guild.get_member(int(user_id))
                if not member:
                    try:
                        member = await guild.fetch_member(int(user_id))
                    except Exception:
                        cur.execute("DELETE FROM voice_sessions WHERE guild_id=%s AND user_id=%s", (guild_id, user_id))
                        conn.commit()
                        continue

                username = member.name
                voice = member.voice
                
                cur.execute("""
                    INSERT INTO users
                    (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
                    VALUES (%s, %s, %s, 0, 0, 0, 0)
                    ON CONFLICT (guild_id, user_id) DO UPDATE SET username = EXCLUDED.username
                """, (guild_id, user_id, username))

                now_ts = datetime.now(timezone.utc).timestamp()
                accumulated = float(accumulated_seconds or 0)
                muted = bool(voice and (voice.self_mute or voice.self_deaf))

                if not voice or not voice.channel:
                    if counting_since is not None:
                        accumulated += max(0, now_ts - float(counting_since))
                    minutes_to_add = int(accumulated // 60)
                    if minutes_to_add > 0:
                        cur.execute("SELECT voice_minutes FROM users WHERE guild_id=%s AND user_id=%s", (guild_id, user_id))
                        row = cur.fetchone()
                        previous = row[0] if row else 0
                        cur.execute("SELECT voice_reward_rate FROM guild_settings WHERE guild_id=%s", (guild_id,))
                        rr = cur.fetchone()
                        rate = rr[0] if rr else 1
                        new_minutes = previous + minutes_to_add
                        crossed = new_minutes // VOICE_REWARD_INTERVAL_MINUTES - previous // VOICE_REWARD_INTERVAL_MINUTES
                        cur.execute("""
                            UPDATE users SET voice_minutes=voice_minutes+%s, coins=coins+%s, username=%s
                            WHERE guild_id=%s AND user_id=%s
                        """, (minutes_to_add, crossed * rate, username, guild_id, user_id))
                    cur.execute("DELETE FROM voice_sessions WHERE guild_id=%s AND user_id=%s", (guild_id, user_id))
                    conn.commit()
                    continue

                if counting_since is None:
                    if not muted:
                        cur.execute("UPDATE voice_sessions SET counting_since=%s WHERE guild_id=%s AND user_id=%s",
                                    (now_ts, guild_id, user_id))
                    conn.commit()
                    continue

                total_seconds = accumulated + max(0, now_ts - float(counting_since))
                minutes_to_add = int(total_seconds // 60)
                remaining = total_seconds - minutes_to_add * 60

                if minutes_to_add <= 0:
                    cur.execute("""
                        UPDATE voice_sessions SET counting_since=%s, accumulated_seconds=%s
                        WHERE guild_id=%s AND user_id=%s
                    """, (None if muted else counting_since, total_seconds, guild_id, user_id))
                    conn.commit()
                    continue

                cur.execute("SELECT voice_minutes FROM users WHERE guild_id=%s AND user_id=%s", (guild_id, user_id))
                row = cur.fetchone()
                previous = row[0] if row else 0
                new_minutes = previous + minutes_to_add
                cur.execute("SELECT voice_reward_rate FROM guild_settings WHERE guild_id=%s", (guild_id,))
                rr = cur.fetchone()
                rate = rr[0] if rr else 1
                crossed = new_minutes // VOICE_REWARD_INTERVAL_MINUTES - previous // VOICE_REWARD_INTERVAL_MINUTES
                
                cur.execute("""
                    UPDATE users SET voice_minutes=voice_minutes+%s, coins=coins+%s, username=%s
                    WHERE guild_id=%s AND user_id=%s
                """, (minutes_to_add, crossed * rate, username, guild_id, user_id))
                
                cur.execute("""
                    UPDATE voice_sessions SET counting_since=%s, accumulated_seconds=%s
                    WHERE guild_id=%s AND user_id=%s
                """, (None if muted else now_ts, remaining, guild_id, user_id))
                conn.commit()

            except Exception as user_error:
                conn.rollback()
                print(f"[VOICE DEBUG] 사용자 처리 오류: guild={guild_id}, user={user_id}, error={user_error}")
                continue
    except Exception as e:
        conn.rollback()
        print(f"[VOICE DEBUG] 전체 음성 체크 오류: {e}")
    finally:
        cur.close()
        conn.close()


@bot.event
async def on_voice_state_update(member, before, after):
    if member.bot:
        return

    guild_id = member.guild.id
    user_id = member.id
    now = time.time()
    username = member.name

    was_connected = before.channel is not None
    is_connected = after.channel is not None
    muted_after = after.self_mute or after.self_deaf

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT join_time, counting_since, COALESCE(accumulated_seconds, 0)
            FROM voice_sessions
            WHERE guild_id = %s AND user_id = %s
            FOR UPDATE
            """,
            (guild_id, user_id),
        )
        row = cursor.fetchone()

        if not is_connected:
            if row:
                join_time, counting_since, accumulated_seconds = row
                if counting_since is not None:
                    accumulated_seconds += max(0, now - counting_since)
                minutes_to_add = int(accumulated_seconds // 60)
                if minutes_to_add > 0:
                    cursor.execute(
                        "SELECT voice_minutes FROM users WHERE guild_id = %s AND user_id = %s",
                        (guild_id, user_id),
                    )
                    user_row = cursor.fetchone()
                    previous_minutes = user_row[0] if user_row else 0
                    new_minutes = previous_minutes + minutes_to_add
                    reward_rate = 1
                    cursor.execute("SELECT voice_reward_rate FROM guild_settings WHERE guild_id = %s", (guild_id,))
                    reward_row = cursor.fetchone()
                    if reward_row:
                        reward_rate = reward_row[0]
                    crossed = (new_minutes // VOICE_REWARD_INTERVAL_MINUTES) - (previous_minutes // VOICE_REWARD_INTERVAL_MINUTES)
                    added_coins = crossed * reward_rate
                    cursor.execute(
                        """
                        INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
                        VALUES (%s, %s, %s, %s, %s, 0, 0)
                        ON CONFLICT (guild_id, user_id) DO UPDATE SET
                            username = EXCLUDED.username,
                            coins = users.coins + EXCLUDED.coins,
                            voice_minutes = users.voice_minutes + EXCLUDED.voice_minutes
                        """,
                        (guild_id, user_id, username, added_coins, minutes_to_add),
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
                        VALUES (%s, %s, %s, 0, 0, 0, 0)
                        ON CONFLICT (guild_id, user_id) DO UPDATE SET username = EXCLUDED.username
                        """,
                        (guild_id, user_id, username),
                    )
                cursor.execute(
                    "DELETE FROM voice_sessions WHERE guild_id = %s AND user_id = %s",
                    (guild_id, user_id),
                )

        elif not was_connected:
            cursor.execute(
                """
                INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
                VALUES (%s, %s, %s, 0, 0, 0, 0)
                ON CONFLICT (guild_id, user_id) DO UPDATE SET username = EXCLUDED.username
                """,
                (guild_id, user_id, username),
            )

            cursor.execute(
                """
                INSERT INTO voice_sessions
                    (guild_id, user_id, join_time, counting_since, accumulated_seconds)
                VALUES (%s, %s, %s, %s, 0)
                ON CONFLICT (guild_id, user_id) DO UPDATE SET
                    join_time = EXCLUDED.join_time,
                    counting_since = EXCLUDED.counting_since,
                    accumulated_seconds = 0
                """,
                (guild_id, user_id, now, None if muted_after else now),
            )

        else:
            cursor.execute(
                """
                INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
                VALUES (%s, %s, %s, 0, 0, 0, 0)
                ON CONFLICT (guild_id, user_id) DO UPDATE SET username = EXCLUDED.username
                """,
                (guild_id, user_id, username),
            )
            
            if row:
                join_time, counting_since, accumulated_seconds = row
                if counting_since is not None:
                    accumulated_seconds += max(0, now - counting_since)

                cursor.execute(
                    """
                    UPDATE voice_sessions
                    SET counting_since = %s, accumulated_seconds = %s
                    WHERE guild_id = %s AND user_id = %s
                    """,
                    (None if muted_after else now, accumulated_seconds, guild_id, user_id),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO voice_sessions
                        (guild_id, user_id, join_time, counting_since, accumulated_seconds)
                    VALUES (%s, %s, %s, %s, 0)
                    """,
                    (guild_id, user_id, now, None if muted_after else now),
                )

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[음성 상태 업데이트 오류] {e}")
    finally:
        cursor.close()
        conn.close()


# ==========================================
# 🧹 채팅 청소 명령어 (관리자 전용 - 순정 purge 방식으로 고속 안정화)
# ==========================================
@bot.tree.command(name="채팅청소", description="특정 유저가 보낸 메시지를 지정한 수량만큼 삭제합니다. (관리자 전용)")
@app_commands.describe(member="청소할 대상 유저", limit="탐색 및 삭제할 최대 메시지 수 (1~100)")
@app_commands.checks.has_permissions(administrator=True)
async def clear_user_chat(interaction: discord.Interaction, member: discord.Member, limit: int = 20):
    await interaction.response.defer(thinking=True, ephemeral=True)

    if limit < 1 or limit > 100:
        await interaction.followup.send("⚠️ 삭제 수량은 1부터 100 사이로 입력해 주세요.", ephemeral=True)
        return

    try:
        # purge 함수를 사용하여 해당 유저가 보낸 메시지만 타겟팅해서 빠르게 일괄 삭제
        def is_target(m: discord.Message):
            return m.author.id == member.id

        deleted = await interaction.channel.purge(limit=limit, check=is_target, bulk=True)

        await interaction.followup.send(
            f"🧹 **{member.display_name}**님의 메시지 **{len(deleted)}개**를 성공적으로 청소했습니다!", 
            ephemeral=True
        )
    except Exception as e:
        await interaction.followup.send(f"❌ 메시지 청소 중 오류가 발생했습니다: {e}", ephemeral=True)

@clear_user_chat.error
async def clear_user_chat_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        if not interaction.response.is_done():
            await interaction.response.send_message("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)
        else:
            await interaction.followup.send("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)
    else:
        if not interaction.response.is_done():
            await interaction.response.send_message(f"❌ 오류 발생: {error}", ephemeral=True)
        else:
            await interaction.followup.send(f"❌ 오류 발생: {error}", ephemeral=True)


# ==========================================
# 🔄 닉네임 일괄 동기화 명령어 (관리자 전용)
# ==========================================
@bot.tree.command(name="닉네임동기화", description="서버 내 모든 멤버의 디스코드 닉네임을 DB에 강제로 일괄 동기화합니다.")
@app_commands.checks.has_permissions(administrator=True)
async def force_sync_usernames(interaction: discord.Interaction):
    guild = interaction.guild
    conn = get_db()
    cursor = conn.cursor()
    updated_count = 0
    try:
        for member in guild.members:
            if member.bot:
                continue
            cursor.execute(
                """
                INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
                VALUES (%s, %s, %s, 0, 0, 0, 0)
                ON CONFLICT (guild_id, user_id) DO UPDATE SET username = EXCLUDED.username
                """,
                (guild.id, member.id, member.name)
            )
            updated_count += 1
        conn.commit()
        await interaction.response.send_message(f"✅ 성공적으로 서버 멤버 {updated_count}명의 닉네임을 DB에 동기화했습니다!", ephemeral=True)
    except Exception as e:
        conn.rollback()
        await interaction.response.send_message(f"❌ 동기화 중 오류 발생: {e}", ephemeral=True)
    finally:
        cursor.close()
        conn.close()

@force_sync_usernames.error
async def force_sync_usernames_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)


# ==========================================
# 🚨 기존 관리자 명령어 (경고, 방어권, 코인 관리 등) 복구 및 통합
# ==========================================
@bot.tree.command(name="경고부여", description="특정 유저에게 경고를 1회 부여합니다. (관리자 전용)")
@app_commands.describe(member="경고를 받을 유저", reason="경고 사유")
@app_commands.checks.has_permissions(administrator=True)
async def add_warning(interaction: discord.Interaction, member: discord.Member, reason: str = "사유 없음"):
    if member.bot:
        await interaction.response.send_message("봇에게는 경고를 부여할 수 없습니다.", ephemeral=True)
        return

    guild_id = interaction.guild_id
    user_id = member.id
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT defense_tickets, warnings FROM users WHERE guild_id = %s AND user_id = %s", (guild_id, user_id))
        row = cursor.fetchone()
        defense_tickets = row[0] if row else 0
        current_warnings = row[1] if row else 0

        if defense_tickets > 0:
            cursor.execute("UPDATE users SET defense_tickets = defense_tickets - 1 WHERE guild_id = %s AND user_id = %s", (guild_id, user_id))
            conn.commit()
            await interaction.response.send_message(
                f"🛡️ **{member.display_name}**님은 방어권을 보유하고 있어 경고를 **방어**했습니다! (남은 방어권: {defense_tickets - 1}개)",
                ephemeral=True
            )
            return

        new_warnings = current_warnings + 1
        cursor.execute(
            """
            INSERT INTO users (guild_id, user_id, username, warnings)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (guild_id, user_id) DO UPDATE SET warnings = users.warnings + 1, username = EXCLUDED.username
            """,
            (guild_id, user_id, member.name, 1)
        )
        conn.commit()

        if new_warnings >= 3:
            try:
                await member.kick(reason="경고 3회 누적")
                kick_msg = " (경고 3회 누적으로 **추방**되었습니다)"
            except Exception:
                kick_msg = " (추방 권한이 없어 추방하지 못했습니다)"
        else:
            kick_msg = ""

        await interaction.response.send_message(
            f"⚠️ **{member.display_name}**님에게 경고를 부여했습니다. (현재 경고: {new_warnings}회){kick_msg}\n- 사유: {reason}",
            ephemeral=True
        )
    except Exception as e:
        conn.rollback()
        await interaction.response.send_message(f"❌ 오류 발생: {e}", ephemeral=True)
    finally:
        cursor.close()
        conn.close()

@add_warning.error
async def add_warning_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)


@bot.tree.command(name="경고차감", description="특정 유저의 경고를 1회 차감합니다. (관리자 전용)")
@app_commands.describe(member="경고를 차감할 유저")
@app_commands.checks.has_permissions(administrator=True)
async def remove_warning(interaction: discord.Interaction, member: discord.Member):
    guild_id = interaction.guild_id
    user_id = member.id
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT warnings FROM users WHERE guild_id = %s AND user_id = %s", (guild_id, user_id))
        row = cursor.fetchone()
        current_warnings = row[0] if row else 0

        if current_warnings <= 0:
            await interaction.response.send_message(f"❌ **{member.display_name}**님의 경고는 이미 0회입니다.", ephemeral=True)
            return

        cursor.execute("UPDATE users SET warnings = warnings - 1 WHERE guild_id = %s AND user_id = %s", (guild_id, user_id))
        conn.commit()
        await interaction.response.send_message(f"✅ **{member.display_name}**님의 경고를 1회 차감했습니다. (현재 경고: {current_warnings - 1}회)", ephemeral=True)
    except Exception as e:
        conn.rollback()
        await interaction.response.send_message(f"❌ 오류 발생: {e}", ephemeral=True)
    finally:
        cursor.close()
        conn.close()

@remove_warning.error
async def remove_warning_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)


@bot.tree.command(name="코인지급", description="특정 유저에게 코인을 지급합니다. (관리자 전용)")
@app_commands.describe(member="코인을 받을 유저", amount="지급할 코인 수량")
@app_commands.checks.has_permissions(administrator=True)
async def give_coins(interaction: discord.Interaction, member: discord.Member, amount: int):
    if amount <= 0:
        await interaction.response.send_message("❌ 지급할 코인 수량은 1 이상이어야 합니다.", ephemeral=True)
        return

    guild_id = interaction.guild_id
    user_id = member.id
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO users (guild_id, user_id, username, coins)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (guild_id, user_id) DO UPDATE SET coins = users.coins + EXCLUDED.coins, username = EXCLUDED.username
            """,
            (guild_id, user_id, member.name, amount)
        )
        conn.commit()
        await interaction.response.send_message(f"✅ **{member.display_name}**님에게 **{amount:,}코인**을 지급했습니다!", ephemeral=True)
    except Exception as e:
        conn.rollback()
        await interaction.response.send_message(f"❌ 오류 발생: {e}", ephemeral=True)
    finally:
        cursor.close()
        conn.close()

@give_coins.error
async def give_coins_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)


@bot.tree.command(name="코인회수", description="특정 유저의 코인을 차감/회수합니다. (관리자 전용)")
@app_commands.describe(member="코인을 회수할 유저", amount="회수할 코인 수량")
@app_commands.checks.has_permissions(administrator=True)
async def take_coins(interaction: discord.Interaction, member: discord.Member, amount: int):
    if amount <= 0:
        await interaction.response.send_message("❌ 회수할 코인 수량은 1 이상이어야 합니다.", ephemeral=True)
        return

    guild_id = interaction.guild_id
    user_id = member.id
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE users SET coins = GREATEST(0, coins - %s)
            WHERE guild_id = %s AND user_id = %s
            RETURNING coins
            """,
            (amount, guild_id, user_id)
        )
        row = cursor.fetchone()
        if not row:
            await interaction.response.send_message("❌ 해당 유저의 데이터를 찾을 수 없습니다.", ephemeral=True)
            return
        conn.commit()
        await interaction.response.send_message(f"✅ **{member.display_name}**님의 코인 중 **{amount:,}코인**을 회수했습니다. (남은 잔액: {row[0]:,}코인)", ephemeral=True)
    except Exception as e:
        conn.rollback()
        await interaction.response.send_message(f"❌ 오류 발생: {e}", ephemeral=True)
    finally:
        cursor.close()
        conn.close()

@take_coins.error
async def take_coins_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)


@bot.tree.command(name="방어권지급", description="특정 유저에게 방어권을 지급합니다. (관리자 전용)")
@app_commands.describe(member="방어권을 받을 유저", amount="지급할 방어권 수량")
@app_commands.checks.has_permissions(administrator=True)
async def give_defense_ticket(interaction: discord.Interaction, member: discord.Member, amount: int = 1):
    if amount <= 0:
        await interaction.response.send_message("❌ 지급할 수량은 1 이상이어야 합니다.", ephemeral=True)
        return

    guild_id = interaction.guild_id
    user_id = member.id
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO users (guild_id, user_id, username, defense_tickets)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (guild_id, user_id) DO UPDATE SET defense_tickets = users.defense_tickets + EXCLUDED.defense_tickets, username = EXCLUDED.username
            """,
            (guild_id, user_id, member.name, amount)
        )
        conn.commit()
        await interaction.response.send_message(f"🛡️ **{member.display_name}**님에게 방어권 **{amount}개**를 지급했습니다!", ephemeral=True)
    except Exception as e:
        conn.rollback()
        await interaction.response.send_message(f"❌ 오류 발생: {e}", ephemeral=True)
    finally:
        cursor.close()
        conn.close()

@give_defense_ticket.error
async def give_defense_ticket_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)


# ==========================================
# 🎰 슬롯머신 및 기타 명령어 기능부
# ==========================================
SLOT_ICONS = ["🍒", "🍋", "🍊", "🔔", "⭐", "💎", "7️⃣"]

SLOT_OUTCOMES = (
    ("jackpot", 3.0),
    ("double", 1.5),
    ("pair", 0.5),
    ("lose", 0.0),
)

active_slot_spins = set()
SLOT_CONCURRENCY_LIMIT = 50
slot_semaphore = asyncio.Semaphore(SLOT_CONCURRENCY_LIMIT)


def roll_slot_result(rtp_percent: int):
    rtp = max(0, min(150, int(rtp_percent))) / 100.0
    winning_average_multiplier = sum(multiplier for _, multiplier in SLOT_OUTCOMES[:3]) / 3.0
    total_win_probability = min(1.0, rtp / winning_average_multiplier)

    if random.random() < total_win_probability:
        outcome_name, multiplier = random.choice(SLOT_OUTCOMES[:3])

        if outcome_name == "jackpot":
            icon = random.choice(SLOT_ICONS)
            result_icons = [icon, icon, icon]
        elif outcome_name == "double":
            icon = random.choice(SLOT_ICONS)
            other = random.choice([i for i in SLOT_ICONS if i != icon])
            result_icons = [icon, icon, other]
            random.shuffle(result_icons)
        else:
            icon = random.choice(SLOT_ICONS)
            other_icons = [i for i in SLOT_ICONS if i != icon]
            result_icons = [icon, icon, random.choice(other_icons)]
            random.shuffle(result_icons)

        return result_icons, multiplier

    result_icons = random.sample(SLOT_ICONS, 3)
    return result_icons, 0.0


def get_slot_rtp(guild_id: int) -> int:
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT slot_rtp FROM guild_settings WHERE guild_id = %s",
            (guild_id,),
        )
        row = cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else 85
    finally:
        cursor.close()
        conn.close()


def deduct_slot_bet(guild_id: int, user_id: int, bet_amount: int) -> bool:
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO users (
                guild_id, user_id, coins, voice_minutes,
                warnings, defense_tickets
            )
            VALUES (%s, %s, 0, 0, 0, 0)
            ON CONFLICT (guild_id, user_id) DO NOTHING
            """,
            (guild_id, user_id),
        )

        cursor.execute(
            """
            UPDATE users
            SET coins = coins - %s
            WHERE guild_id = %s
              AND user_id = %s
              AND coins >= %s
            RETURNING coins
            """,
            (bet_amount, guild_id, user_id, bet_amount),
        )
        row = cursor.fetchone()

        if row is None:
            conn.rollback()
            return False

        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def add_slot_payout(guild_id: int, user_id: int, payout: int) -> int:
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE users
            SET coins = coins + %s
            WHERE guild_id = %s AND user_id = %s
            RETURNING coins
            """,
            (payout, guild_id, user_id),
        )
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError("슬롯머신 당첨금 지급 대상 사용자를 찾을 수 없습니다.")
        conn.commit()
        return int(row[0])
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def get_current_coins(guild_id: int, user_id: int) -> int:
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT coins FROM users WHERE guild_id = %s AND user_id = %s",
            (guild_id, user_id),
        )
        row = cursor.fetchone()
        return int(row[0]) if row else 0
    finally:
        cursor.close()
        conn.close()


class SlotMachineView(discord.ui.View):
    def __init__(self, author_id: int, bet_amount: int):
        super().__init__(timeout=30)
        self.author_id = author_id
        self.bet_amount = bet_amount

    @discord.ui.button(label="🎰 다시 돌리기", style=discord.ButtonStyle.success)
    async def spin_again(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "본인이 실행한 슬롯머신만 다시 돌릴 수 있습니다.",
                ephemeral=True,
            )
            return

        if interaction.guild_id is None:
            await interaction.response.send_message(
                "슬롯머신은 서버에서만 사용할 수 있습니다.",
                ephemeral=True,
            )
            return

        spin_key = (interaction.guild_id, self.author_id)
        if spin_key in active_slot_spins:
            await interaction.response.send_message(
                "⏳ 이미 슬롯머신이 돌아가고 있습니다. 잠시만 기다려주세요.",
                ephemeral=True,
            )
            return

        active_slot_spins.add(spin_key)
        button.disabled = True

        try:
            guild_id = interaction.guild_id
            user_id = interaction.user.id

            await interaction.response.edit_message(
                content="🎰 **슬롯머신이 돌아가는 중입니다...**\n` 🔄 | 🔄 | 🔄 `",
                view=None,
            )
            msg = interaction.message

            async with slot_semaphore:
                if not deduct_slot_bet(guild_id, user_id, self.bet_amount):
                    current_coins = get_current_coins(guild_id, user_id)
                    await msg.edit(
                        content=(
                            "❌ 코인이 부족합니다! "
                            f"(현재 잔액: {current_coins:,}코인)"
                        ),
                        view=None,
                    )
                    return

                rtp = get_slot_rtp(guild_id)
                await asyncio.sleep(0.6)
                result_icons, multiplier = roll_slot_result(rtp)
                payout = int(self.bet_amount * multiplier)

                if payout > 0:
                    final_coins = add_slot_payout(guild_id, user_id, payout)
                else:
                    final_coins = get_current_coins(guild_id, user_id)

                if multiplier >= 3.0:
                    result_text = (
                        "🎉 **[잭팟 당첨! 3배 승리!]** "
                        f"배팅액의 3배인 **+{payout:,}코인**을 획득하셨습니다!"
                    )
                elif multiplier > 0:
                    result_text = f"✨ **[당첨!]** 배팅액의 **{multiplier:g}배**인 **+{payout:,}코인**을 획득하셨습니다!"
                else:
                    result_text = "😢 **[꽝]** 배팅액의 **0배**입니다. 아쉽게도 꽝입니다. 다음 기회에 도전해보세요!"

                final_view = SlotMachineView(self.author_id, self.bet_amount)
                await msg.edit(
                    content=(
                        "🎰 **[슬롯머신 결과]**\n"
                        f"` {result_icons[0]} | {result_icons[1]} | {result_icons[2]} `\n\n"
                        f"{result_text}\n"
                        f"💰 현재 잔액: **{final_coins:,}코인**"
                    ),
                    view=final_view,
                )

        except Exception as e:
            print(f"[슬롯머신 버튼 오류] {type(e).__name__}: {e}")
        finally:
            active_slot_spins.discard(spin_key)


@bot.tree.command(
    name="정보", description="본인 또는 선택한 사용자의 코인, 음성 접속 시간, 경고 횟수, 방어권을 확인합니다."
)
@app_commands.describe(member="조회할 사용자 (선택하지 않으면 본인)")
async def my_info(
    interaction: discord.Interaction, member: discord.Member = None
):
    target = member or interaction.user
    guild_id = interaction.guild_id
    user_id = target.id

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT coins, voice_minutes, warnings, defense_tickets FROM users WHERE guild_id = %s AND user_id = %s",
        (guild_id, user_id),
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    coins = row[0] if row else 0
    minutes = row[1] if row else 0
    warnings = row[2] if row else 0
    defense_tickets = row[3] if row else 0

    await interaction.response.send_message(
        f"**{target.mention}**님의 서버 활동 정보:\n"
        f"- 🪙 대깨 코인: **{coins:,}개**\n"
        f"- ⌛ 음성 접속 시간: **{minutes}분**\n"
        f"- ⚠️ 경고 횟수: **{warnings}회** (3회 누적 시 차단)\n"
        f"- 🛡 방어권: **{defense_tickets}개**",
        ephemeral=True,
    )


@bot.tree.command(
    name="추천인",
    description="나를 초대해준 사람을 추천인으로 등록합니다. (누적 음성 30분 이상 시 가능)",
)
@app_commands.describe(referrer="추천할 유저를 선택하세요")
async def register_referral(interaction: discord.Interaction, referrer: discord.Member):
    if referrer.id == interaction.user.id:
        await interaction.response.send_message(
            "자기 자신을 추천인으로 등록할 수 없습니다.", ephemeral=True
        )
        return
    if referrer.bot:
        await interaction.response.send_message(
            "봇은 추천인으로 등록할 수 없습니다.", ephemeral=True
        )
        return

    guild_id = interaction.guild_id
    user_id = interaction.user.id

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT voice_minutes, referred_by FROM users WHERE guild_id = %s AND user_id = %s",
        (guild_id, user_id),
    )
    row = cursor.fetchone()

    if row and row[1] is not None:
        cursor.close()
        conn.close()
        await interaction.response.send_message(
            "이미 추천인을 등록하셨습니다.", ephemeral=True
        )
        return

    user_minutes = row[0] if row else 0
    if user_minutes < 30:
        cursor.close()
        conn.close()
        await interaction.response.send_message(
            f"❌ 음성 접속 시간이 **30분 이상**일 때만 추천인 등록이 가능합니다. (현재: {user_minutes}분)",
            ephemeral=True,
        )
        return

    cursor.execute(
        "SELECT referral_reward FROM guild_settings WHERE guild_id = %s",
        (guild_id,),
    )
    setting = cursor.fetchone()
    referral_reward = setting[0] if setting else 30

    cursor.execute(
        """
        INSERT INTO users (guild_id, user_id, coins, voice_minutes, referred_by)
        VALUES (%s, %s, 0, %s, %s)
        ON CONFLICT (guild_id, user_id) DO UPDATE SET referred_by = EXCLUDED.referred_by
    """,
        (guild_id, user_id, user_minutes, referrer.id),
    )

    cursor.execute(
        """
        INSERT INTO users (guild_id, user_id, coins, voice_minutes)
        VALUES (%s, %s, %s, 0)
        ON CONFLICT (guild_id, user_id) DO UPDATE SET coins = users.coins + EXCLUDED.coins
    """,
        (guild_id, referrer.id, referral_reward),
    )

    conn.commit()
    cursor.close()
    conn.close()

    await interaction.response.send_message(
        f"✅ 성공적으로 {referrer.mention}님을 추천인으로 등록했습니다! 추천인에게 **{referral_reward}코인**이 지급되었습니다.",
        ephemeral=True,
    )


@bot.tree.command(
    name="슬롯머신",
    description="코인을 걸고 슬롯머신을 돌립니다. (최대 500코인, 최대 3배 배율)",
)
@app_commands.describe(bet="배팅할 코인 수량 (1 ~ 500코인)")
async def slot_machine(interaction: discord.Interaction, bet: int):
    if interaction.guild_id is None:
        await interaction.response.send_message(
            "❌ 슬롯머신은 서버에서만 사용할 수 있습니다.",
            ephemeral=True,
        )
        return

    if bet < 1 or bet > 500:
        await interaction.response.send_message(
            "❌ 배팅 코인은 **1 ~ 500코인** 사이여야 합니다.",
            ephemeral=True,
        )
        return

    user_id = interaction.user.id
    guild_id = interaction.guild_id
    spin_key = (guild_id, user_id)

    if spin_key in active_slot_spins:
        await interaction.response.send_message(
            "⏳ 이미 슬롯머신이 돌아가고 있습니다. 잠시만 기다려주세요.",
            ephemeral=True,
        )
        return

    active_slot_spins.add(spin_key)

    try:
        await interaction.response.send_message(
            "🎰 **슬롯머신이 돌아가는 중입니다...**\n` 🔄 | 🔄 | 🔄 `"
        )
        msg = await interaction.original_response()

        async with slot_semaphore:
            if not deduct_slot_bet(guild_id, user_id, bet):
                current_coins = get_current_coins(guild_id, user_id)
                await msg.edit(
                    content=(
                        "❌ 코인이 부족합니다! "
                        f"(현재 잔액: {current_coins:,}코인)"
                    ),
                    view=None,
                )
                return

            rtp = get_slot_rtp(guild_id)
            await asyncio.sleep(0.6)
            result_icons, multiplier = roll_slot_result(rtp)
            payout = int(bet * multiplier)

            if payout > 0:
                final_coins = add_slot_payout(guild_id, user_id, payout)
            else:
                final_coins = get_current_coins(guild_id, user_id)

            if multiplier >= 3.0:
                result_text = f"🎉 **[잭팟 당첨! 3배 승리!]** 배팅액의 3배인 **+{payout:,}코인**을 획득하셨습니다!"
            elif multiplier > 0:
                result_text = f"✨ **[당첨!]** 배팅액의 **{multiplier:g}배**인 **+{payout:,}코인**을 획득하셨습니다!"
            else:
                result_text = "😢 **[꽝]** 아쉽게도 꽝입니다."

            view = SlotMachineView(user_id, bet)
            await msg.edit(
                content=(
                    "🎰 **[슬롯머신 결과]**\n"
                    f"` {result_icons[0]} | {result_icons[1]} | {result_icons[2]} `\n\n"
                    f"{result_text}\n"
                    f"💰 현재 잔액: **{final_coins:,}코인**"
                ),
                view=view,
            )

    except Exception as e:
        print(f"[슬롯머신 오류] {e}")
    finally:
        active_slot_spins.discard(spin_key)


@bot.tree.command(
    name="코인순위", description="이 서버에서 코인이 많은 상위 10명을 확인합니다."
)
async def coin_ranking(interaction: discord.Interaction):
    guild_id = interaction.guild_id
    if guild_id is None:
        await interaction.response.send_message("서버 안에서만 사용할 수 있습니다.", ephemeral=True)
        return

    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT user_id, coins
            FROM users
            WHERE guild_id = %s AND coins > 0
            ORDER BY coins DESC, user_id ASC
            LIMIT 10
            """,
            (guild_id,),
        )
        rows = cursor.fetchall()
        cursor.close()
    finally:
        conn.close()

    if rows:
        medals = {1: "🥇", 2: "🥈", 3: "🥉"}
        ranking_lines = []
        for rank, (user_id, coins) in enumerate(rows, start=1):
            rank_label = medals.get(rank, f"{rank}.")
            ranking_lines.append(f"{rank_label} <@{user_id}> — **{coins:,}코인**")
        description = "\n".join(ranking_lines)
    else:
        description = "아직 코인을 보유한 사용자가 없습니다."

    embed = discord.Embed(
        title=f"🏆 {interaction.guild.name} 코인 순위",
        description=description,
        color=discord.Color.gold(),
    )
    embed.set_footer(text="코인 보유량 상위 10명")
    await interaction.response.send_message(
        embed=embed,
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )


@bot.tree.command(
    name="명령어", description="봇이 사용할 수 있는 명령어 목록을 확인합니다."
)
async def show_commands(interaction: discord.Interaction):
    embed = discord.Embed(
        title="🤖 봇 명령어 안내",
        description="이 서버에서 사용할 수 있는 명령어입니다.",
        color=discord.Color.blue(),
    )
    embed.add_field(
        name="👤 일반 사용자용 명령어",
        value=(
            "• `/정보 [유저]` — 코인, 음성 시간, 경고, 방어권 확인\n"
            "• `/추천인 [유저]` — 추천인 등록 (음성 30분 이상)\n"
            "• `/슬롯머신 [배팅액]` — 슬롯머신 미니게임 (최대 500코인)\n"
            "• `/코인순위` — 코인 상위 10명 확인\n"
            "• `/명령어` — 명령어 안내"
        ),
        inline=False,
    )
    embed.add_field(
        name="🛡 관리자 전용 명령어",
        value=(
            "• `/경고부여 [유저] [사유]` — 유저 경고 1회 추가 (3회 누적 시 추방)\n"
            "• `/경고차감 [유저]` — 유저 경고 1회 차감\n"
            "• `/코인지급 [유저] [수량]` — 유저에게 코인 지급\n"
            "• `/코인회수 [유저] [수량]` — 유저의 코인 회수\n"
            "• `/방어권지급 [유저] [수량]` — 유저에게 방어권 지급\n"
            "• `/채팅청소 [유저] [수량]` — 특정 유저 메시지 삭제\n"
            "• `/닉네임동기화` — 서버 멤버 전체 닉네임 DB 일괄 동기화"
        ),
        inline=False,
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


if __name__ == "__main__":
    token = os.getenv("DISCORD_TOKEN") or os.getenv("DISCORD_BOT_TOKEN")
    
    if not token:
        print("❌ 에러: DISCORD_TOKEN 환경 변수가 설정되지 않았습니다!")
        exit(1)
        
bot.run(token)

import os
import time
import asyncio
from threading import Thread
from datetime import datetime, timezone, timedelta
import random
import discord
from discord import app_commands
from discord.ext import commands, tasks
from flask import Flask
import psycopg2

# 0. 렌더(Render) 24시간 유지용 Flask 웹서버 설정 (가장 먼저 실행)
app = Flask('')

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

# 봇이 켜지기 전에 웹서버와 포트를 확실히 먼저 엽니다.
keep_alive()


VOICE_REWARD_INTERVAL_MINUTES = 30
KST = timezone(timedelta(hours=9)) # 한국 표준시 (UTC+9)

# 외부 클라우드 DB 연결 주소
DATABASE_URL = os.getenv("DATABASE_URL")


# 1. 데이터베이스 연결 함수
def get_db():
    if not DATABASE_URL:
        raise RuntimeError("❌ 에러: DATABASE_URL 환경 변수가 설정되지 않았습니다!")
    return psycopg2.connect(DATABASE_URL, sslmode='require')


# 2. 데이터베이스 초기화 및 테이블 생성 함수
def init_db():
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("""
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
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS voice_sessions (
            guild_id BIGINT,
            user_id BIGINT,
            join_time DOUBLE PRECISION,
            counting_since DOUBLE PRECISION DEFAULT NULL,
            accumulated_seconds DOUBLE PRECISION DEFAULT 0,
            PRIMARY KEY (guild_id, user_id)
        )
    """)
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT DEFAULT NULL")
    cursor.execute("ALTER TABLE voice_sessions ADD COLUMN IF NOT EXISTS counting_since DOUBLE PRECISION DEFAULT NULL")
    cursor.execute("ALTER TABLE voice_sessions ADD COLUMN IF NOT EXISTS accumulated_seconds DOUBLE PRECISION DEFAULT 0")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS guild_settings (
            guild_id BIGINT PRIMARY KEY,
            voice_reward_rate INTEGER DEFAULT 1,
            referral_reward INTEGER NOT NULL DEFAULT 30,
            log_channel_id BIGINT DEFAULT NULL,
            slot_rtp INTEGER DEFAULT 85
        )
    """)

    cursor.execute("""
        ALTER TABLE guild_settings
        ADD COLUMN IF NOT EXISTS slot_rtp INTEGER DEFAULT 85
    """)

    conn.commit()
    cursor.close()
    conn.close()


init_db()

# 인텐트 설정 강화 (Server Members Intent 포함)
intents = discord.Intents.default()
intents.guilds = True
intents.voice_states = True
intents.members = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)
commands_synced = False


@bot.event
async def on_ready():
    global commands_synced
    print(f"로그인 완료: {bot.user}")

    for guild in bot.guilds:
        try:
            await guild.chunk(cache=True)
            print(f"[{guild.name}] 서버 멤버 캐싱 완료")
        except Exception as e:
            print(f"[{guild.name}] 멤버 캐싱 중 오류 발생: {e}")

    if not commands_synced:
        try:
            for guild in bot.guilds:
                bot.tree.clear_commands(guild=guild)
                bot.tree.copy_global_to(guild=guild)
                synced = await bot.tree.sync(guild=guild)
                print(f"[{guild.name}] 서버 명령어 동기화 완료: {len(synced)}개")
            commands_synced = True
        except Exception as e:
            print(f"명령어 동기화 중 오류 발생: {e}")

    # 봇 시작 시점 음성채널 강제 동기화 및 username 즉시 업데이트
    conn = get_db()
    cursor = conn.cursor()
    try:
        for guild in bot.guilds:
            active_voice_user_ids = set()
            for channel in guild.voice_channels:
                for member in channel.members:
                    if member.bot:
                        continue
                    active_voice_user_ids.add(member.id)
                    username = member.name

                    cursor.execute(
                        """
                        INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
                        VALUES (%s, %s, %s, 0, 0, 0, 0)
                        ON CONFLICT (guild_id, user_id) DO UPDATE SET username = EXCLUDED.username
                        """,
                        (guild.id, member.id, username),
                    )

                    now = time.time()
                    is_muted = member.voice.self_mute or member.voice.self_deaf
                    
                    cursor.execute(
                        """
                        INSERT INTO voice_sessions
                            (guild_id, user_id, join_time, counting_since, accumulated_seconds)
                        VALUES (%s, %s, %s, %s, 0)
                        ON CONFLICT (guild_id, user_id) DO NOTHING
                        """,
                        (guild.id, member.id, now, None if is_muted else now),
                    )
            
            cursor.execute(
                "SELECT user_id FROM voice_sessions WHERE guild_id = %s",
                (guild.id,)
            )
            db_sessions = cursor.fetchall()
            for (db_uid,) in db_sessions:
                if db_uid not in active_voice_user_ids:
                    cursor.execute(
                        "DELETE FROM voice_sessions WHERE guild_id = %s AND user_id = %s",
                        (guild.id, db_uid)
                    )

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[음성 세션 복구 오류] {e}")
    finally:
        cursor.close()
        conn.close()

    if not check_voice_time.is_running():
        check_voice_time.start()


@bot.event
async def on_guild_join(guild: discord.Guild):
    try:
        await guild.chunk(cache=True)
    except Exception:
        pass
    bot.tree.clear_commands(guild=guild)
    bot.tree.copy_global_to(guild=guild)
    synced = await bot.tree.sync(guild=guild)
    print(f"[{guild.name}] 서버 명령어 동기화 완료: {len(synced)}개")


# ==========================================
# 🚀 음성 시간 체크 루프 (username 실시간 동기화 포함)
# ==========================================
@tasks.loop(minutes=1)
async def check_voice_time():
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT guild_id, user_id, join_time, counting_since, accumulated_seconds FROM voice_sessions")
        sessions = cur.fetchall()

        for guild_id, user_id, join_time, counting_since, accumulated_seconds in sessions:
            try:
                guild = bot.get_guild(int(guild_id))
                if not guild:
                    continue
                member = guild.get_member(int(user_id))
                if not member:
                    try:
                        member = await guild.fetch_member(int(user_id))
                    except Exception:
                        cur.execute("DELETE FROM voice_sessions WHERE guild_id=%s AND user_id=%s", (guild_id, user_id))
                        conn.commit()
                        continue

                username = member.name
                voice = member.voice
                
                cur.execute("""
                    INSERT INTO users
                    (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
                    VALUES (%s, %s, %s, 0, 0, 0, 0)
                    ON CONFLICT (guild_id, user_id) DO UPDATE SET username = EXCLUDED.username
                """, (guild_id, user_id, username))

                now_ts = datetime.now(timezone.utc).timestamp()
                accumulated = float(accumulated_seconds or 0)
                muted = bool(voice and (voice.self_mute or voice.self_deaf))

                if not voice or not voice.channel:
                    if counting_since is not None:
                        accumulated += max(0, now_ts - float(counting_since))
                    minutes_to_add = int(accumulated // 60)
                    if minutes_to_add > 0:
                        cur.execute("SELECT voice_minutes FROM users WHERE guild_id=%s AND user_id=%s", (guild_id, user_id))
                        row = cur.fetchone()
                        previous = row[0] if row else 0
                        cur.execute("SELECT voice_reward_rate FROM guild_settings WHERE guild_id=%s", (guild_id,))
                        rr = cur.fetchone()
                        rate = rr[0] if rr else 1
                        new_minutes = previous + minutes_to_add
                        crossed = new_minutes // VOICE_REWARD_INTERVAL_MINUTES - previous // VOICE_REWARD_INTERVAL_MINUTES
                        cur.execute("""
                            UPDATE users SET voice_minutes=voice_minutes+%s, coins=coins+%s, username=%s
                            WHERE guild_id=%s AND user_id=%s
                        """, (minutes_to_add, crossed * rate, username, guild_id, user_id))
                    cur.execute("DELETE FROM voice_sessions WHERE guild_id=%s AND user_id=%s", (guild_id, user_id))
                    conn.commit()
                    continue

                if counting_since is None:
                    if not muted:
                        cur.execute("UPDATE voice_sessions SET counting_since=%s WHERE guild_id=%s AND user_id=%s",
                                    (now_ts, guild_id, user_id))
                    conn.commit()
                    continue

                total_seconds = accumulated + max(0, now_ts - float(counting_since))
                minutes_to_add = int(total_seconds // 60)
                remaining = total_seconds - minutes_to_add * 60

                if minutes_to_add <= 0:
                    cur.execute("""
                        UPDATE voice_sessions SET counting_since=%s, accumulated_seconds=%s
                        WHERE guild_id=%s AND user_id=%s
                    """, (None if muted else counting_since, total_seconds, guild_id, user_id))
                    conn.commit()
                    continue

                cur.execute("SELECT voice_minutes FROM users WHERE guild_id=%s AND user_id=%s", (guild_id, user_id))
                row = cur.fetchone()
                previous = row[0] if row else 0
                new_minutes = previous + minutes_to_add
                cur.execute("SELECT voice_reward_rate FROM guild_settings WHERE guild_id=%s", (guild_id,))
                rr = cur.fetchone()
                rate = rr[0] if rr else 1
                crossed = new_minutes // VOICE_REWARD_INTERVAL_MINUTES - previous // VOICE_REWARD_INTERVAL_MINUTES
                
                cur.execute("""
                    UPDATE users SET voice_minutes=voice_minutes+%s, coins=coins+%s, username=%s
                    WHERE guild_id=%s AND user_id=%s
                """, (minutes_to_add, crossed * rate, username, guild_id, user_id))
                
                cur.execute("""
                    UPDATE voice_sessions SET counting_since=%s, accumulated_seconds=%s
                    WHERE guild_id=%s AND user_id=%s
                """, (None if muted else now_ts, remaining, guild_id, user_id))
                conn.commit()

            except Exception as user_error:
                conn.rollback()
                print(f"[VOICE DEBUG] 사용자 처리 오류: guild={guild_id}, user={user_id}, error={user_error}")
                continue
    except Exception as e:
        conn.rollback()
        print(f"[VOICE DEBUG] 전체 음성 체크 오류: {e}")
    finally:
        cur.close()
        conn.close()


@bot.event
async def on_voice_state_update(member, before, after):
    if member.bot:
        return

    guild_id = member.guild.id
    user_id = member.id
    now = time.time()
    username = member.name

    was_connected = before.channel is not None
    is_connected = after.channel is not None
    muted_after = after.self_mute or after.self_deaf

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT join_time, counting_since, COALESCE(accumulated_seconds, 0)
            FROM voice_sessions
            WHERE guild_id = %s AND user_id = %s
            FOR UPDATE
            """,
            (guild_id, user_id),
        )
        row = cursor.fetchone()

        if not is_connected:
            if row:
                join_time, counting_since, accumulated_seconds = row
                if counting_since is not None:
                    accumulated_seconds += max(0, now - counting_since)
                minutes_to_add = int(accumulated_seconds // 60)
                if minutes_to_add > 0:
                    cursor.execute(
                        "SELECT voice_minutes FROM users WHERE guild_id = %s AND user_id = %s",
                        (guild_id, user_id),
                    )
                    user_row = cursor.fetchone()
                    previous_minutes = user_row[0] if user_row else 0
                    new_minutes = previous_minutes + minutes_to_add
                    reward_rate = 1
                    cursor.execute("SELECT voice_reward_rate FROM guild_settings WHERE guild_id = %s", (guild_id,))
                    reward_row = cursor.fetchone()
                    if reward_row:
                        reward_rate = reward_row[0]
                    crossed = (new_minutes // VOICE_REWARD_INTERVAL_MINUTES) - (previous_minutes // VOICE_REWARD_INTERVAL_MINUTES)
                    added_coins = crossed * reward_rate
                    cursor.execute(
                        """
                        INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
                        VALUES (%s, %s, %s, %s, %s, 0, 0)
                        ON CONFLICT (guild_id, user_id) DO UPDATE SET
                            username = EXCLUDED.username,
                            coins = users.coins + EXCLUDED.coins,
                            voice_minutes = users.voice_minutes + EXCLUDED.voice_minutes
                        """,
                        (guild_id, user_id, username, added_coins, minutes_to_add),
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
                        VALUES (%s, %s, %s, 0, 0, 0, 0)
                        ON CONFLICT (guild_id, user_id) DO UPDATE SET username = EXCLUDED.username
                        """,
                        (guild_id, user_id, username),
                    )
                cursor.execute(
                    "DELETE FROM voice_sessions WHERE guild_id = %s AND user_id = %s",
                    (guild_id, user_id),
                )

        elif not was_connected:
            cursor.execute(
                """
                INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
                VALUES (%s, %s, %s, 0, 0, 0, 0)
                ON CONFLICT (guild_id, user_id) DO UPDATE SET username = EXCLUDED.username
                """,
                (guild_id, user_id, username),
            )

            cursor.execute(
                """
                INSERT INTO voice_sessions
                    (guild_id, user_id, join_time, counting_since, accumulated_seconds)
                VALUES (%s, %s, %s, %s, 0)
                ON CONFLICT (guild_id, user_id) DO UPDATE SET
                    join_time = EXCLUDED.join_time,
                    counting_since = EXCLUDED.counting_since,
                    accumulated_seconds = 0
                """,
                (guild_id, user_id, now, None if muted_after else now),
            )

        else:
            cursor.execute(
                """
                INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
                VALUES (%s, %s, %s, 0, 0, 0, 0)
                ON CONFLICT (guild_id, user_id) DO UPDATE SET username = EXCLUDED.username
                """,
                (guild_id, user_id, username),
            )
            
            if row:
                join_time, counting_since, accumulated_seconds = row
                if counting_since is not None:
                    accumulated_seconds += max(0, now - counting_since)

                cursor.execute(
                    """
                    UPDATE voice_sessions
                    SET counting_since = %s, accumulated_seconds = %s
                    WHERE guild_id = %s AND user_id = %s
                    """,
                    (None if muted_after else now, accumulated_seconds, guild_id, user_id),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO voice_sessions
                        (guild_id, user_id, join_time, counting_since, accumulated_seconds)
                    VALUES (%s, %s, %s, %s, 0)
                    """,
                    (guild_id, user_id, now, None if muted_after else now),
                )

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[음성 상태 업데이트 오류] {e}")
    finally:
        cursor.close()
        conn.close()


# ==========================================
# 🧹 채팅 청소 명령어 (관리자 전용 - 순정 purge 방식으로 고속 안정화)
# ==========================================
@bot.tree.command(name="채팅청소", description="특정 유저가 보낸 메시지를 지정한 수량만큼 삭제합니다. (관리자 전용)")
@app_commands.describe(member="청소할 대상 유저", limit="탐색 및 삭제할 최대 메시지 수 (1~100)")
@app_commands.checks.has_permissions(administrator=True)
async def clear_user_chat(interaction: discord.Interaction, member: discord.Member, limit: int = 20):
    await interaction.response.defer(thinking=True, ephemeral=True)

    if limit < 1 or limit > 100:
        await interaction.followup.send("⚠️ 삭제 수량은 1부터 100 사이로 입력해 주세요.", ephemeral=True)
        return

    try:
        # purge 함수를 사용하여 해당 유저가 보낸 메시지만 타겟팅해서 빠르게 일괄 삭제
        def is_target(m: discord.Message):
            return m.author.id == member.id

        deleted = await interaction.channel.purge(limit=limit, check=is_target, bulk=True)

        await interaction.followup.send(
            f"🧹 **{member.display_name}**님의 메시지 **{len(deleted)}개**를 성공적으로 청소했습니다!", 
            ephemeral=True
        )
    except Exception as e:
        await interaction.followup.send(f"❌ 메시지 청소 중 오류가 발생했습니다: {e}", ephemeral=True)

@clear_user_chat.error
async def clear_user_chat_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        if not interaction.response.is_done():
            await interaction.response.send_message("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)
        else:
            await interaction.followup.send("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)
    else:
        if not interaction.response.is_done():
            await interaction.response.send_message(f"❌ 오류 발생: {error}", ephemeral=True)
        else:
            await interaction.followup.send(f"❌ 오류 발생: {error}", ephemeral=True)


# ==========================================
# 🔄 닉네임 일괄 동기화 명령어 (관리자 전용)
# ==========================================
@bot.tree.command(name="닉네임동기화", description="서버 내 모든 멤버의 디스코드 닉네임을 DB에 강제로 일괄 동기화합니다.")
@app_commands.checks.has_permissions(administrator=True)
async def force_sync_usernames(interaction: discord.Interaction):
    guild = interaction.guild
    conn = get_db()
    cursor = conn.cursor()
    updated_count = 0
    try:
        for member in guild.members:
            if member.bot:
                continue
            cursor.execute(
                """
                INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
                VALUES (%s, %s, %s, 0, 0, 0, 0)
                ON CONFLICT (guild_id, user_id) DO UPDATE SET username = EXCLUDED.username
                """,
                (guild.id, member.id, member.name)
            )
            updated_count += 1
        conn.commit()
        await interaction.response.send_message(f"✅ 성공적으로 서버 멤버 {updated_count}명의 닉네임을 DB에 동기화했습니다!", ephemeral=True)
    except Exception as e:
        conn.rollback()
        await interaction.response.send_message(f"❌ 동기화 중 오류 발생: {e}", ephemeral=True)
    finally:
        cursor.close()
        conn.close()

@force_sync_usernames.error
async def force_sync_usernames_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)


# ==========================================
# 🚨 기존 관리자 명령어 (경고, 방어권, 코인 관리 등) 복구 및 통합
# ==========================================
@bot.tree.command(name="경고부여", description="특정 유저에게 경고를 1회 부여합니다. (관리자 전용)")
@app_commands.describe(member="경고를 받을 유저", reason="경고 사유")
@app_commands.checks.has_permissions(administrator=True)
async def add_warning(interaction: discord.Interaction, member: discord.Member, reason: str = "사유 없음"):
    if member.bot:
        await interaction.response.send_message("봇에게는 경고를 부여할 수 없습니다.", ephemeral=True)
        return

    guild_id = interaction.guild_id
    user_id = member.id
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT defense_tickets, warnings FROM users WHERE guild_id = %s AND user_id = %s", (guild_id, user_id))
        row = cursor.fetchone()
        defense_tickets = row[0] if row else 0
        current_warnings = row[1] if row else 0

        if defense_tickets > 0:
            cursor.execute("UPDATE users SET defense_tickets = defense_tickets - 1 WHERE guild_id = %s AND user_id = %s", (guild_id, user_id))
            conn.commit()
            await interaction.response.send_message(
                f"🛡️ **{member.display_name}**님은 방어권을 보유하고 있어 경고를 **방어**했습니다! (남은 방어권: {defense_tickets - 1}개)",
                ephemeral=True
            )
            return

        new_warnings = current_warnings + 1
        cursor.execute(
            """
            INSERT INTO users (guild_id, user_id, username, warnings)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (guild_id, user_id) DO UPDATE SET warnings = users.warnings + 1, username = EXCLUDED.username
            """,
            (guild_id, user_id, member.name, 1)
        )
        conn.commit()

        if new_warnings >= 3:
            try:
                await member.kick(reason="경고 3회 누적")
                kick_msg = " (경고 3회 누적으로 **추방**되었습니다)"
            except Exception:
                kick_msg = " (추방 권한이 없어 추방하지 못했습니다)"
        else:
            kick_msg = ""

        await interaction.response.send_message(
            f"⚠️ **{member.display_name}**님에게 경고를 부여했습니다. (현재 경고: {new_warnings}회){kick_msg}\n- 사유: {reason}",
            ephemeral=True
        )
    except Exception as e:
        conn.rollback()
        await interaction.response.send_message(f"❌ 오류 발생: {e}", ephemeral=True)
    finally:
        cursor.close()
        conn.close()

@add_warning.error
async def add_warning_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)


@bot.tree.command(name="경고차감", description="특정 유저의 경고를 1회 차감합니다. (관리자 전용)")
@app_commands.describe(member="경고를 차감할 유저")
@app_commands.checks.has_permissions(administrator=True)
async def remove_warning(interaction: discord.Interaction, member: discord.Member):
    guild_id = interaction.guild_id
    user_id = member.id
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT warnings FROM users WHERE guild_id = %s AND user_id = %s", (guild_id, user_id))
        row = cursor.fetchone()
        current_warnings = row[0] if row else 0

        if current_warnings <= 0:
            await interaction.response.send_message(f"❌ **{member.display_name}**님의 경고는 이미 0회입니다.", ephemeral=True)
            return

        cursor.execute("UPDATE users SET warnings = warnings - 1 WHERE guild_id = %s AND user_id = %s", (guild_id, user_id))
        conn.commit()
        await interaction.response.send_message(f"✅ **{member.display_name}**님의 경고를 1회 차감했습니다. (현재 경고: {current_warnings - 1}회)", ephemeral=True)
    except Exception as e:
        conn.rollback()
        await interaction.response.send_message(f"❌ 오류 발생: {e}", ephemeral=True)
    finally:
        cursor.close()
        conn.close()

@remove_warning.error
async def remove_warning_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)


@bot.tree.command(name="코인지급", description="특정 유저에게 코인을 지급합니다. (관리자 전용)")
@app_commands.describe(member="코인을 받을 유저", amount="지급할 코인 수량")
@app_commands.checks.has_permissions(administrator=True)
async def give_coins(interaction: discord.Interaction, member: discord.Member, amount: int):
    if amount <= 0:
        await interaction.response.send_message("❌ 지급할 코인 수량은 1 이상이어야 합니다.", ephemeral=True)
        return

    guild_id = interaction.guild_id
    user_id = member.id
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO users (guild_id, user_id, username, coins)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (guild_id, user_id) DO UPDATE SET coins = users.coins + EXCLUDED.coins, username = EXCLUDED.username
            """,
            (guild_id, user_id, member.name, amount)
        )
        conn.commit()
        await interaction.response.send_message(f"✅ **{member.display_name}**님에게 **{amount:,}코인**을 지급했습니다!", ephemeral=True)
    except Exception as e:
        conn.rollback()
        await interaction.response.send_message(f"❌ 오류 발생: {e}", ephemeral=True)
    finally:
        cursor.close()
        conn.close()

@give_coins.error
async def give_coins_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)


@bot.tree.command(name="코인회수", description="특정 유저의 코인을 차감/회수합니다. (관리자 전용)")
@app_commands.describe(member="코인을 회수할 유저", amount="회수할 코인 수량")
@app_commands.checks.has_permissions(administrator=True)
async def take_coins(interaction: discord.Interaction, member: discord.Member, amount: int):
    if amount <= 0:
        await interaction.response.send_message("❌ 회수할 코인 수량은 1 이상이어야 합니다.", ephemeral=True)
        return

    guild_id = interaction.guild_id
    user_id = member.id
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE users SET coins = GREATEST(0, coins - %s)
            WHERE guild_id = %s AND user_id = %s
            RETURNING coins
            """,
            (amount, guild_id, user_id)
        )
        row = cursor.fetchone()
        if not row:
            await interaction.response.send_message("❌ 해당 유저의 데이터를 찾을 수 없습니다.", ephemeral=True)
            return
        conn.commit()
        await interaction.response.send_message(f"✅ **{member.display_name}**님의 코인 중 **{amount:,}코인**을 회수했습니다. (남은 잔액: {row[0]:,}코인)", ephemeral=True)
    except Exception as e:
        conn.rollback()
        await interaction.response.send_message(f"❌ 오류 발생: {e}", ephemeral=True)
    finally:
        cursor.close()
        conn.close()

@take_coins.error
async def take_coins_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)


@bot.tree.command(name="방어권지급", description="특정 유저에게 방어권을 지급합니다. (관리자 전용)")
@app_commands.describe(member="방어권을 받을 유저", amount="지급할 방어권 수량")
@app_commands.checks.has_permissions(administrator=True)
async def give_defense_ticket(interaction: discord.Interaction, member: discord.Member, amount: int = 1):
    if amount <= 0:
        await interaction.response.send_message("❌ 지급할 수량은 1 이상이어야 합니다.", ephemeral=True)
        return

    guild_id = interaction.guild_id
    user_id = member.id
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO users (guild_id, user_id, username, defense_tickets)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (guild_id, user_id) DO UPDATE SET defense_tickets = users.defense_tickets + EXCLUDED.defense_tickets, username = EXCLUDED.username
            """,
            (guild_id, user_id, member.name, amount)
        )
        conn.commit()
        await interaction.response.send_message(f"🛡️ **{member.display_name}**님에게 방어권 **{amount}개**를 지급했습니다!", ephemeral=True)
    except Exception as e:
        conn.rollback()
        await interaction.response.send_message(f"❌ 오류 발생: {e}", ephemeral=True)
    finally:
        cursor.close()
        conn.close()

@give_defense_ticket.error
async def give_defense_ticket_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)


# ==========================================
# 🎰 슬롯머신 및 기타 명령어 기능부
# ==========================================
SLOT_ICONS = ["🍒", "🍋", "🍊", "🔔", "⭐", "💎", "7️⃣"]

SLOT_OUTCOMES = (
    ("jackpot", 3.0),
    ("double", 1.5),
    ("pair", 0.5),
    ("lose", 0.0),
)

active_slot_spins = set()
SLOT_CONCURRENCY_LIMIT = 50
slot_semaphore = asyncio.Semaphore(SLOT_CONCURRENCY_LIMIT)


def roll_slot_result(rtp_percent: int):
    rtp = max(0, min(150, int(rtp_percent))) / 100.0
    winning_average_multiplier = sum(multiplier for _, multiplier in SLOT_OUTCOMES[:3]) / 3.0
    total_win_probability = min(1.0, rtp / winning_average_multiplier)

    if random.random() < total_win_probability:
        outcome_name, multiplier = random.choice(SLOT_OUTCOMES[:3])

        if outcome_name == "jackpot":
            icon = random.choice(SLOT_ICONS)
            result_icons = [icon, icon, icon]
        elif outcome_name == "double":
            icon = random.choice(SLOT_ICONS)
            other = random.choice([i for i in SLOT_ICONS if i != icon])
            result_icons = [icon, icon, other]
            random.shuffle(result_icons)
        else:
            icon = random.choice(SLOT_ICONS)
            other_icons = [i for i in SLOT_ICONS if i != icon]
            result_icons = [icon, icon, random.choice(other_icons)]
            random.shuffle(result_icons)

        return result_icons, multiplier

    result_icons = random.sample(SLOT_ICONS, 3)
    return result_icons, 0.0


def get_slot_rtp(guild_id: int) -> int:
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT slot_rtp FROM guild_settings WHERE guild_id = %s",
            (guild_id,),
        )
        row = cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else 85
    finally:
        cursor.close()
        conn.close()


def deduct_slot_bet(guild_id: int, user_id: int, bet_amount: int) -> bool:
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO users (
                guild_id, user_id, coins, voice_minutes,
                warnings, defense_tickets
            )
            VALUES (%s, %s, 0, 0, 0, 0)
            ON CONFLICT (guild_id, user_id) DO NOTHING
            """,
            (guild_id, user_id),
        )

        cursor.execute(
            """
            UPDATE users
            SET coins = coins - %s
            WHERE guild_id = %s
              AND user_id = %s
              AND coins >= %s
            RETURNING coins
            """,
            (bet_amount, guild_id, user_id, bet_amount),
        )
        row = cursor.fetchone()

        if row is None:
            conn.rollback()
            return False

        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def add_slot_payout(guild_id: int, user_id: int, payout: int) -> int:
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE users
            SET coins = coins + %s
            WHERE guild_id = %s AND user_id = %s
            RETURNING coins
            """,
            (payout, guild_id, user_id),
        )
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError("슬롯머신 당첨금 지급 대상 사용자를 찾을 수 없습니다.")
        conn.commit()
        return int(row[0])
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def get_current_coins(guild_id: int, user_id: int) -> int:
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT coins FROM users WHERE guild_id = %s AND user_id = %s",
            (guild_id, user_id),
        )
        row = cursor.fetchone()
        return int(row[0]) if row else 0
    finally:
        cursor.close()
        conn.close()


class SlotMachineView(discord.ui.View):
    def __init__(self, author_id: int, bet_amount: int):
        super().__init__(timeout=30)
        self.author_id = author_id
        self.bet_amount = bet_amount

    @discord.ui.button(label="🎰 다시 돌리기", style=discord.ButtonStyle.success)
    async def spin_again(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "본인이 실행한 슬롯머신만 다시 돌릴 수 있습니다.",
                ephemeral=True,
            )
            return

        if interaction.guild_id is None:
            await interaction.response.send_message(
                "슬롯머신은 서버에서만 사용할 수 있습니다.",
                ephemeral=True,
            )
            return

        spin_key = (interaction.guild_id, self.author_id)
        if spin_key in active_slot_spins:
            await interaction.response.send_message(
                "⏳ 이미 슬롯머신이 돌아가고 있습니다. 잠시만 기다려주세요.",
                ephemeral=True,
            )
            return

        active_slot_spins.add(spin_key)
        button.disabled = True

        try:
            guild_id = interaction.guild_id
            user_id = interaction.user.id

            await interaction.response.edit_message(
                content="🎰 **슬롯머신이 돌아가는 중입니다...**\n` 🔄 | 🔄 | 🔄 `",
                view=None,
            )
            msg = interaction.message

            async with slot_semaphore:
                if not deduct_slot_bet(guild_id, user_id, self.bet_amount):
                    current_coins = get_current_coins(guild_id, user_id)
                    await msg.edit(
                        content=(
                            "❌ 코인이 부족합니다! "
                            f"(현재 잔액: {current_coins:,}코인)"
                        ),
                        view=None,
                    )
                    return

                rtp = get_slot_rtp(guild_id)
                await asyncio.sleep(0.6)
                result_icons, multiplier = roll_slot_result(rtp)
                payout = int(self.bet_amount * multiplier)

                if payout > 0:
                    final_coins = add_slot_payout(guild_id, user_id, payout)
                else:
                    final_coins = get_current_coins(guild_id, user_id)

                if multiplier >= 3.0:
                    result_text = (
                        "🎉 **[잭팟 당첨! 3배 승리!]** "
                        f"배팅액의 3배인 **+{payout:,}코인**을 획득하셨습니다!"
                    )
                elif multiplier > 0:
                    result_text = f"✨ **[당첨!]** 배팅액의 **{multiplier:g}배**인 **+{payout:,}코인**을 획득하셨습니다!"
                else:
                    result_text = "😢 **[꽝]** 배팅액의 **0배**입니다. 아쉽게도 꽝입니다. 다음 기회에 도전해보세요!"

                final_view = SlotMachineView(self.author_id, self.bet_amount)
                await msg.edit(
                    content=(
                        "🎰 **[슬롯머신 결과]**\n"
                        f"` {result_icons[0]} | {result_icons[1]} | {result_icons[2]} `\n\n"
                        f"{result_text}\n"
                        f"💰 현재 잔액: **{final_coins:,}코인**"
                    ),
                    view=final_view,
                )

        except Exception as e:
            print(f"[슬롯머신 버튼 오류] {type(e).__name__}: {e}")
        finally:
            active_slot_spins.discard(spin_key)


@bot.tree.command(
    name="정보", description="본인 또는 선택한 사용자의 코인, 음성 접속 시간, 경고 횟수, 방어권을 확인합니다."
)
@app_commands.describe(member="조회할 사용자 (선택하지 않으면 본인)")
async def my_info(
    interaction: discord.Interaction, member: discord.Member = None
):
    target = member or interaction.user
    guild_id = interaction.guild_id
    user_id = target.id

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT coins, voice_minutes, warnings, defense_tickets FROM users WHERE guild_id = %s AND user_id = %s",
        (guild_id, user_id),
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    coins = row[0] if row else 0
    minutes = row[1] if row else 0
    warnings = row[2] if row else 0
    defense_tickets = row[3] if row else 0

    await interaction.response.send_message(
        f"**{target.mention}**님의 서버 활동 정보:\n"
        f"- 🪙 대깨 코인: **{coins:,}개**\n"
        f"- ⌛ 음성 접속 시간: **{minutes}분**\n"
        f"- ⚠️ 경고 횟수: **{warnings}회** (3회 누적 시 차단)\n"
        f"- 🛡 방어권: **{defense_tickets}개**",
        ephemeral=True,
    )


@bot.tree.command(
    name="추천인",
    description="나를 초대해준 사람을 추천인으로 등록합니다. (누적 음성 30분 이상 시 가능)",
)
@app_commands.describe(referrer="추천할 유저를 선택하세요")
async def register_referral(interaction: discord.Interaction, referrer: discord.Member):
    if referrer.id == interaction.user.id:
        await interaction.response.send_message(
            "자기 자신을 추천인으로 등록할 수 없습니다.", ephemeral=True
        )
        return
    if referrer.bot:
        await interaction.response.send_message(
            "봇은 추천인으로 등록할 수 없습니다.", ephemeral=True
        )
        return

    guild_id = interaction.guild_id
    user_id = interaction.user.id

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT voice_minutes, referred_by FROM users WHERE guild_id = %s AND user_id = %s",
        (guild_id, user_id),
    )
    row = cursor.fetchone()

    if row and row[1] is not None:
        cursor.close()
        conn.close()
        await interaction.response.send_message(
            "이미 추천인을 등록하셨습니다.", ephemeral=True
        )
        return

    user_minutes = row[0] if row else 0
    if user_minutes < 30:
        cursor.close()
        conn.close()
        await interaction.response.send_message(
            f"❌ 음성 접속 시간이 **30분 이상**일 때만 추천인 등록이 가능합니다. (현재: {user_minutes}분)",
            ephemeral=True,
        )
        return

    cursor.execute(
        "SELECT referral_reward FROM guild_settings WHERE guild_id = %s",
        (guild_id,),
    )
    setting = cursor.fetchone()
    referral_reward = setting[0] if setting else 30

    cursor.execute(
        """
        INSERT INTO users (guild_id, user_id, coins, voice_minutes, referred_by)
        VALUES (%s, %s, 0, %s, %s)
        ON CONFLICT (guild_id, user_id) DO UPDATE SET referred_by = EXCLUDED.referred_by
    """,
        (guild_id, user_id, user_minutes, referrer.id),
    )

    cursor.execute(
        """
        INSERT INTO users (guild_id, user_id, coins, voice_minutes)
        VALUES (%s, %s, %s, 0)
        ON CONFLICT (guild_id, user_id) DO UPDATE SET coins = users.coins + EXCLUDED.coins
    """,
        (guild_id, referrer.id, referral_reward),
    )

    conn.commit()
    cursor.close()
    conn.close()

    await interaction.response.send_message(
        f"✅ 성공적으로 {referrer.mention}님을 추천인으로 등록했습니다! 추천인에게 **{referral_reward}코인**이 지급되었습니다.",
        ephemeral=True,
    )


@bot.tree.command(
    name="슬롯머신",
    description="코인을 걸고 슬롯머신을 돌립니다. (최대 500코인, 최대 3배 배율)",
)
@app_commands.describe(bet="배팅할 코인 수량 (1 ~ 500코인)")
async def slot_machine(interaction: discord.Interaction, bet: int):
    if interaction.guild_id is None:
        await interaction.response.send_message(
            "❌ 슬롯머신은 서버에서만 사용할 수 있습니다.",
            ephemeral=True,
        )
        return

    if bet < 1 or bet > 500:
        await interaction.response.send_message(
            "❌ 배팅 코인은 **1 ~ 500코인** 사이여야 합니다.",
            ephemeral=True,
        )
        return

    user_id = interaction.user.id
    guild_id = interaction.guild_id
    spin_key = (guild_id, user_id)

    if spin_key in active_slot_spins:
        await interaction.response.send_message(
            "⏳ 이미 슬롯머신이 돌아가고 있습니다. 잠시만 기다려주세요.",
            ephemeral=True,
        )
        return

    active_slot_spins.add(spin_key)

    try:
        await interaction.response.send_message(
            "🎰 **슬롯머신이 돌아가는 중입니다...**\n` 🔄 | 🔄 | 🔄 `"
        )
        msg = await interaction.original_response()

        async with slot_semaphore:
            if not deduct_slot_bet(guild_id, user_id, bet):
                current_coins = get_current_coins(guild_id, user_id)
                await msg.edit(
                    content=(
                        "❌ 코인이 부족합니다! "
                        f"(현재 잔액: {current_coins:,}코인)"
                    ),
                    view=None,
                )
                return

            rtp = get_slot_rtp(guild_id)
            await asyncio.sleep(0.6)
            result_icons, multiplier = roll_slot_result(rtp)
            payout = int(bet * multiplier)

            if payout > 0:
                final_coins = add_slot_payout(guild_id, user_id, payout)
            else:
                final_coins = get_current_coins(guild_id, user_id)

            if multiplier >= 3.0:
                result_text = f"🎉 **[잭팟 당첨! 3배 승리!]** 배팅액의 3배인 **+{payout:,}코인**을 획득하셨습니다!"
            elif multiplier > 0:
                result_text = f"✨ **[당첨!]** 배팅액의 **{multiplier:g}배**인 **+{payout:,}코인**을 획득하셨습니다!"
            else:
                result_text = "😢 **[꽝]** 아쉽게도 꽝입니다."

            view = SlotMachineView(user_id, bet)
            await msg.edit(
                content=(
                    "🎰 **[슬롯머신 결과]**\n"
                    f"` {result_icons[0]} | {result_icons[1]} | {result_icons[2]} `\n\n"
                    f"{result_text}\n"
                    f"💰 현재 잔액: **{final_coins:,}코인**"
                ),
                view=view,
            )

    except Exception as e:
        print(f"[슬롯머신 오류] {e}")
    finally:
        active_slot_spins.discard(spin_key)


@bot.tree.command(
    name="코인순위", description="이 서버에서 코인이 많은 상위 10명을 확인합니다."
)
async def coin_ranking(interaction: discord.Interaction):
    guild_id = interaction.guild_id
    if guild_id is None:
        await interaction.response.send_message("서버 안에서만 사용할 수 있습니다.", ephemeral=True)
        return

    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT user_id, coins
            FROM users
            WHERE guild_id = %s AND coins > 0
            ORDER BY coins DESC, user_id ASC
            LIMIT 10
            """,
            (guild_id,),
        )
        rows = cursor.fetchall()
        cursor.close()
    finally:
        conn.close()

    if rows:
        medals = {1: "🥇", 2: "🥈", 3: "🥉"}
        ranking_lines = []
        for rank, (user_id, coins) in enumerate(rows, start=1):
            rank_label = medals.get(rank, f"{rank}.")
            ranking_lines.append(f"{rank_label} <@{user_id}> — **{coins:,}코인**")
        description = "\n".join(ranking_lines)
    else:
        description = "아직 코인을 보유한 사용자가 없습니다."

    embed = discord.Embed(
        title=f"🏆 {interaction.guild.name} 코인 순위",
        description=description,
        color=discord.Color.gold(),
    )
    embed.set_footer(text="코인 보유량 상위 10명")
    await interaction.response.send_message(
        embed=embed,
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )


@bot.tree.command(
    name="명령어", description="봇이 사용할 수 있는 명령어 목록을 확인합니다."
)
async def show_commands(interaction: discord.Interaction):
    embed = discord.Embed(
        title="🤖 봇 명령어 안내",
        description="이 서버에서 사용할 수 있는 명령어입니다.",
        color=discord.Color.blue(),
    )
    embed.add_field(
        name="👤 일반 사용자용 명령어",
        value=(
            "• `/정보 [유저]` — 코인, 음성 시간, 경고, 방어권 확인\n"
            "• `/추천인 [유저]` — 추천인 등록 (음성 30분 이상)\n"
            "• `/슬롯머신 [배팅액]` — 슬롯머신 미니게임 (최대 500코인)\n"
            "• `/코인순위` — 코인 상위 10명 확인\n"
            "• `/명령어` — 명령어 안내"
        ),
        inline=False,
    )
    embed.add_field(
        name="🛡 관리자 전용 명령어",
        value=(
            "• `/경고부여 [유저] [사유]` — 유저 경고 1회 추가 (3회 누적 시 추방)\n"
            "• `/경고차감 [유저]` — 유저 경고 1회 차감\n"
            "• `/코인지급 [유저] [수량]` — 유저에게 코인 지급\n"
            "• `/코인회수 [유저] [수량]` — 유저의 코인 회수\n"
            "• `/방어권지급 [유저] [수량]` — 유저에게 방어권 지급\n"
            "• `/채팅청소 [유저] [수량]` — 특정 유저 메시지 삭제\n"
            "• `/닉네임동기화` — 서버 멤버 전체 닉네임 DB 일괄 동기화"
        ),
        inline=False,
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


if __name__ == "__main__":
    token = os.getenv("DISCORD_TOKEN") or os.getenv("DISCORD_BOT_TOKEN")
    
    if not token:
        print("❌ 에러: DISCORD_TOKEN 환경 변수가 설정되지 않았습니다!")
        exit(1)
        
bot.run(token)

import os
import time
import asyncio
from threading import Thread
from datetime import datetime, timezone, timedelta
import random
import discord
from discord import app_commands
from discord.ext import commands, tasks
from flask import Flask
import psycopg2

# 0. 렌더(Render) 24시간 유지용 Flask 웹서버 설정 (가장 먼저 실행)
app = Flask('')

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

# 봇이 켜지기 전에 웹서버와 포트를 확실히 먼저 엽니다.
keep_alive()


VOICE_REWARD_INTERVAL_MINUTES = 30
KST = timezone(timedelta(hours=9)) # 한국 표준시 (UTC+9)

# 외부 클라우드 DB 연결 주소
DATABASE_URL = os.getenv("DATABASE_URL")


# 1. 데이터베이스 연결 함수
def get_db():
    if not DATABASE_URL:
        raise RuntimeError("❌ 에러: DATABASE_URL 환경 변수가 설정되지 않았습니다!")
    return psycopg2.connect(DATABASE_URL, sslmode='require')


# 2. 데이터베이스 초기화 및 테이블 생성 함수
def init_db():
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("""
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
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS voice_sessions (
            guild_id BIGINT,
            user_id BIGINT,
            join_time DOUBLE PRECISION,
            counting_since DOUBLE PRECISION DEFAULT NULL,
            accumulated_seconds DOUBLE PRECISION DEFAULT 0,
            PRIMARY KEY (guild_id, user_id)
        )
    """)
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT DEFAULT NULL")
    cursor.execute("ALTER TABLE voice_sessions ADD COLUMN IF NOT EXISTS counting_since DOUBLE PRECISION DEFAULT NULL")
    cursor.execute("ALTER TABLE voice_sessions ADD COLUMN IF NOT EXISTS accumulated_seconds DOUBLE PRECISION DEFAULT 0")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS guild_settings (
            guild_id BIGINT PRIMARY KEY,
            voice_reward_rate INTEGER DEFAULT 1,
            referral_reward INTEGER NOT NULL DEFAULT 30,
            log_channel_id BIGINT DEFAULT NULL,
            slot_rtp INTEGER DEFAULT 85
        )
    """)

    cursor.execute("""
        ALTER TABLE guild_settings
        ADD COLUMN IF NOT EXISTS slot_rtp INTEGER DEFAULT 85
    """)

    conn.commit()
    cursor.close()
    conn.close()


init_db()

# 인텐트 설정 강화 (Server Members Intent 포함)
intents = discord.Intents.default()
intents.guilds = True
intents.voice_states = True
intents.members = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)
commands_synced = False


@bot.event
async def on_ready():
    global commands_synced
    print(f"로그인 완료: {bot.user}")

    for guild in bot.guilds:
        try:
            await guild.chunk(cache=True)
            print(f"[{guild.name}] 서버 멤버 캐싱 완료")
        except Exception as e:
            print(f"[{guild.name}] 멤버 캐싱 중 오류 발생: {e}")

    if not commands_synced:
        try:
            for guild in bot.guilds:
                bot.tree.clear_commands(guild=guild)
                bot.tree.copy_global_to(guild=guild)
                synced = await bot.tree.sync(guild=guild)
                print(f"[{guild.name}] 서버 명령어 동기화 완료: {len(synced)}개")
            commands_synced = True
        except Exception as e:
            print(f"명령어 동기화 중 오류 발생: {e}")

    # 봇 시작 시점 음성채널 강제 동기화 및 username 즉시 업데이트
    conn = get_db()
    cursor = conn.cursor()
    try:
        for guild in bot.guilds:
            active_voice_user_ids = set()
            for channel in guild.voice_channels:
                for member in channel.members:
                    if member.bot:
                        continue
                    active_voice_user_ids.add(member.id)
                    username = member.name

                    cursor.execute(
                        """
                        INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
                        VALUES (%s, %s, %s, 0, 0, 0, 0)
                        ON CONFLICT (guild_id, user_id) DO UPDATE SET username = EXCLUDED.username
                        """,
                        (guild.id, member.id, username),
                    )

                    now = time.time()
                    is_muted = member.voice.self_mute or member.voice.self_deaf
                    
                    cursor.execute(
                        """
                        INSERT INTO voice_sessions
                            (guild_id, user_id, join_time, counting_since, accumulated_seconds)
                        VALUES (%s, %s, %s, %s, 0)
                        ON CONFLICT (guild_id, user_id) DO NOTHING
                        """,
                        (guild.id, member.id, now, None if is_muted else now),
                    )
            
            cursor.execute(
                "SELECT user_id FROM voice_sessions WHERE guild_id = %s",
                (guild.id,)
            )
            db_sessions = cursor.fetchall()
            for (db_uid,) in db_sessions:
                if db_uid not in active_voice_user_ids:
                    cursor.execute(
                        "DELETE FROM voice_sessions WHERE guild_id = %s AND user_id = %s",
                        (guild.id, db_uid)
                    )

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[음성 세션 복구 오류] {e}")
    finally:
        cursor.close()
        conn.close()

    if not check_voice_time.is_running():
        check_voice_time.start()


@bot.event
async def on_guild_join(guild: discord.Guild):
    try:
        await guild.chunk(cache=True)
    except Exception:
        pass
    bot.tree.clear_commands(guild=guild)
    bot.tree.copy_global_to(guild=guild)
    synced = await bot.tree.sync(guild=guild)
    print(f"[{guild.name}] 서버 명령어 동기화 완료: {len(synced)}개")


# ==========================================
# 🚀 음성 시간 체크 루프 (username 실시간 동기화 포함)
# ==========================================
@tasks.loop(minutes=1)
async def check_voice_time():
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT guild_id, user_id, join_time, counting_since, accumulated_seconds FROM voice_sessions")
        sessions = cur.fetchall()

        for guild_id, user_id, join_time, counting_since, accumulated_seconds in sessions:
            try:
                guild = bot.get_guild(int(guild_id))
                if not guild:
                    continue
                member = guild.get_member(int(user_id))
                if not member:
                    try:
                        member = await guild.fetch_member(int(user_id))
                    except Exception:
                        cur.execute("DELETE FROM voice_sessions WHERE guild_id=%s AND user_id=%s", (guild_id, user_id))
                        conn.commit()
                        continue

                username = member.name
                voice = member.voice
                
                cur.execute("""
                    INSERT INTO users
                    (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
                    VALUES (%s, %s, %s, 0, 0, 0, 0)
                    ON CONFLICT (guild_id, user_id) DO UPDATE SET username = EXCLUDED.username
                """, (guild_id, user_id, username))

                now_ts = datetime.now(timezone.utc).timestamp()
                accumulated = float(accumulated_seconds or 0)
                muted = bool(voice and (voice.self_mute or voice.self_deaf))

                if not voice or not voice.channel:
                    if counting_since is not None:
                        accumulated += max(0, now_ts - float(counting_since))
                    minutes_to_add = int(accumulated // 60)
                    if minutes_to_add > 0:
                        cur.execute("SELECT voice_minutes FROM users WHERE guild_id=%s AND user_id=%s", (guild_id, user_id))
                        row = cur.fetchone()
                        previous = row[0] if row else 0
                        cur.execute("SELECT voice_reward_rate FROM guild_settings WHERE guild_id=%s", (guild_id,))
                        rr = cur.fetchone()
                        rate = rr[0] if rr else 1
                        new_minutes = previous + minutes_to_add
                        crossed = new_minutes // VOICE_REWARD_INTERVAL_MINUTES - previous // VOICE_REWARD_INTERVAL_MINUTES
                        cur.execute("""
                            UPDATE users SET voice_minutes=voice_minutes+%s, coins=coins+%s, username=%s
                            WHERE guild_id=%s AND user_id=%s
                        """, (minutes_to_add, crossed * rate, username, guild_id, user_id))
                    cur.execute("DELETE FROM voice_sessions WHERE guild_id=%s AND user_id=%s", (guild_id, user_id))
                    conn.commit()
                    continue

                if counting_since is None:
                    if not muted:
                        cur.execute("UPDATE voice_sessions SET counting_since=%s WHERE guild_id=%s AND user_id=%s",
                                    (now_ts, guild_id, user_id))
                    conn.commit()
                    continue

                total_seconds = accumulated + max(0, now_ts - float(counting_since))
                minutes_to_add = int(total_seconds // 60)
                remaining = total_seconds - minutes_to_add * 60

                if minutes_to_add <= 0:
                    cur.execute("""
                        UPDATE voice_sessions SET counting_since=%s, accumulated_seconds=%s
                        WHERE guild_id=%s AND user_id=%s
                    """, (None if muted else counting_since, total_seconds, guild_id, user_id))
                    conn.commit()
                    continue

                cur.execute("SELECT voice_minutes FROM users WHERE guild_id=%s AND user_id=%s", (guild_id, user_id))
                row = cur.fetchone()
                previous = row[0] if row else 0
                new_minutes = previous + minutes_to_add
                cur.execute("SELECT voice_reward_rate FROM guild_settings WHERE guild_id=%s", (guild_id,))
                rr = cur.fetchone()
                rate = rr[0] if rr else 1
                crossed = new_minutes // VOICE_REWARD_INTERVAL_MINUTES - previous // VOICE_REWARD_INTERVAL_MINUTES
                
                cur.execute("""
                    UPDATE users SET voice_minutes=voice_minutes+%s, coins=coins+%s, username=%s
                    WHERE guild_id=%s AND user_id=%s
                """, (minutes_to_add, crossed * rate, username, guild_id, user_id))
                
                cur.execute("""
                    UPDATE voice_sessions SET counting_since=%s, accumulated_seconds=%s
                    WHERE guild_id=%s AND user_id=%s
                """, (None if muted else now_ts, remaining, guild_id, user_id))
                conn.commit()

            except Exception as user_error:
                conn.rollback()
                print(f"[VOICE DEBUG] 사용자 처리 오류: guild={guild_id}, user={user_id}, error={user_error}")
                continue
    except Exception as e:
        conn.rollback()
        print(f"[VOICE DEBUG] 전체 음성 체크 오류: {e}")
    finally:
        cur.close()
        conn.close()


@bot.event
async def on_voice_state_update(member, before, after):
    if member.bot:
        return

    guild_id = member.guild.id
    user_id = member.id
    now = time.time()
    username = member.name

    was_connected = before.channel is not None
    is_connected = after.channel is not None
    muted_after = after.self_mute or after.self_deaf

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT join_time, counting_since, COALESCE(accumulated_seconds, 0)
            FROM voice_sessions
            WHERE guild_id = %s AND user_id = %s
            FOR UPDATE
            """,
            (guild_id, user_id),
        )
        row = cursor.fetchone()

        if not is_connected:
            if row:
                join_time, counting_since, accumulated_seconds = row
                if counting_since is not None:
                    accumulated_seconds += max(0, now - counting_since)
                minutes_to_add = int(accumulated_seconds // 60)
                if minutes_to_add > 0:
                    cursor.execute(
                        "SELECT voice_minutes FROM users WHERE guild_id = %s AND user_id = %s",
                        (guild_id, user_id),
                    )
                    user_row = cursor.fetchone()
                    previous_minutes = user_row[0] if user_row else 0
                    new_minutes = previous_minutes + minutes_to_add
                    reward_rate = 1
                    cursor.execute("SELECT voice_reward_rate FROM guild_settings WHERE guild_id = %s", (guild_id,))
                    reward_row = cursor.fetchone()
                    if reward_row:
                        reward_rate = reward_row[0]
                    crossed = (new_minutes // VOICE_REWARD_INTERVAL_MINUTES) - (previous_minutes // VOICE_REWARD_INTERVAL_MINUTES)
                    added_coins = crossed * reward_rate
                    cursor.execute(
                        """
                        INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
                        VALUES (%s, %s, %s, %s, %s, 0, 0)
                        ON CONFLICT (guild_id, user_id) DO UPDATE SET
                            username = EXCLUDED.username,
                            coins = users.coins + EXCLUDED.coins,
                            voice_minutes = users.voice_minutes + EXCLUDED.voice_minutes
                        """,
                        (guild_id, user_id, username, added_coins, minutes_to_add),
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
                        VALUES (%s, %s, %s, 0, 0, 0, 0)
                        ON CONFLICT (guild_id, user_id) DO UPDATE SET username = EXCLUDED.username
                        """,
                        (guild_id, user_id, username),
                    )
                cursor.execute(
                    "DELETE FROM voice_sessions WHERE guild_id = %s AND user_id = %s",
                    (guild_id, user_id),
                )

        elif not was_connected:
            cursor.execute(
                """
                INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
                VALUES (%s, %s, %s, 0, 0, 0, 0)
                ON CONFLICT (guild_id, user_id) DO UPDATE SET username = EXCLUDED.username
                """,
                (guild_id, user_id, username),
            )

            cursor.execute(
                """
                INSERT INTO voice_sessions
                    (guild_id, user_id, join_time, counting_since, accumulated_seconds)
                VALUES (%s, %s, %s, %s, 0)
                ON CONFLICT (guild_id, user_id) DO UPDATE SET
                    join_time = EXCLUDED.join_time,
                    counting_since = EXCLUDED.counting_since,
                    accumulated_seconds = 0
                """,
                (guild_id, user_id, now, None if muted_after else now),
            )

        else:
            cursor.execute(
                """
                INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
                VALUES (%s, %s, %s, 0, 0, 0, 0)
                ON CONFLICT (guild_id, user_id) DO UPDATE SET username = EXCLUDED.username
                """,
                (guild_id, user_id, username),
            )
            
            if row:
                join_time, counting_since, accumulated_seconds = row
                if counting_since is not None:
                    accumulated_seconds += max(0, now - counting_since)

                cursor.execute(
                    """
                    UPDATE voice_sessions
                    SET counting_since = %s, accumulated_seconds = %s
                    WHERE guild_id = %s AND user_id = %s
                    """,
                    (None if muted_after else now, accumulated_seconds, guild_id, user_id),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO voice_sessions
                        (guild_id, user_id, join_time, counting_since, accumulated_seconds)
                    VALUES (%s, %s, %s, %s, 0)
                    """,
                    (guild_id, user_id, now, None if muted_after else now),
                )

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[음성 상태 업데이트 오류] {e}")
    finally:
        cursor.close()
        conn.close()


# ==========================================
# 🧹 채팅 청소 명령어 (관리자 전용 - 순정 purge 방식으로 고속 안정화)
# ==========================================
@bot.tree.command(name="채팅청소", description="특정 유저가 보낸 메시지를 지정한 수량만큼 삭제합니다. (관리자 전용)")
@app_commands.describe(member="청소할 대상 유저", limit="탐색 및 삭제할 최대 메시지 수 (1~100)")
@app_commands.checks.has_permissions(administrator=True)
async def clear_user_chat(interaction: discord.Interaction, member: discord.Member, limit: int = 20):
    await interaction.response.defer(thinking=True, ephemeral=True)

    if limit < 1 or limit > 100:
        await interaction.followup.send("⚠️ 삭제 수량은 1부터 100 사이로 입력해 주세요.", ephemeral=True)
        return

    try:
        # purge 함수를 사용하여 해당 유저가 보낸 메시지만 타겟팅해서 빠르게 일괄 삭제
        def is_target(m: discord.Message):
            return m.author.id == member.id

        deleted = await interaction.channel.purge(limit=limit, check=is_target, bulk=True)

        await interaction.followup.send(
            f"🧹 **{member.display_name}**님의 메시지 **{len(deleted)}개**를 성공적으로 청소했습니다!", 
            ephemeral=True
        )
    except Exception as e:
        await interaction.followup.send(f"❌ 메시지 청소 중 오류가 발생했습니다: {e}", ephemeral=True)

@clear_user_chat.error
async def clear_user_chat_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        if not interaction.response.is_done():
            await interaction.response.send_message("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)
        else:
            await interaction.followup.send("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)
    else:
        if not interaction.response.is_done():
            await interaction.response.send_message(f"❌ 오류 발생: {error}", ephemeral=True)
        else:
            await interaction.followup.send(f"❌ 오류 발생: {error}", ephemeral=True)


# ==========================================
# 🔄 닉네임 일괄 동기화 명령어 (관리자 전용)
# ==========================================
@bot.tree.command(name="닉네임동기화", description="서버 내 모든 멤버의 디스코드 닉네임을 DB에 강제로 일괄 동기화합니다.")
@app_commands.checks.has_permissions(administrator=True)
async def force_sync_usernames(interaction: discord.Interaction):
    guild = interaction.guild
    conn = get_db()
    cursor = conn.cursor()
    updated_count = 0
    try:
        for member in guild.members:
            if member.bot:
                continue
            cursor.execute(
                """
                INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
                VALUES (%s, %s, %s, 0, 0, 0, 0)
                ON CONFLICT (guild_id, user_id) DO UPDATE SET username = EXCLUDED.username
                """,
                (guild.id, member.id, member.name)
            )
            updated_count += 1
        conn.commit()
        await interaction.response.send_message(f"✅ 성공적으로 서버 멤버 {updated_count}명의 닉네임을 DB에 동기화했습니다!", ephemeral=True)
    except Exception as e:
        conn.rollback()
        await interaction.response.send_message(f"❌ 동기화 중 오류 발생: {e}", ephemeral=True)
    finally:
        cursor.close()
        conn.close()

@force_sync_usernames.error
async def force_sync_usernames_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)


# ==========================================
# 🚨 기존 관리자 명령어 (경고, 방어권, 코인 관리 등) 복구 및 통합
# ==========================================
@bot.tree.command(name="경고부여", description="특정 유저에게 경고를 1회 부여합니다. (관리자 전용)")
@app_commands.describe(member="경고를 받을 유저", reason="경고 사유")
@app_commands.checks.has_permissions(administrator=True)
async def add_warning(interaction: discord.Interaction, member: discord.Member, reason: str = "사유 없음"):
    if member.bot:
        await interaction.response.send_message("봇에게는 경고를 부여할 수 없습니다.", ephemeral=True)
        return

    guild_id = interaction.guild_id
    user_id = member.id
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT defense_tickets, warnings FROM users WHERE guild_id = %s AND user_id = %s", (guild_id, user_id))
        row = cursor.fetchone()
        defense_tickets = row[0] if row else 0
        current_warnings = row[1] if row else 0

        if defense_tickets > 0:
            cursor.execute("UPDATE users SET defense_tickets = defense_tickets - 1 WHERE guild_id = %s AND user_id = %s", (guild_id, user_id))
            conn.commit()
            await interaction.response.send_message(
                f"🛡️ **{member.display_name}**님은 방어권을 보유하고 있어 경고를 **방어**했습니다! (남은 방어권: {defense_tickets - 1}개)",
                ephemeral=True
            )
            return

        new_warnings = current_warnings + 1
        cursor.execute(
            """
            INSERT INTO users (guild_id, user_id, username, warnings)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (guild_id, user_id) DO UPDATE SET warnings = users.warnings + 1, username = EXCLUDED.username
            """,
            (guild_id, user_id, member.name, 1)
        )
        conn.commit()

        if new_warnings >= 3:
            try:
                await member.kick(reason="경고 3회 누적")
                kick_msg = " (경고 3회 누적으로 **추방**되었습니다)"
            except Exception:
                kick_msg = " (추방 권한이 없어 추방하지 못했습니다)"
        else:
            kick_msg = ""

        await interaction.response.send_message(
            f"⚠️ **{member.display_name}**님에게 경고를 부여했습니다. (현재 경고: {new_warnings}회){kick_msg}\n- 사유: {reason}",
            ephemeral=True
        )
    except Exception as e:
        conn.rollback()
        await interaction.response.send_message(f"❌ 오류 발생: {e}", ephemeral=True)
    finally:
        cursor.close()
        conn.close()

@add_warning.error
async def add_warning_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)


@bot.tree.command(name="경고차감", description="특정 유저의 경고를 1회 차감합니다. (관리자 전용)")
@app_commands.describe(member="경고를 차감할 유저")
@app_commands.checks.has_permissions(administrator=True)
async def remove_warning(interaction: discord.Interaction, member: discord.Member):
    guild_id = interaction.guild_id
    user_id = member.id
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT warnings FROM users WHERE guild_id = %s AND user_id = %s", (guild_id, user_id))
        row = cursor.fetchone()
        current_warnings = row[0] if row else 0

        if current_warnings <= 0:
            await interaction.response.send_message(f"❌ **{member.display_name}**님의 경고는 이미 0회입니다.", ephemeral=True)
            return

        cursor.execute("UPDATE users SET warnings = warnings - 1 WHERE guild_id = %s AND user_id = %s", (guild_id, user_id))
        conn.commit()
        await interaction.response.send_message(f"✅ **{member.display_name}**님의 경고를 1회 차감했습니다. (현재 경고: {current_warnings - 1}회)", ephemeral=True)
    except Exception as e:
        conn.rollback()
        await interaction.response.send_message(f"❌ 오류 발생: {e}", ephemeral=True)
    finally:
        cursor.close()
        conn.close()

@remove_warning.error
async def remove_warning_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)


@bot.tree.command(name="코인지급", description="특정 유저에게 코인을 지급합니다. (관리자 전용)")
@app_commands.describe(member="코인을 받을 유저", amount="지급할 코인 수량")
@app_commands.checks.has_permissions(administrator=True)
async def give_coins(interaction: discord.Interaction, member: discord.Member, amount: int):
    if amount <= 0:
        await interaction.response.send_message("❌ 지급할 코인 수량은 1 이상이어야 합니다.", ephemeral=True)
        return

    guild_id = interaction.guild_id
    user_id = member.id
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO users (guild_id, user_id, username, coins)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (guild_id, user_id) DO UPDATE SET coins = users.coins + EXCLUDED.coins, username = EXCLUDED.username
            """,
            (guild_id, user_id, member.name, amount)
        )
        conn.commit()
        await interaction.response.send_message(f"✅ **{member.display_name}**님에게 **{amount:,}코인**을 지급했습니다!", ephemeral=True)
    except Exception as e:
        conn.rollback()
        await interaction.response.send_message(f"❌ 오류 발생: {e}", ephemeral=True)
    finally:
        cursor.close()
        conn.close()

@give_coins.error
async def give_coins_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)


@bot.tree.command(name="코인회수", description="특정 유저의 코인을 차감/회수합니다. (관리자 전용)")
@app_commands.describe(member="코인을 회수할 유저", amount="회수할 코인 수량")
@app_commands.checks.has_permissions(administrator=True)
async def take_coins(interaction: discord.Interaction, member: discord.Member, amount: int):
    if amount <= 0:
        await interaction.response.send_message("❌ 회수할 코인 수량은 1 이상이어야 합니다.", ephemeral=True)
        return

    guild_id = interaction.guild_id
    user_id = member.id
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE users SET coins = GREATEST(0, coins - %s)
            WHERE guild_id = %s AND user_id = %s
            RETURNING coins
            """,
            (amount, guild_id, user_id)
        )
        row = cursor.fetchone()
        if not row:
            await interaction.response.send_message("❌ 해당 유저의 데이터를 찾을 수 없습니다.", ephemeral=True)
            return
        conn.commit()
        await interaction.response.send_message(f"✅ **{member.display_name}**님의 코인 중 **{amount:,}코인**을 회수했습니다. (남은 잔액: {row[0]:,}코인)", ephemeral=True)
    except Exception as e:
        conn.rollback()
        await interaction.response.send_message(f"❌ 오류 발생: {e}", ephemeral=True)
    finally:
        cursor.close()
        conn.close()

@take_coins.error
async def take_coins_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)


@bot.tree.command(name="방어권지급", description="특정 유저에게 방어권을 지급합니다. (관리자 전용)")
@app_commands.describe(member="방어권을 받을 유저", amount="지급할 방어권 수량")
@app_commands.checks.has_permissions(administrator=True)
async def give_defense_ticket(interaction: discord.Interaction, member: discord.Member, amount: int = 1):
    if amount <= 0:
        await interaction.response.send_message("❌ 지급할 수량은 1 이상이어야 합니다.", ephemeral=True)
        return

    guild_id = interaction.guild_id
    user_id = member.id
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO users (guild_id, user_id, username, defense_tickets)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (guild_id, user_id) DO UPDATE SET defense_tickets = users.defense_tickets + EXCLUDED.defense_tickets, username = EXCLUDED.username
            """,
            (guild_id, user_id, member.name, amount)
        )
        conn.commit()
        await interaction.response.send_message(f"🛡️ **{member.display_name}**님에게 방어권 **{amount}개**를 지급했습니다!", ephemeral=True)
    except Exception as e:
        conn.rollback()
        await interaction.response.send_message(f"❌ 오류 발생: {e}", ephemeral=True)
    finally:
        cursor.close()
        conn.close()

@give_defense_ticket.error
async def give_defense_ticket_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        await interaction.response.send_message("❌ 이 명령어는 **서버 관리자**만 사용할 수 있습니다.", ephemeral=True)


# ==========================================
# 🎰 슬롯머신 및 기타 명령어 기능부
# ==========================================
SLOT_ICONS = ["🍒", "🍋", "🍊", "🔔", "⭐", "💎", "7️⃣"]

SLOT_OUTCOMES = (
    ("jackpot", 3.0),
    ("double", 1.5),
    ("pair", 0.5),
    ("lose", 0.0),
)

active_slot_spins = set()
SLOT_CONCURRENCY_LIMIT = 50
slot_semaphore = asyncio.Semaphore(SLOT_CONCURRENCY_LIMIT)


def roll_slot_result(rtp_percent: int):
    rtp = max(0, min(150, int(rtp_percent))) / 100.0
    winning_average_multiplier = sum(multiplier for _, multiplier in SLOT_OUTCOMES[:3]) / 3.0
    total_win_probability = min(1.0, rtp / winning_average_multiplier)

    if random.random() < total_win_probability:
        outcome_name, multiplier = random.choice(SLOT_OUTCOMES[:3])

        if outcome_name == "jackpot":
            icon = random.choice(SLOT_ICONS)
            result_icons = [icon, icon, icon]
        elif outcome_name == "double":
            icon = random.choice(SLOT_ICONS)
            other = random.choice([i for i in SLOT_ICONS if i != icon])
            result_icons = [icon, icon, other]
            random.shuffle(result_icons)
        else:
            icon = random.choice(SLOT_ICONS)
            other_icons = [i for i in SLOT_ICONS if i != icon]
            result_icons = [icon, icon, random.choice(other_icons)]
            random.shuffle(result_icons)

        return result_icons, multiplier

    result_icons = random.sample(SLOT_ICONS, 3)
    return result_icons, 0.0


def get_slot_rtp(guild_id: int) -> int:
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT slot_rtp FROM guild_settings WHERE guild_id = %s",
            (guild_id,),
        )
        row = cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else 85
    finally:
        cursor.close()
        conn.close()


def deduct_slot_bet(guild_id: int, user_id: int, bet_amount: int) -> bool:
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO users (
                guild_id, user_id, coins, voice_minutes,
                warnings, defense_tickets
            )
            VALUES (%s, %s, 0, 0, 0, 0)
            ON CONFLICT (guild_id, user_id) DO NOTHING
            """,
            (guild_id, user_id),
        )

        cursor.execute(
            """
            UPDATE users
            SET coins = coins - %s
            WHERE guild_id = %s
              AND user_id = %s
              AND coins >= %s
            RETURNING coins
            """,
            (bet_amount, guild_id, user_id, bet_amount),
        )
        row = cursor.fetchone()

        if row is None:
            conn.rollback()
            return False

        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def add_slot_payout(guild_id: int, user_id: int, payout: int) -> int:
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE users
            SET coins = coins + %s
            WHERE guild_id = %s AND user_id = %s
            RETURNING coins
            """,
            (payout, guild_id, user_id),
        )
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError("슬롯머신 당첨금 지급 대상 사용자를 찾을 수 없습니다.")
        conn.commit()
        return int(row[0])
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def get_current_coins(guild_id: int, user_id: int) -> int:
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT coins FROM users WHERE guild_id = %s AND user_id = %s",
            (guild_id, user_id),
        )
        row = cursor.fetchone()
        return int(row[0]) if row else 0
    finally:
        cursor.close()
        conn.close()


class SlotMachineView(discord.ui.View):
    def __init__(self, author_id: int, bet_amount: int):
        super().__init__(timeout=30)
        self.author_id = author_id
        self.bet_amount = bet_amount

    @discord.ui.button(label="🎰 다시 돌리기", style=discord.ButtonStyle.success)
    async def spin_again(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "본인이 실행한 슬롯머신만 다시 돌릴 수 있습니다.",
                ephemeral=True,
            )
            return

        if interaction.guild_id is None:
            await interaction.response.send_message(
                "슬롯머신은 서버에서만 사용할 수 있습니다.",
                ephemeral=True,
            )
            return

        spin_key = (interaction.guild_id, self.author_id)
        if spin_key in active_slot_spins:
            await interaction.response.send_message(
                "⏳ 이미 슬롯머신이 돌아가고 있습니다. 잠시만 기다려주세요.",
                ephemeral=True,
            )
            return

        active_slot_spins.add(spin_key)
        button.disabled = True

        try:
            guild_id = interaction.guild_id
            user_id = interaction.user.id

            await interaction.response.edit_message(
                content="🎰 **슬롯머신이 돌아가는 중입니다...**\n` 🔄 | 🔄 | 🔄 `",
                view=None,
            )
            msg = interaction.message

            async with slot_semaphore:
                if not deduct_slot_bet(guild_id, user_id, self.bet_amount):
                    current_coins = get_current_coins(guild_id, user_id)
                    await msg.edit(
                        content=(
                            "❌ 코인이 부족합니다! "
                            f"(현재 잔액: {current_coins:,}코인)"
                        ),
                        view=None,
                    )
                    return

                rtp = get_slot_rtp(guild_id)
                await asyncio.sleep(0.6)
                result_icons, multiplier = roll_slot_result(rtp)
                payout = int(self.bet_amount * multiplier)

                if payout > 0:
                    final_coins = add_slot_payout(guild_id, user_id, payout)
                else:
                    final_coins = get_current_coins(guild_id, user_id)

                if multiplier >= 3.0:
                    result_text = (
                        "🎉 **[잭팟 당첨! 3배 승리!]** "
                        f"배팅액의 3배인 **+{payout:,}코인**을 획득하셨습니다!"
                    )
                elif multiplier > 0:
                    result_text = f"✨ **[당첨!]** 배팅액의 **{multiplier:g}배**인 **+{payout:,}코인**을 획득하셨습니다!"
                else:
                    result_text = "😢 **[꽝]** 배팅액의 **0배**입니다. 아쉽게도 꽝입니다. 다음 기회에 도전해보세요!"

                final_view = SlotMachineView(self.author_id, self.bet_amount)
                await msg.edit(
                    content=(
                        "🎰 **[슬롯머신 결과]**\n"
                        f"` {result_icons[0]} | {result_icons[1]} | {result_icons[2]} `\n\n"
                        f"{result_text}\n"
                        f"💰 현재 잔액: **{final_coins:,}코인**"
                    ),
                    view=final_view,
                )

        except Exception as e:
            print(f"[슬롯머신 버튼 오류] {type(e).__name__}: {e}")
        finally:
            active_slot_spins.discard(spin_key)


@bot.tree.command(
    name="정보", description="본인 또는 선택한 사용자의 코인, 음성 접속 시간, 경고 횟수, 방어권을 확인합니다."
)
@app_commands.describe(member="조회할 사용자 (선택하지 않으면 본인)")
async def my_info(
    interaction: discord.Interaction, member: discord.Member = None
):
    target = member or interaction.user
    guild_id = interaction.guild_id
    user_id = target.id

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT coins, voice_minutes, warnings, defense_tickets FROM users WHERE guild_id = %s AND user_id = %s",
        (guild_id, user_id),
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    coins = row[0] if row else 0
    minutes = row[1] if row else 0
    warnings = row[2] if row else 0
    defense_tickets = row[3] if row else 0

    await interaction.response.send_message(
        f"**{target.mention}**님의 서버 활동 정보:\n"
        f"- 🪙 대깨 코인: **{coins:,}개**\n"
        f"- ⌛ 음성 접속 시간: **{minutes}분**\n"
        f"- ⚠️ 경고 횟수: **{warnings}회** (3회 누적 시 차단)\n"
        f"- 🛡 방어권: **{defense_tickets}개**",
        ephemeral=True,
    )


@bot.tree.command(
    name="추천인",
    description="나를 초대해준 사람을 추천인으로 등록합니다. (누적 음성 30분 이상 시 가능)",
)
@app_commands.describe(referrer="추천할 유저를 선택하세요")
async def register_referral(interaction: discord.Interaction, referrer: discord.Member):
    if referrer.id == interaction.user.id:
        await interaction.response.send_message(
            "자기 자신을 추천인으로 등록할 수 없습니다.", ephemeral=True
        )
        return
    if referrer.bot:
        await interaction.response.send_message(
            "봇은 추천인으로 등록할 수 없습니다.", ephemeral=True
        )
        return

    guild_id = interaction.guild_id
    user_id = interaction.user.id

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT voice_minutes, referred_by FROM users WHERE guild_id = %s AND user_id = %s",
        (guild_id, user_id),
    )
    row = cursor.fetchone()

    if row and row[1] is not None:
        cursor.close()
        conn.close()
        await interaction.response.send_message(
            "이미 추천인을 등록하셨습니다.", ephemeral=True
        )
        return

    user_minutes = row[0] if row else 0
    if user_minutes < 30:
        cursor.close()
        conn.close()
        await interaction.response.send_message(
            f"❌ 음성 접속 시간이 **30분 이상**일 때만 추천인 등록이 가능합니다. (현재: {user_minutes}분)",
            ephemeral=True,
        )
        return

    cursor.execute(
        "SELECT referral_reward FROM guild_settings WHERE guild_id = %s",
        (guild_id,),
    )
    setting = cursor.fetchone()
    referral_reward = setting[0] if setting else 30

    cursor.execute(
        """
        INSERT INTO users (guild_id, user_id, coins, voice_minutes, referred_by)
        VALUES (%s, %s, 0, %s, %s)
        ON CONFLICT (guild_id, user_id) DO UPDATE SET referred_by = EXCLUDED.referred_by
    """,
        (guild_id, user_id, user_minutes, referrer.id),
    )

    cursor.execute(
        """
        INSERT INTO users (guild_id, user_id, coins, voice_minutes)
        VALUES (%s, %s, %s, 0)
        ON CONFLICT (guild_id, user_id) DO UPDATE SET coins = users.coins + EXCLUDED.coins
    """,
        (guild_id, referrer.id, referral_reward),
    )

    conn.commit()
    cursor.close()
    conn.close()

    await interaction.response.send_message(
        f"✅ 성공적으로 {referrer.mention}님을 추천인으로 등록했습니다! 추천인에게 **{referral_reward}코인**이 지급되었습니다.",
        ephemeral=True,
    )


@bot.tree.command(
    name="슬롯머신",
    description="코인을 걸고 슬롯머신을 돌립니다. (최대 500코인, 최대 3배 배율)",
)
@app_commands.describe(bet="배팅할 코인 수량 (1 ~ 500코인)")
async def slot_machine(interaction: discord.Interaction, bet: int):
    if interaction.guild_id is None:
        await interaction.response.send_message(
            "❌ 슬롯머신은 서버에서만 사용할 수 있습니다.",
            ephemeral=True,
        )
        return

    if bet < 1 or bet > 500:
        await interaction.response.send_message(
            "❌ 배팅 코인은 **1 ~ 500코인** 사이여야 합니다.",
            ephemeral=True,
        )
        return

    user_id = interaction.user.id
    guild_id = interaction.guild_id
    spin_key = (guild_id, user_id)

    if spin_key in active_slot_spins:
        await interaction.response.send_message(
            "⏳ 이미 슬롯머신이 돌아가고 있습니다. 잠시만 기다려주세요.",
            ephemeral=True,
        )
        return

    active_slot_spins.add(spin_key)

    try:
        await interaction.response.send_message(
            "🎰 **슬롯머신이 돌아가는 중입니다...**\n` 🔄 | 🔄 | 🔄 `"
        )
        msg = await interaction.original_response()

        async with slot_semaphore:
            if not deduct_slot_bet(guild_id, user_id, bet):
                current_coins = get_current_coins(guild_id, user_id)
                await msg.edit(
                    content=(
                        "❌ 코인이 부족합니다! "
                        f"(현재 잔액: {current_coins:,}코인)"
                    ),
                    view=None,
                )
                return

            rtp = get_slot_rtp(guild_id)
            await asyncio.sleep(0.6)
            result_icons, multiplier = roll_slot_result(rtp)
            payout = int(bet * multiplier)

            if payout > 0:
                final_coins = add_slot_payout(guild_id, user_id, payout)
            else:
                final_coins = get_current_coins(guild_id, user_id)

            if multiplier >= 3.0:
                result_text = f"🎉 **[잭팟 당첨! 3배 승리!]** 배팅액의 3배인 **+{payout:,}코인**을 획득하셨습니다!"
            elif multiplier > 0:
                result_text = f"✨ **[당첨!]** 배팅액의 **{multiplier:g}배**인 **+{payout:,}코인**을 획득하셨습니다!"
            else:
                result_text = "😢 **[꽝]** 아쉽게도 꽝입니다."

            view = SlotMachineView(user_id, bet)
            await msg.edit(
                content=(
                    "🎰 **[슬롯머신 결과]**\n"
                    f"` {result_icons[0]} | {result_icons[1]} | {result_icons[2]} `\n\n"
                    f"{result_text}\n"
                    f"💰 현재 잔액: **{final_coins:,}코인**"
                ),
                view=view,
            )

    except Exception as e:
        print(f"[슬롯머신 오류] {e}")
    finally:
        active_slot_spins.discard(spin_key)


@bot.tree.command(
    name="코인순위", description="이 서버에서 코인이 많은 상위 10명을 확인합니다."
)
async def coin_ranking(interaction: discord.Interaction):
    guild_id = interaction.guild_id
    if guild_id is None:
        await interaction.response.send_message("서버 안에서만 사용할 수 있습니다.", ephemeral=True)
        return

    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT user_id, coins
            FROM users
            WHERE guild_id = %s AND coins > 0
            ORDER BY coins DESC, user_id ASC
            LIMIT 10
            """,
            (guild_id,),
        )
        rows = cursor.fetchall()
        cursor.close()
    finally:
        conn.close()

    if rows:
        medals = {1: "🥇", 2: "🥈", 3: "🥉"}
        ranking_lines = []
        for rank, (user_id, coins) in enumerate(rows, start=1):
            rank_label = medals.get(rank, f"{rank}.")
            ranking_lines.append(f"{rank_label} <@{user_id}> — **{coins:,}코인**")
        description = "\n".join(ranking_lines)
    else:
        description = "아직 코인을 보유한 사용자가 없습니다."

    embed = discord.Embed(
        title=f"🏆 {interaction.guild.name} 코인 순위",
        description=description,
        color=discord.Color.gold(),
    )
    embed.set_footer(text="코인 보유량 상위 10명")
    await interaction.response.send_message(
        embed=embed,
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )


@bot.tree.command(
    name="명령어", description="봇이 사용할 수 있는 명령어 목록을 확인합니다."
)
async def show_commands(interaction: discord.Interaction):
    embed = discord.Embed(
        title="🤖 봇 명령어 안내",
        description="이 서버에서 사용할 수 있는 명령어입니다.",
        color=discord.Color.blue(),
    )
    embed.add_field(
        name="👤 일반 사용자용 명령어",
        value=(
            "• `/정보 [유저]` — 코인, 음성 시간, 경고, 방어권 확인\n"
            "• `/추천인 [유저]` — 추천인 등록 (음성 30분 이상)\n"
            "• `/슬롯머신 [배팅액]` — 슬롯머신 미니게임 (최대 500코인)\n"
            "• `/코인순위` — 코인 상위 10명 확인\n"
            "• `/명령어` — 명령어 안내"
        ),
        inline=False,
    )
    embed.add_field(
        name="🛡 관리자 전용 명령어",
        value=(
            "• `/경고부여 [유저] [사유]` — 유저 경고 1회 추가 (3회 누적 시 추방)\n"
            "• `/경고차감 [유저]` — 유저 경고 1회 차감\n"
            "• `/코인지급 [유저] [수량]` — 유저에게 코인 지급\n"
            "• `/코인회수 [유저] [수량]` — 유저의 코인 회수\n"
            "• `/방어권지급 [유저] [수량]` — 유저에게 방어권 지급\n"
            "• `/채팅청소 [유저] [수량]` — 특정 유저 메시지 삭제\n"
            "• `/닉네임동기화` — 서버 멤버 전체 닉네임 DB 일괄 동기화"
        ),
        inline=False,
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


if __name__ == "__main__":
    token = os.getenv("DISCORD_TOKEN") or os.getenv("DISCORD_BOT_TOKEN")
    
    if not token:
        print("❌ 에러: DISCORD_TOKEN 환경 변수가 설정되지 않았습니다!")
        exit(1)
        
    bot.run(token)

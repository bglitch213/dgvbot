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

# 0. 렌더(Render) 24시간 유지용 Flask 웹서버 설정
app = Flask('')

@app.route('/')
def home():
    return "Bot is running!"

def run():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run)
    t.start()


VOICE_REWARD_INTERVAL_MINUTES = 30
KST = timezone(timedelta(hours=9)) # 한국 표준시 (UTC+9)

# 외부 클라우드 DB 연결 주소 (Render 환경 변수에 DATABASE_URL 설정 필요)
DATABASE_URL = os.getenv("DATABASE_URL")


# 1. 데이터베이스 연결 함수
def get_db():
    if not DATABASE_URL:
        raise RuntimeError("❌ 에러: DATABASE_URL 환경 변수가 설정되지 않았습니다! Render Environment 설정을 확인해주세요.")
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

# 인텐트 설정 강화
intents = discord.Intents.default()
intents.guilds = True
intents.voice_states = True
intents.members = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)
commands_synced = False


async def log_admin_action(guild: discord.Guild, action_text: str):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT log_channel_id FROM guild_settings WHERE guild_id = %s", (guild.id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if row and row[0]:
        channel = guild.get_channel(row[0])
        if channel:
            now = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
            embed = discord.Embed(
                title="🛡 관리자 명령어 실행 기록",
                description=f"**내용:** {action_text}\n**시간:** {now}",
                color=discord.Color.orange()
            )
            try:
                async for message in channel.history(limit=1):
                    if message.embeds and message.embeds[0].description:
                        if action_text in message.embeds[0].description:
                            return
                await channel.send(embed=embed)
            except Exception as e:
                print(f"로그 전송 중 오류 발생: {e}")


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

    # 봇 재시작 당시 이미 음성채널에 있던 사용자 세션 동기화 및 복구
    conn = get_db()
    cursor = conn.cursor()
    try:
        for guild in bot.guilds:
            for channel in guild.voice_channels:
                for member in channel.members:
                    if member.bot:
                        continue

                    cursor.execute(
                        """
                        INSERT INTO users (guild_id, user_id, username, coins, voice_minutes, warnings, defense_tickets)
                        VALUES (%s, %s, %s, 0, 0, 0, 0)
                        ON CONFLICT (guild_id, user_id) DO UPDATE SET username = EXCLUDED.username
                        """,
                        (guild.id, member.id, member.name),
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
# 🚀 최적화된 음성 시간 체크 루프 (예외 방어 적용)
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
            # 퇴장 시 유저 네임 및 누적 시간 정산 반영
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
                    # 시간이 0분이어도 퇴장 시 닉네임 동기화 보장
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

        elif before.channel.id != after.channel.id:
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

        else:
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

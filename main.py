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
import psycopg2  # SQLite 대신 외부 PostgreSQL(Supabase) 사용

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


# 2. 데이터베이스 초기화 및 테이블 생성 함수 (PostgreSQL 문법 적용)
def init_db():
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            guild_id BIGINT,
            user_id BIGINT,
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
            PRIMARY KEY (guild_id, user_id)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS guild_settings (
            guild_id BIGINT PRIMARY KEY,
            voice_reward_rate INTEGER DEFAULT 1,
            referral_reward INTEGER NOT NULL DEFAULT 30,
            log_channel_id BIGINT DEFAULT NULL,
            slot_rtp INTEGER DEFAULT 85
        )
    """)

    # 기존 봇 DB로 운영 중인 서버는 CREATE TABLE IF NOT EXISTS만으로
    # 새로 추가된 slot_rtp 컬럼이 생성되지 않습니다.
    # 따라서 기존 테이블에도 안전하게 컬럼을 추가합니다.
    cursor.execute("""
        ALTER TABLE guild_settings
        ADD COLUMN IF NOT EXISTS slot_rtp INTEGER DEFAULT 85
    """)

    conn.commit()
    cursor.close()
    conn.close()


init_db()

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

    if not check_voice_time.is_running():
        check_voice_time.start()


@bot.event
async def on_guild_join(guild: discord.Guild):
    bot.tree.clear_commands(guild=guild)
    bot.tree.copy_global_to(guild=guild)
    synced = await bot.tree.sync(guild=guild)
    print(f"[{guild.name}] 서버 명령어 동기화 완료: {len(synced)}개")


@tasks.loop(minutes=1)
async def check_voice_time():
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT guild_id, user_id, join_time FROM voice_sessions")
    sessions = cursor.fetchall()

    for guild_id, user_id, join_time in sessions:
        guild = bot.get_guild(guild_id)
        if not guild:
            continue
        member = guild.get_member(user_id)

        if (
            member 
            and member.voice 
            and member.voice.channel 
            and not member.bot
            and not member.voice.self_mute
            and not member.voice.self_deaf
        ):
            cursor.execute(
                "SELECT voice_reward_rate FROM guild_settings WHERE guild_id = %s",
                (guild_id,),
            )
            setting = cursor.fetchone()
            reward_rate = setting[0] if setting else 1

            cursor.execute(
                """
                INSERT INTO users (guild_id, user_id, coins, voice_minutes, warnings, defense_tickets)
                VALUES (%s, %s, 0, 0, 0, 0)
                ON CONFLICT (guild_id, user_id) DO NOTHING
                """,
                (guild_id, user_id),
            )
            
            cursor.execute(
                "SELECT coins, voice_minutes FROM users WHERE guild_id = %s AND user_id = %s",
                (guild_id, user_id)
            )
            user_row = cursor.fetchone()
            current_coins = user_row[0]
            current_minutes = user_row[1]
            
            new_minutes = current_minutes + 1
            
            added_coins = 0
            if new_minutes > 0 and new_minutes % VOICE_REWARD_INTERVAL_MINUTES == 0:
                added_coins = reward_rate

            cursor.execute(
                """
                UPDATE users
                SET coins = coins + %s,
                    voice_minutes = %s
                WHERE guild_id = %s AND user_id = %s
                """,
                (added_coins, new_minutes, guild_id, user_id)
            )
        else:
            cursor.execute(
                "DELETE FROM voice_sessions WHERE guild_id = %s AND user_id = %s",
                (guild_id, user_id),
            )

    conn.commit()
    cursor.close()
    conn.close()


@bot.event
async def on_voice_state_update(member, before, after):
    if member.bot:
        return

    guild_id = member.guild.id
    user_id = member.id

    conn = get_db()
    cursor = conn.cursor()

    is_connected = after.channel is not None
    is_muted_or_deafed = after.self_mute or after.self_deaf

    if is_connected and not is_muted_or_deafed:
        cursor.execute(
            """
            INSERT INTO voice_sessions (guild_id, user_id, join_time)
            VALUES (%s, %s, %s)
            ON CONFLICT (guild_id, user_id) DO UPDATE SET join_time = EXCLUDED.join_time
            """,
            (guild_id, user_id, time.time()),
        )
    else:
        cursor.execute(
            "DELETE FROM voice_sessions WHERE guild_id = %s AND user_id = %s",
            (guild_id, user_id),
        )

    conn.commit()
    cursor.close()
    conn.close()


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


# ==========================================
# 🎰 슬롯머신 미니게임 기능부
# ==========================================
SLOT_ICONS = ["🍒", "🍋", "🍊", "🔔", "⭐", "💎", "7️⃣"]

# 슬롯 결과별 실제 지급 배율.
# bet을 먼저 차감하므로 3.0x는 "배팅액의 3배를 지급"한다는 의미입니다.
SLOT_OUTCOMES = (
    ("jackpot", 3.0),
    ("double", 1.5),
    ("pair", 0.5),
    ("lose", 0.0),
)

# 같은 사용자가 동시에 여러 슬롯 버튼을 눌러 중복 처리하는 것을 방지합니다.
active_slot_spins = set()
# 서버 규모가 커져도 동시에 너무 많은 슬롯 DB/Discord 작업이 몰리지 않도록 제한합니다.
# 2,000명 규모에서 많은 사용자가 동시에 이용해도 요청은 순차적으로 안전하게 처리됩니다.
SLOT_CONCURRENCY_LIMIT = 50
slot_semaphore = asyncio.Semaphore(SLOT_CONCURRENCY_LIMIT)


def roll_slot_result(rtp_percent: int):
    """
    설정된 RTP에 맞춰 결과를 선택합니다.

    RTP는 '배팅액 대비 장기적으로 돌려주는 금액의 비율'입니다.
    예: RTP 85 -> 장기 기대 지급액이 배팅액의 약 85%.
    각 당첨 종류는 동일한 비율로 배분하고, 나머지는 꽝으로 처리합니다.

    이 방식은 결과 확률과 지급 배율을 함께 계산하므로 기존 코드처럼
    '잭팟 확률 + 더블 확률'이 누적되어 RTP가 크게 초과하는 문제가 없습니다.
    """
    rtp = max(0, min(150, int(rtp_percent))) / 100.0

    # 세 가지 당첨 결과의 평균 배율 = (3.0 + 1.5 + 0.5) / 3 = 1.666...
    winning_average_multiplier = sum(multiplier for _, multiplier in SLOT_OUTCOMES[:3]) / 3.0
    total_win_probability = min(1.0, rtp / winning_average_multiplier)

    # 당첨 결과는 동일한 확률로 선택합니다.
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
        else:  # pair
            icon = random.choice(SLOT_ICONS)
            other_icons = [i for i in SLOT_ICONS if i != icon]
            result_icons = [icon, icon, random.choice(other_icons)]
            random.shuffle(result_icons)

        return result_icons, multiplier

    # 꽝: 3개가 모두 다른 아이콘으로 만들어 당첨 결과와 겹치지 않게 합니다.
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
    """
    SELECT -> UPDATE 방식의 경쟁 조건을 제거하고,
    '잔액이 충분한 경우에만' 한 번의 UPDATE로 배팅액을 차감합니다.
    """
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
    """당첨금을 지급하고 최신 잔액을 반환합니다."""
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

            # 먼저 Discord 인터랙션에 응답하여 3초 제한을 피합니다.
            await interaction.response.edit_message(
                content="🎰 **슬롯머신이 돌아가는 중입니다...**\n` 🔄 | 🔄 | 🔄 `",
                view=None,
            )
            msg = interaction.message

            # 2,000명 규모에서 동시에 요청이 몰려도 DB/Discord 작업이 폭주하지 않도록 제한합니다.
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
                    result_text = f"✨ **[당첨!]** **+{payout:,}코인**을 획득하셨습니다!"
                else:
                    result_text = "😢 **[꽝]** 아쉽게도 꽝입니다. 다음 기회에 도전해보세요!"

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
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "⚠️ 슬롯머신 처리 중 오류가 발생했습니다.",
                        ephemeral=True,
                    )
                else:
                    await interaction.followup.send(
                        "⚠️ 슬롯머신 처리 중 오류가 발생했습니다.",
                        ephemeral=True,
                    )
            except Exception as followup_error:
                print(f"[슬롯머신 오류 메시지 전송 실패] {followup_error}")
        finally:
            active_slot_spins.discard(spin_key)


@bot.tree.command(
    name="슬롯머신",
    description="코인을 걸고 슬롯머신을 돌립니다. (최대 5000코인, 최대 3배 배율)",
)
@app_commands.describe(bet="배팅할 코인 수량 (1 ~ 5000코인)")
async def slot_machine(interaction: discord.Interaction, bet: int):
    if interaction.guild_id is None:
        await interaction.response.send_message(
            "❌ 슬롯머신은 서버에서만 사용할 수 있습니다.",
            ephemeral=True,
        )
        return

    if bet < 1:
        await interaction.response.send_message(
            "❌ 배팅 코인은 최소 1개 이상이어야 합니다.",
            ephemeral=True,
        )
        return

    if bet > 5000:
        await interaction.response.send_message(
            "❌ 1회 최대 배팅 금액은 **5,000코인**입니다.",
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
        # 먼저 응답하여 Discord의 3초 인터랙션 제한을 피합니다.
        await interaction.response.send_message(
            "🎰 **슬롯머신이 돌아가는 중입니다...**\n` 🔄 | 🔄 | 🔄 `"
        )
        msg = await interaction.original_response()

        # 2,000명 규모에서 동시에 요청이 몰려도 DB/Discord 작업이 폭주하지 않도록 제한합니다.
        async with slot_semaphore:
            if not deduct_slot_bet(guild_id, user_id, bet):
                current_coins = get_current_coins(guild_id, user_id)
                await msg.edit(
                    content=(
                        "❌ 슬롯머신을 이용할 수 없습니다. 현재 잔액이 부족합니다. "
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
                result_text = (
                    "🎉 **[잭팟 당첨! 3배 승리!]** "
                    f"배팅액의 3배인 **+{payout:,}코인**을 획득하셨습니다!"
                )
            elif multiplier > 0:
                result_text = f"✨ **[당첨!]** **+{payout:,}코인**을 획득하셨습니다!"
            else:
                result_text = "😢 **[꽝]** 아쉽게도 꽝입니다. 다음 기회에 도전해보세요!"

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
        print(f"[슬롯머신 명령어 오류] {type(e).__name__}: {e}")
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "⚠️ 슬롯머신 처리 중 오류가 발생했습니다.",
                    ephemeral=True,
                )
            else:
                await interaction.followup.send(
                    "⚠️ 슬롯머신 처리 중 오류가 발생했습니다.",
                    ephemeral=True,
                )
        except Exception as followup_error:
            print(f"[슬롯머신 오류 메시지 전송 실패] {followup_error}")
    finally:
        active_slot_spins.discard(spin_key)


@bot.tree.command(
    name="슬롯머신설정",
    description="[관리자 전용] 슬롯머신 환수율(RTP, %)을 조정합니다.",
)
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    rate="설정할 환수율 수치 (10~150%, 기본값 85%)"
)
async def set_slot_rtp(
    interaction: discord.Interaction,
    rate: int,
):
    """관리자가 서버별 슬롯머신 RTP를 변경합니다."""
    if interaction.guild_id is None or interaction.guild is None:
        await interaction.response.send_message(
            "❌ 서버에서만 사용할 수 있는 명령어입니다.",
            ephemeral=True,
        )
        return

    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message(
            "❌ 관리자만 사용할 수 있는 명령어입니다.",
            ephemeral=True,
        )
        return

    if rate < 10 or rate > 150:
        await interaction.response.send_message(
            "❌ 환수율은 **10~150%** 사이의 값으로 설정해주세요.",
            ephemeral=True,
        )
        return

    conn = None
    cursor = None
    previous_rtp = 85

    try:
        conn = get_db()
        cursor = conn.cursor()

        # 기존 DB에도 슬롯 RTP 컬럼이 반드시 존재하도록 보장합니다.
        cursor.execute("""
            ALTER TABLE guild_settings
            ADD COLUMN IF NOT EXISTS slot_rtp INTEGER DEFAULT 85
        """)

        cursor.execute(
            "SELECT slot_rtp FROM guild_settings WHERE guild_id = %s",
            (interaction.guild_id,),
        )
        previous_row = cursor.fetchone()
        if previous_row and previous_row[0] is not None:
            previous_rtp = int(previous_row[0])

        # 서버 설정 행이 없어도 새로 생성하고, 이미 있으면 RTP만 갱신합니다.
        cursor.execute(
            """
            INSERT INTO guild_settings (guild_id, slot_rtp)
            VALUES (%s, %s)
            ON CONFLICT (guild_id)
            DO UPDATE SET slot_rtp = EXCLUDED.slot_rtp
            """,
            (interaction.guild_id, rate),
        )
        conn.commit()

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"[슬롯머신 RTP 설정 오류] {type(e).__name__}: {e}")
        try:
            await interaction.response.send_message(
                "❌ 슬롯머신 환수율 설정 중 데이터베이스 오류가 발생했습니다. "
                "콘솔 로그의 '[슬롯머신 RTP 설정 오류]' 내용을 확인해주세요.",
                ephemeral=True,
            )
        except discord.InteractionResponded:
            await interaction.followup.send(
                "❌ 슬롯머신 환수율 설정 중 데이터베이스 오류가 발생했습니다.",
                ephemeral=True,
            )
        return
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    # RTP 저장이 성공한 뒤 로그 전송이 실패하더라도 설정 성공 메시지는 정상 출력합니다.
    try:
        await log_admin_action(
            interaction.guild,
            f"{interaction.user}님이 슬롯머신 환수율을 {previous_rtp}% → {rate}%로 변경함",
        )
    except Exception as e:
        print(f"[슬롯머신 RTP 설정 로그 오류] {type(e).__name__}: {e}")

    await interaction.response.send_message(
        f"⚙️ **[슬롯머신 환수율 변경]**\n"
        f"👤 변경자: {interaction.user.mention}\n"
        f"📊 변경 전: **{previous_rtp}%**\n"
        f"📈 변경 후: **{rate}%**\n"
        f"📝 관리자 로그에도 변경 내역이 기록되었습니다.",
        ephemeral=False,
    )


@bot.tree.command(
    name="채팅청소",
    description="지정한 유저의 채팅을 입력한 수량만큼 삭제합니다. (일반 유저는 본인만 가능, 관리자는 타인 가능)",
)
@app_commands.describe(
    member="청소할 대상 유저 (생략 시 본인)",
    count="삭제할 메시지 최대 수량 (1~100)"
)
async def clear_chat(
    interaction: discord.Interaction, member: discord.Member = None, count: int = 10
):
    await interaction.response.defer(thinking=True, ephemeral=False)

    if count < 1 or count > 100:
        await interaction.followup.send("❌ 삭제할 수량은 **1개 이상 100개 이하**로 입력해주세요.", ephemeral=True)
        return

    target = member or interaction.user
    is_admin = interaction.user.guild_permissions.administrator

    if not is_admin and target.id != interaction.user.id:
        await interaction.followup.send("❌ 일반 사용자는 **본인의 채팅만** 청소할 수 있습니다.", ephemeral=True)
        return

    is_admin_clearing_others = is_admin and target.id != interaction.user.id

    channel = interaction.channel
    deleted_count = 0
    now = datetime.now(timezone.utc)

    try:
        messages_to_delete = []
        async for message in channel.history(limit=1000):
            if message.author.id == target.id:
                messages_to_delete.append(message)
                if len(messages_to_delete) >= count:
                    break

        if not messages_to_delete:
            await interaction.followup.send(f"🧹 삭제할 수 있는 {target.mention}님의 최근 메시지가 없습니다.", ephemeral=True)
            return

        two_weeks_ago = now - timedelta(days=14)
        bulk_list = []
        old_list = []

        for msg in messages_to_delete:
            if msg.created_at > two_weeks_ago:
                bulk_list.append(msg)
            else:
                old_list.append(msg)

        if bulk_list:
            if len(bulk_list) == 1:
                await bulk_list[0].delete()
            else:
                await channel.delete_messages(bulk_list)
            deleted_count += len(bulk_list)

        for msg in old_list:
            try:
                await msg.delete()
                deleted_count += 1
                await asyncio.sleep(0.5)
            except Exception:
                pass

        if is_admin_clearing_others:
            await interaction.followup.send(
                f"🧹 **[관리자 청소]** {interaction.user.mention}님이 {target.mention}님의 메시지 **{deleted_count}개**를 삭제했습니다!"
            )
            await log_admin_action(interaction.guild, f"{interaction.user}님이 {target}님의 메시지 {deleted_count}개를 채널({channel.name})에서 청소함")
        else:
            await interaction.followup.send(
                f"🧹 본인의 메시지 **{deleted_count}개**를 청소했습니다!",
                ephemeral=True
            )

    except Exception as e:
        await interaction.followup.send(f"⚠️ 메시지 청소 중 오류가 발생했습니다: {e}", ephemeral=True)


class ConfirmClearAllView(discord.ui.View):
    def __init__(self, author_id: int):
        super().__init__(timeout=60)
        self.author_id = author_id

    @discord.ui.button(label="확인 (전체 삭제 진행)", style=discord.ButtonStyle.danger)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("명령어를 실행한 관리자만 누를 수 있습니다.", ephemeral=True)
            return

        for child in self.children:
            child.disabled = True

        await interaction.response.edit_message(
            content=f"🧹 **[전체 청소 진행 중]** 메시지를 삭제하고 있습니다. 잠시만 기다려주세요...",
            view=self
        )

        channel = interaction.channel
        now = datetime.now(timezone.utc)
        two_weeks_ago = now - timedelta(days=14)
        deleted_total = 0

        try:
            while True:
                messages = [msg async for msg in channel.history(limit=100)]
                if not messages:
                    break

                bulk_list = [msg for msg in messages if msg.created_at > two_weeks_ago]
                old_list = [msg for msg in messages if msg.created_at <= two_weeks_ago]

                if bulk_list:
                    if len(bulk_list) == 1:
                        await bulk_list[0].delete()
                    else:
                        await channel.delete_messages(bulk_list)
                    deleted_total += len(bulk_list)

                for msg in old_list:
                    try:
                        await msg.delete()
                        deleted_total += 1
                        await asyncio.sleep(0.5)
                    except Exception:
                        pass

                if len(messages) < 100:
                    break

            await channel.send(
                f"🧹 **[전체 청소 완료]** {interaction.user.mention}님에 의해 이 채널의 메시지 **{deleted_total}개**가 모두 청소되었습니다!"
            )
            await log_admin_action(interaction.guild, f"{interaction.user}님이 채널({channel.name})의 전체 메시지 {deleted_total}개를 청소함")

        except Exception as e:
            await channel.send(f"⚠️ 전체 채널 청소 중 오류가 발생했습니다: {e}")

        self.stop()

    @discord.ui.button(label="취소", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("명령어를 실행한 관리자만 누를 수 있습니다.", ephemeral=True)
            return

        for child in self.children:
            child.disabled = True

        await interaction.response.edit_message(
            content=f"❌ 전체 채널 청소가 취소되었습니다.",
            view=self
        )
        self.stop()


@bot.tree.command(
    name="전체청소",
    description="[관리자 전용] 현재 채널의 모든 채팅을 확인 절차를 거쳐 모두 삭제합니다.",
)
@app_commands.default_permissions(administrator=True)
async def clear_all_chat(interaction: discord.Interaction):
    view = ConfirmClearAllView(interaction.user.id)
    await interaction.response.send_message(
        f"⚠️ **{interaction.user.mention}님이 현재 채널의 전체 채팅 삭제를 요청했습니다!**\n정말로 이 채널의 모든 메시지를 전부 삭제하시겠습니까? (이 작업은 되돌릴 수 없습니다)",
        view=view,
        ephemeral=True
    )


@bot.tree.command(
    name="코인지급",
    description="[관리자 전용] 특정 유저의 코인을 지급하거나 차감합니다.",
)
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    member="대상을 선택하세요", amount="지급할 코인 양 (차감은 마이너스 입력)"
)
async def admin_coin(
    interaction: discord.Interaction, member: discord.Member, amount: int
):
    await interaction.response.defer(thinking=True)
    
    guild_id = interaction.guild_id
    target_id = member.id

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO users (guild_id, user_id, coins, voice_minutes, warnings, defense_tickets)
        VALUES (%s, %s, %s, 0, 0, 0)
        ON CONFLICT (guild_id, user_id) DO UPDATE SET coins = users.coins + EXCLUDED.coins
    """,
        (guild_id, target_id, amount),
    )

    conn.commit()
    cursor.execute(
        "SELECT coins FROM users WHERE guild_id = %s AND user_id = %s",
        (guild_id, target_id),
    )
    new_coins = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    if amount > 0:
        action = (
            f"{interaction.user.mention}님이 {member.mention}님에게 "
            f"코인 **{amount:,}개**를 지급했습니다."
        )
    elif amount < 0:
        action = (
            f"{interaction.user.mention}님이 {member.mention}님의 코인 "
            f"**{abs(amount):,}개**를 차감했습니다."
        )
    else:
        action = (
            f"{interaction.user.mention}님이 {member.mention}님의 코인을 "
            "변경하지 않았습니다. (수량: 0개)"
        )

    log_msg = f"{interaction.user}님이 {member}님의 코인을 {amount}만큼 조정함 (현재 잔액: {new_coins:,}코인)"
    await log_admin_action(interaction.guild, log_msg)

    await interaction.followup.send(
        f"⚙️ {action}\n현재 잔액: **{new_coins:,}코인**",
        allowed_mentions=discord.AllowedMentions.none(),
    )


@bot.tree.command(
    name="경고지급",
    description="[관리자 전용] 특정 유저의 경고 횟수를 부여하거나 차감합니다. (방어권이 있으면 방어권 우선 소모)",
)
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    member="대상을 선택하세요", count="부여할 횟수 (차감은 마이너스 입력, 예: -1)"
)
async def give_warning(
    interaction: discord.Interaction, member: discord.Member, count: int
):
    await interaction.response.defer(thinking=True)

    if count == 0:
        await interaction.followup.send("경고 변동 횟수는 0이 될 수 없습니다. (지급은 양수, 차감은 음수 입력)")
        return

    guild_id = interaction.guild_id
    target_id = member.id

    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute(
        """
        INSERT INTO users (guild_id, user_id, coins, voice_minutes, warnings, defense_tickets)
        VALUES (%s, %s, 0, 0, 0, 0)
        ON CONFLICT (guild_id, user_id) DO NOTHING
        """,
        (guild_id, target_id),
    )

    cursor.execute(
        "SELECT warnings, defense_tickets FROM users WHERE guild_id = %s AND user_id = %s",
        (guild_id, target_id),
    )
    row = cursor.fetchone()
    current_warnings = row[0]
    current_defense = row[1]

    if count > 0:
        applied_warnings_count = 0
        used_defense_count = 0
        
        temp_warnings = current_warnings
        temp_defense = current_defense
        
        for _ in range(count):
            if temp_defense > 0:
                temp_defense -= 1
                used_defense_count += 1
            else:
                temp_warnings += 1
                applied_warnings_count += 1
                
        new_warnings = temp_warnings
        new_defense = temp_defense
        
        if used_defense_count > 0 and applied_warnings_count == 0:
            action_desc = f"방어권 **{used_defense_count}개**가 소모되어 경고가 방어되었습니다!"
            log_text = f"{interaction.user}님이 {member}님에게 경고 {count}회를 부여하려 했으나 방어권 {used_defense_count}개로 방어됨"
        elif used_defense_count > 0:
            action_desc = f"방어권 **{used_defense_count}개**가 소모되고, 경고 **{applied_warnings_count}회**가 부여되었습니다."
            log_text = f"{interaction.user}님이 {member}님에게 방어권 {used_defense_count}개 소모 및 경고 {applied_warnings_count}회 부여함"
        else:
            action_desc = f"경고 **{count}회**가 부여되었습니다."
            log_text = f"{interaction.user}님이 {member}님에게 경고 {count}회를 부여함"

        cursor.execute(
            """
            UPDATE users 
            SET warnings = %s, defense_tickets = %s
            WHERE guild_id = %s AND user_id = %s
            """,
            (new_warnings, new_defense, guild_id, target_id)
        )

    else:
        deduct_amount = abs(count)
        
        if current_warnings >= deduct_amount:
            new_warnings = current_warnings - deduct_amount
            new_defense = current_defense
            action_desc = f"경고 **{deduct_amount}회**가 차감되었습니다."
            log_text = f"{interaction.user}님이 {member}님의 경고 {deduct_amount}회를 차감함"
        else:
            leftover = deduct_amount - current_warnings
            new_warnings = 0
            new_defense = current_defense + leftover
            if current_warnings > 0:
                action_desc = f"경고 **{current_warnings}회**가 모두 소진되고, 초과된 **{leftover}회**만큼 **방어권 {leftover}개**로 적립되었습니다."
                log_text = f"{interaction.user}님이 {member}님의 경고를 모두 차감하고 초과분 {leftover}회를 방어권으로 전환함"
            else:
                action_desc = f"보유 중인 경고가 없어, 차감 수량만큼 **방어권 {leftover}개**가 충전되었습니다."
                log_text = f"{interaction.user}님이 {member}님에게 방어권 {leftover}개를 충전함"

        cursor.execute(
            """
            UPDATE users 
            SET warnings = %s, defense_tickets = %s
            WHERE guild_id = %s AND user_id = %s
            """,
            (new_warnings, new_defense, guild_id, target_id)
        )

    conn.commit()
    cursor.execute(
        "SELECT warnings, defense_tickets FROM users WHERE guild_id = %s AND user_id = %s",
        (guild_id, target_id),
    )
    final_row = cursor.fetchone()
    total_warnings = final_row[0]
    total_defense = final_row[1]
    cursor.close()
    conn.close()

    detailed_log_text = f"{log_text} (현재 누적 경고: **{total_warnings}회**, 방어권: **{total_defense}개**)"
    await log_admin_action(interaction.guild, detailed_log_text)

    if count > 0 and applied_warnings_count > 0 and total_warnings >= 3:
        try:
            await interaction.guild.ban(member, reason=f"경고 3회 누적 자동 차단 (관리자: {interaction.user})")
            ban_log_msg = f"🚨 {member}님이 경고 3회 누적으로 자동 차단됨 (최종 경고: {total_warnings}회)"
            await log_admin_action(interaction.guild, ban_log_msg)
            
            await interaction.followup.send(
                f"🚨 **[경고 누적 차단]** {member.mention}님이 경고 3회를 초과(`누적 {total_warnings}회`)하여 **서버에서 자동으로 차단(밴)** 되었습니다!\n"
                f"⚠️ 대상자 현재 상태 — 경고: **{total_warnings}회**, 방어권: **{total_defense}개**"
            )
        except Exception as e:
            await interaction.followup.send(
                f"⚠ 경고가 부여되었으나, 봇의 권한 부족으로 차단에 실패했습니다. (권한을 확인해주세요)\n오류: {e}\n"
                f"⚠️ 대상자 현재 상태 — 경고: **{total_warnings}회**, 방어권: **{total_defense}개**"
            )
    else:
        await interaction.followup.send(
            f"⚠️ {member.mention}님에게 {action_desc}\n"
            f"⚠️ 대상자 현재 상태 — 경고: **{total_warnings}회**, 방어권: **{total_defense}개**",
            allowed_mentions=discord.AllowedMentions.none()
        )


@bot.tree.command(
    name="보상설정",
    description="[관리자 전용] 음성 채널 30분 이용 시 지급될 코인 양을 설정합니다.",
)
@app_commands.default_permissions(administrator=True)
@app_commands.describe(amount="음성 채널을 누적 30분 이용할 때 지급할 코인 수")
async def set_voice_reward(interaction: discord.Interaction, amount: int):
    await interaction.response.defer(thinking=True)
    
    if amount < 0:
        await interaction.followup.send("보상 코인은 0 이상으로 설정해야 합니다.")
        return

    guild_id = interaction.guild_id
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO guild_settings (guild_id, voice_reward_rate)
        VALUES (%s, %s)
        ON CONFLICT (guild_id) DO UPDATE SET voice_reward_rate = EXCLUDED.voice_reward_rate
    """,
        (guild_id, amount),
    )

    conn.commit()
    cursor.close()
    conn.close()

    await log_admin_action(interaction.guild, f"{interaction.user}님이 음성 30분당 보상 코인을 {amount}개로 설정함")

    await interaction.followup.send(
        f"⚙ [관리자 설정 완료] 앞으로 음성 채널 누적 **30분마다 {amount}코인**이 지급됩니다."
    )


@bot.tree.command(
    name="추천보상설정",
    description="[관리자 전용] 추천인 등록 시 추천인에게 지급되는 코인 수를 설정합니다.",
)
@app_commands.default_permissions(administrator=True)
@app_commands.describe(amount="추천인 등록이 성공할 때 추천인에게 지급할 코인 수 (0 이상)")
async def set_referral_reward(interaction: discord.Interaction, amount: int):
    await interaction.response.defer(thinking=True)
    
    if amount < 0:
        await interaction.followup.send("추천 보상 코인은 0 이상으로 설정해야 합니다.")
        return

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO guild_settings (guild_id, referral_reward)
        VALUES (%s, %s)
        ON CONFLICT (guild_id) DO UPDATE SET referral_reward = EXCLUDED.referral_reward
        """,
        (interaction.guild_id, amount),
    )
    conn.commit()
    cursor.close()
    conn.close()

    await log_admin_action(interaction.guild, f"{interaction.user}님이 추천 보상 코인을 {amount}개로 설정함")

    await interaction.followup.send(
        f"⚙ [관리자 설정 완료] 추천인 등록 성공 시 추천인에게 **{amount}코인**을 지급합니다."
    )


class ConfirmLogChangeView(discord.ui.View):
    def __init__(self, author_id: int, guild_id: int, new_channel_id: int):
        super().__init__(timeout=60)
        self.author_id = author_id
        self.guild_id = guild_id
        self.new_channel_id = new_channel_id

    @discord.ui.button(label="변경하기", style=discord.ButtonStyle.danger)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("명령어를 실행한 관리자만 누를 수 있습니다.", ephemeral=True)
            return

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO guild_settings (guild_id, log_channel_id)
            VALUES (%s, %s)
            ON CONFLICT (guild_id) DO UPDATE SET log_channel_id = EXCLUDED.log_channel_id
            """,
            (self.guild_id, self.new_channel_id),
        )
        conn.commit()
        cursor.close()
        conn.close()

        for child in self.children:
            child.disabled = True

        await log_admin_action(interaction.guild, f"{interaction.user}님이 관리자 로그 채널을 이 채널로 변경함")

        await interaction.response.edit_message(
            content=f"🛡 [관리자 설정 완료] 이 채널({interaction.channel.mention})이 새로운 관리자 명령어 로그 기록 채널로 변경되었습니다.",
            view=self
        )
        self.stop()

    @discord.ui.button(label="취소", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("명령어를 실행한 관리자만 누를 수 있습니다.", ephemeral=True)
            return

        for child in self.children:
            child.disabled = True

        await interaction.response.edit_message(
            content=f"❌ 로그 채널 변경이 취소되었습니다. 기존 로그 채널이 유지됩니다.",
            view=self
        )
        self.stop()


@bot.tree.command(
    name="로그",
    description="[관리자 전용] 관리자 명령어 실행 기록이 남을 채널을 현재 채널로 설정합니다.",
)
@app_commands.default_permissions(administrator=True)
async def set_log_channel(interaction: discord.Interaction):
    guild_id = interaction.guild_id
    channel_id = interaction.channel_id

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT log_channel_id FROM guild_settings WHERE guild_id = %s", (guild_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if row and row[0]:
        existing_channel_id = row[0]
        existing_channel = interaction.guild.get_channel(existing_channel_id)
        existing_channel_mention = existing_channel.mention if existing_channel else f"<#{existing_channel_id}>"

        view = ConfirmLogChangeView(interaction.user.id, guild_id, channel_id)
        await interaction.response.send_message(
            f"⚠️ **이미 이 서버에는 지정된 관리자 로그 채널({existing_channel_mention})이 존재합니다!**\n"
            f"새로운 채널({interaction.channel.mention})로 로그 채널을 변경하시겠습니까?",
            view=view
        )
    else:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO guild_settings (guild_id, log_channel_id)
            VALUES (%s, %s)
            ON CONFLICT (guild_id) DO UPDATE SET log_channel_id = EXCLUDED.log_channel_id
            """,
            (guild_id, channel_id),
        )
        conn.commit()
        cursor.close()
        conn.close()

        await interaction.response.send_message(
            f"🛡 [관리자 설정 완료] 이 채널({interaction.channel.mention})이 관리자 명령어 로그 기록 채널로 설정되었습니다."
        )
        await log_admin_action(interaction.guild, f"{interaction.user}님이 이 채널을 관리자 로그 채널로 지정함")


class ConfirmResetView(discord.ui.View):
    def __init__(self, author_id: int):
        super().__init__(timeout=60)
        self.author_id = author_id

    @discord.ui.button(label="확인 (초기화 진행)", style=discord.ButtonStyle.danger)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("명령어를 실행한 관리자만 누를 수 있습니다.", ephemeral=True)
            return

        guild_id = interaction.guild_id
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET coins = 0 WHERE guild_id = %s", (guild_id,))
        conn.commit()
        cursor.close()
        conn.close()

        for child in self.children:
            child.disabled = True

        await log_admin_action(interaction.guild, f"{interaction.user}님이 서버 내 모든 유저의 코인 전체 초기화를 최종 승인 및 실행함")

        await interaction.response.edit_message(
            content=f"⚠️ **[관리자 초기화 완료]** {interaction.user.mention}님이 이 서버의 모든 유저 코인을 **0개**로 초기화했습니다.",
            view=self
        )
        self.stop()

    @discord.ui.button(label="취소", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("명령어를 실행한 관리자만 누를 수 있습니다.", ephemeral=True)
            return

        for child in self.children:
            child.disabled = True

        await log_admin_action(interaction.guild, f"{interaction.user}님이 코인 전체 초기화 요청을 취소함")

        await interaction.response.edit_message(
            content=f"❌ {interaction.user.mention}님에 의해 코인 초기화가 취소되었습니다.",
            view=self
        )
        self.stop()


@bot.tree.command(
    name="코인초기화",
    description="[관리자 전용] 이 서버의 모든 유저 코인을 공개 경고창을 통해 0으로 초기화합니다.",
)
@app_commands.default_permissions(administrator=True)
async def reset_all_coins(interaction: discord.Interaction):
    await log_admin_action(interaction.guild, f"{interaction.user}님이 코인 전체 초기화 명령어를 실행(요청)함")

    view = ConfirmResetView(interaction.user.id)
    await interaction.response.send_message(
        f"⚠️ **{interaction.user.mention}님이 코인 전체 초기화를 요청했습니다!**\n정말로 이 서버의 모든 유저 코인을 0으로 초기화하시겠습니까? 이 작업은 되돌릴 수 없습니다.",
        view=view
    )


@bot.tree.command(
    name="코인순위", description="이 서버에서 코인이 많은 상위 10명을 확인합니다."
)
async def coin_ranking(interaction: discord.Interaction):
    guild_id = interaction.guild_id
    if guild_id is None:
        await interaction.response.send_message(
            "이 명령어는 서버 안에서만 사용할 수 있습니다.", ephemeral=True
        )
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
        description="이 서버에서 사용할 수 있는 명령어입니다. 사용자 권한별로 분류되어 있습니다.",
        color=discord.Color.blue(),
    )
    
    embed.add_field(
        name="👤 일반 사용자용 명령어",
        value=(
            "• `/정보 [유저]` — 본인 또는 다른 유저의 코인 잔액, 음성 접속 시간, 경고 횟수, 방어권을 확인합니다.\n"
            "• `/추천인 [유저]` — 나를 초대해준 사람을 추천인으로 등록합니다. (음성 접속 30분 이상 시 가능)\n"
            "• `/슬롯머신 [배팅액]` — 1~5,000코인을 배팅하여 슬롯머신(최대 3배 배율)을 돌립니다.\n"
            "• `/채팅청소 [유저] [수량]` — 최근 채팅을 수량만큼 삭제합니다. **(일반 유저는 본인 채팅만 삭제 가능)**\n"
            "• `/코인순위` — 이 서버의 코인 보유량 상위 10명을 확인합니다.\n"
            "• `/명령어` — 봇의 전체 명령어 안내를 확인합니다."
        ),
        inline=False,
    )
    
    embed.add_field(
        name="🛡️ 관리자 전용 명령어",
        value=(
            "• `/슬롯머신설정 [확률]` — 슬롯머신의 환수율(RTP)을 조정합니다.\n"
            "• `/채팅청소 [타유저] [수량]` — **관리자 권한**으로 다른 유저의 채팅을 지정한 수량만큼 강제로 청소할 수 있습니다. (공개 출력)\n"
            "• `/전체청소` — **관리자 권한**으로 현재 채널의 모든 채팅을 확인창을 거쳐 전부 비우고 완료 메시지를 남깁니다.\n"
            "• `/코인지급 [유저] [수량]` — 특정 유저의 코인을 지급하거나 차감합니다. (차감은 마이너스 입력)\n"
            "• `/경고지급 [유저] [횟수]` — 경고를 부여하거나 차감합니다. (방어권 우선 소모, 음수 입력 시 경고 차감/방어권 충전, 3회 누적 시 자동 밴)\n"
            "• `/보상설정 [수량]` — 음성 채널 누적 30분 이용 시 지급될 코인 양을 설정합니다.\n"
            "• `/추천보상설정 [수량]` — 추천인 등록 성공 시 추천인에게 지급할 코인 수를 설정합니다.\n"
            "• `/코인초기화` — 서버 내 모든 유저의 코인을 공개 경고창을 통해 0으로 초기화합니다.\n"
            "• `/로그` — 관리자 명령어 실행 기록이 남을 채널을 현재 채널로 설정합니다."
        ),
        inline=False,
    )

    await interaction.response.send_message(embed=embed, ephemeral=True)


if __name__ == "__main__":
    keep_alive()
    
    # Render 및 일반 환경에서 사용하는 DISCORD_TOKEN을 우선적으로 안전하게 로드합니다.
    token = os.getenv("DISCORD_TOKEN") or os.getenv("DISCORD_BOT_TOKEN")
    
    if not token:
        print("❌ 에러: DISCORD_TOKEN 환경 변수가 설정되지 않았습니다! Render 대시보드의 Environment 설정을 확인해주세요.")
        exit(1)
        
    bot.run(token)

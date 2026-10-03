import os
import sqlite3
import time
from threading import Thread
from datetime import datetime, timezone, timedelta
import discord
from discord import app_commands
from discord.ext import commands, tasks
from flask import Flask

# 0. 렌더(Render) 24시간 유지용 Flask 웹서버 설정
app = Flask('')

@app.route('/')
def home():
    return "Bot is running!"

def run():
    # Render가 할당해주는 PORT 환경 변수를 사용 (기본값: 10000)
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run)
    t.start()


VOICE_REWARD_INTERVAL_MINUTES = 30
KST = timezone(timedelta(hours=9)) # 한국 표준시 (UTC+9)


# 1. 데이터베이스 초기화 및 연결 함수
def init_db():
    conn = sqlite3.connect("bot_database_per_guild.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            guild_id INTEGER,
            user_id INTEGER,
            coins INTEGER DEFAULT 0,
            voice_minutes INTEGER DEFAULT 0,
            referred_by INTEGER DEFAULT NULL,
            PRIMARY KEY (guild_id, user_id)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS voice_sessions (
            guild_id INTEGER,
            user_id INTEGER,
            join_time REAL,
            PRIMARY KEY (guild_id, user_id)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS guild_settings (
            guild_id INTEGER PRIMARY KEY,
            voice_reward_rate INTEGER DEFAULT 1,
            referral_reward INTEGER NOT NULL DEFAULT 30,
            log_channel_id INTEGER DEFAULT NULL
        )
    """)
    cursor.execute("PRAGMA table_info(guild_settings)")
    settings_columns = {row[1] for row in cursor.fetchall()}
    if "referral_reward" not in settings_columns:
        cursor.execute(
            "ALTER TABLE guild_settings ADD COLUMN referral_reward INTEGER NOT NULL DEFAULT 30"
        )
    if "log_channel_id" not in settings_columns:
        cursor.execute(
            "ALTER TABLE guild_settings ADD COLUMN log_channel_id INTEGER DEFAULT NULL"
        )
    conn.commit()
    conn.close()


init_db()

intents = discord.Intents.default()
intents.guilds = True
intents.voice_states = True
intents.members = True
intents.message_content = True  # 디스코드 개발자 포털 설정과 일치시킴

bot = commands.Bot(command_prefix="!", intents=intents)
commands_synced = False


def get_db():
    return sqlite3.connect("bot_database_per_guild.db")


# 관리자 명령어 사용 로그를 기록하는 헬퍼 함수 (한국 시간 적용 및 중복 방지)
async def log_admin_action(guild: discord.Guild, action_text: str):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT log_channel_id FROM guild_settings WHERE guild_id = ?", (guild.id,))
    row = cursor.fetchone()
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


# 2. 매분 음성 채널 접속 유저 확인 및 보상 지급 (마이크/헤드셋 음소거 시 제외)
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
                "SELECT voice_reward_rate FROM guild_settings WHERE guild_id = ?",
                (guild_id,),
            )
            setting = cursor.fetchone()
            reward_rate = setting[0] if setting else 1

            cursor.execute(
                """
                INSERT OR IGNORE INTO users (guild_id, user_id, coins, voice_minutes)
                VALUES (?, ?, 0, 0)
                """,
                (guild_id, user_id),
            )
            
            cursor.execute(
                "SELECT coins, voice_minutes FROM users WHERE guild_id = ? AND user_id = ?",
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
                SET coins = coins + ?,
                    voice_minutes = ?
                WHERE guild_id = ? AND user_id = ?
                """,
                (added_coins, new_minutes, guild_id, user_id)
            )
        else:
            cursor.execute(
                "DELETE FROM voice_sessions WHERE guild_id = ? AND user_id = ?",
                (guild_id, user_id),
            )

    conn.commit()
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
            INSERT OR REPLACE INTO voice_sessions (guild_id, user_id, join_time)
            VALUES (?, ?, ?)
            """,
            (guild_id, user_id, time.time()),
        )
    else:
        cursor.execute(
            "DELETE FROM voice_sessions WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        )

    conn.commit()
    conn.close()


# 3. 슬래시 명령어 그룹 (일반 명령어)
@bot.tree.command(
    name="정보", description="본인 또는 선택한 사용자의 코인과 음성 접속 시간을 확인합니다."
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
        "SELECT coins, voice_minutes FROM users WHERE guild_id = ? AND user_id = ?",
        (guild_id, user_id),
    )
    row = cursor.fetchone()
    conn.close()

    coins = row[0] if row else 0
    minutes = row[1] if row else 0

    await interaction.response.send_message(
        f"📊 **{target.name}**님의 서버 활동 정보:\n- 코인: **{coins}개**\n- 음성 접속 시간: **{minutes}분**",
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
        "SELECT voice_minutes, referred_by FROM users WHERE guild_id = ? AND user_id = ?",
        (guild_id, user_id),
    )
    row = cursor.fetchone()

    if row and row[1] is not None:
        conn.close()
        await interaction.response.send_message(
            "이미 추천인을 등록하셨습니다.", ephemeral=True
        )
        return

    user_minutes = row[0] if row else 0
    if user_minutes < 30:
        conn.close()
        await interaction.response.send_message(
            f"❌ 음성 접속 시간이 **30분 이상**일 때만 추천인 등록이 가능합니다. (현재: {user_minutes}분)",
            ephemeral=True,
        )
        return

    cursor.execute(
        "SELECT referral_reward FROM guild_settings WHERE guild_id = ?",
        (guild_id,),
    )
    setting = cursor.fetchone()
    referral_reward = setting[0] if setting else 30

    cursor.execute(
        """
        INSERT INTO users (guild_id, user_id, coins, voice_minutes, referred_by)
        VALUES (?, ?, 0, ?, ?)
        ON CONFLICT(guild_id, user_id) DO UPDATE SET referred_by = ?
    """,
        (guild_id, user_id, user_minutes, referrer.id, referrer.id),
    )

    cursor.execute(
        """
        INSERT INTO users (guild_id, user_id, coins, voice_minutes)
        VALUES (?, ?, ?, 0)
        ON CONFLICT(guild_id, user_id) DO UPDATE SET coins = coins + ?
    """,
        (guild_id, referrer.id, referral_reward, referral_reward),
    )

    conn.commit()
    conn.close()

    await interaction.response.send_message(
        f"✅ 성공적으로 {referrer.mention}님을 추천인으로 등록했습니다! 추천인에게 **{referral_reward}코인**이 지급되었습니다.",
        ephemeral=True,
    )


# ----------------------------------------------------
# 관리자 명령어들 (default_permissions로 관리자 외 목록 비노출 처리)
# ----------------------------------------------------

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
        INSERT INTO users (guild_id, user_id, coins, voice_minutes)
        VALUES (?, ?, ?, 0)
        ON CONFLICT(guild_id, user_id) DO UPDATE SET coins = coins + ?
    """,
        (guild_id, target_id, amount, amount),
    )

    conn.commit()
    cursor.execute(
        "SELECT coins FROM users WHERE guild_id = ? AND user_id = ?",
        (guild_id, target_id),
    )
    new_coins = cursor.fetchone()[0]
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
        VALUES (?, ?)
        ON CONFLICT(guild_id) DO UPDATE SET voice_reward_rate = ?
    """,
        (guild_id, amount, amount),
    )

    conn.commit()
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
        VALUES (?, ?)
        ON CONFLICT(guild_id) DO UPDATE SET referral_reward = excluded.referral_reward
        """,
        (interaction.guild_id, amount),
    )
    conn.commit()
    conn.close()

    await log_admin_action(interaction.guild, f"{interaction.user}님이 추천 보상 코인을 {amount}개로 설정함")

    await interaction.followup.send(
        f"⚙️ [관리자 설정 완료] 추천인 등록 성공 시 추천인에게 **{amount}코인**을 지급합니다."
    )


# 로그 채널 변경 확인 버튼 뷰
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
            VALUES (?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET log_channel_id = excluded.log_channel_id
            """,
            (self.guild_id, self.new_channel_id),
        )
        conn.commit()
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
    cursor.execute("SELECT log_channel_id FROM guild_settings WHERE guild_id = ?", (guild_id,))
    row = cursor.fetchone()
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
            VALUES (?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET log_channel_id = excluded.log_channel_id
            """,
            (guild_id, channel_id),
        )
        conn.commit()
        conn.close()

        await interaction.response.send_message(
            f"🛡 [관리자 설정 완료] 이 채널({interaction.channel.mention})이 관리자 명령어 로그 기록 채널로 설정되었습니다."
        )
        await log_admin_action(interaction.guild, f"{interaction.user}님이 이 채널을 관리자 로그 채널로 지정함")


# 코인 초기화 확인 버튼 뷰
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
        cursor.execute("UPDATE users SET coins = 0 WHERE guild_id = ?", (guild_id,))
        conn.commit()
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
            WHERE guild_id = ? AND coins > 0
            ORDER BY coins DESC, user_id ASC
            LIMIT 10
            """,
            (guild_id,),
        )
        rows = cursor.fetchall()
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
    name="명령어", description="봇에서 사용할 수 있는 명령어 목록을 확인합니다."
)
async def show_commands(interaction: discord.Interaction):
    embed = discord.Embed(
        title="🤖 봇 명령어 안내",
        description="이 서버에서 사용할 수 있는 명령어입니다.",
        color=discord.Color.blue(),
    )
    embed.add_field(
        name="일반 명령어",
        value=(
            "`/정보` — 내 코인 잔액과 음성 접속 시간을 확인합니다.\n"
            "`/추천인 [유저]` — 추천인을 등록합니다. 음성 접속 시간이 30분 이상이어야 합니다.\n"
            "`/코인순위` — 이 서버의 코인 보유량 상위 10명을 확인합니다."
        ),
        inline=False,
    )
    embed.add_field(
        name="관리자 명령어",
        value=(
            "`/코인지급 [유저] [수량]` — 코인을 조정합니다. 음수 입력은 차감입니다.\n"
            "`/보상설정 [수량]` — 음성 접속 누적 30분마다 지급할 코인을 설정합니다.\n"
            "`/추천보상설정 [수량]` — 추천인 등록 성공 시 지급할 코인을 설정합니다.\n"
            "`/코인초기화` — 서버 내 모든 유저의 코인을 공개 경고창을 통해 0으로 초기화합니다.\n"
            "`/로그` — 관리자 명령어 실행 기록을 남길 채널을 설정합니다."
        ),
        inline=False,
    )
    embed.add_field(
        name="도움말",
        value="`/명령어` — 이 명령어 안내를 다시 표시합니다.",
        inline=False,
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


if __name__ == "__main__":
    keep_alive() # Flask 서버 가동 (Render 포트 바인딩 해결)
    token = os.getenv("DISCORD_BOT_TOKEN") or os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("DISCORD_TOKEN or DISCORD_BOT_TOKEN must be configured.")
    bot.run(token)

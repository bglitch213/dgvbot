import os
import sqlite3
import time
import asyncio
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
            warnings INTEGER DEFAULT 0,
            defense_tickets INTEGER DEFAULT 0,
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
    
    # 기존 데이터베이스 호환을 위한 컬럼 자동 추가 체크
    cursor.execute("PRAGMA table_info(users)")
    users_columns = {row[1] for row in cursor.fetchall()}
    if "warnings" not in users_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN warnings INTEGER DEFAULT 0")
    if "defense_tickets" not in users_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN defense_tickets INTEGER DEFAULT 0")

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
                INSERT OR IGNORE INTO users (guild_id, user_id, coins, voice_minutes, warnings, defense_tickets)
                VALUES (?, ?, 0, 0, 0, 0)
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
        "SELECT coins, voice_minutes, warnings, defense_tickets FROM users WHERE guild_id = ? AND user_id = ?",
        (guild_id, user_id),
    )
    row = cursor.fetchone()
    conn.close()

    coins = row[0] if row else 0
    minutes = row[1] if row else 0
    warnings = row[2] if row else 0
    defense_tickets = row[3] if row else 0

    await interaction.response.send_message(
        f"**{target.name}**님의 서버 활동 정보:\n"
        f"- 🪙 대깨 코인: **{coins}개**\n"
        f"- ⌛ 음성 접속 시간: **{minutes}분**\n"
        f"- ⚠️ 경고 횟수: **{warnings}회** (3회 누적 시 차단)\n"
        f"- 🛡️ 방어권: **{defense_tickets}개**",
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
    await interaction.response.defer(ephemeral=True)

    if count < 1 or count > 100:
        await interaction.followup.send("❌ 삭제할 수량은 **1개 이상 100개 이하**로 입력해주세요.", ephemeral=True)
        return

    target = member or interaction.user
    is_admin = interaction.user.guild_permissions.administrator

    if not is_admin and target.id != interaction.user.id:
        await interaction.followup.send("❌ 일반 사용자는 **본인의 채팅만** 청소할 수 있습니다.", ephemeral=True)
        return

    channel = interaction.channel
    deleted_count = 0
    now = datetime.now(timezone.utc)

    try:
        messages_to_delete = []
        async for message in channel.history(limit=200):
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

        # 🧹 누가 몇 개의 메시지를 지웠는지 명시하여 출력
        await interaction.followup.send(
            f"🧹 **{interaction.user.name}**님이 **{target.name}**님의 메시지 **{deleted_count}개**를 성공적으로 청소했습니다!",
            ephemeral=True
        )

        # 관리자일 경우 타인의 채팅을 지웠다면 로그 채널에 기록
        if is_admin:
            await log_admin_action(interaction.guild, f"{interaction.user}님이 {target}님의 메시지 {deleted_count}개를 채널({channel.name})에서 청소함")

    except Exception as e:
        await interaction.followup.send(f"⚠️ 메시지 청소 중 오류가 발생했습니다: {e}", ephemeral=True)


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
        INSERT INTO users (guild_id, user_id, coins, voice_minutes, warnings, defense_tickets)
        VALUES (?, ?, ?, 0, 0, 0)
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
    name="경고지급",
    description="[관리자 전용] 특정 유저의 경고 횟수를 부여하거나 차감합니다. (음수 입력 시 차감, 경고 소진 후 방어권 충전)",
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
        VALUES (?, ?, 0, 0, 0, 0)
        ON CONFLICT(guild_id, user_id) DO NOTHING
        """,
        (guild_id, target_id),
    )

    cursor.execute(
        "SELECT warnings, defense_tickets FROM users WHERE guild_id = ? AND user_id = ?",
        (guild_id, target_id),
    )
    row = cursor.fetchone()
    current_warnings = row[0]
    current_defense = row[1]

    if count > 0:
        new_warnings = current_warnings + count
        new_defense = current_defense
        cursor.execute(
            """
            UPDATE users 
            SET warnings = ?
            WHERE guild_id = ? AND user_id = ?
            """,
            (new_warnings, guild_id, target_id)
        )
        action_desc = f"경고 **{count}회**가 부여되었습니다."
        log_text = f"{interaction.user}님이 {member}님에게 경고 {count}회를 부여함"
    else:
        deduct_amount = abs(count)
        
        # 💡 수정된 부분: 경고가 남아있다면 경고를 우선 차감하고, 남은 차감 수량이 있을 때만 방어권 충전
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
            SET warnings = ?, defense_tickets = ?
            WHERE guild_id = ? AND user_id = ?
            """,
            (new_warnings, new_defense, guild_id, target_id)
        )

    conn.commit()
    cursor.execute(
        "SELECT warnings, defense_tickets FROM users WHERE guild_id = ? AND user_id = ?",
        (guild_id, target_id),
    )
    final_row = cursor.fetchone()
    total_warnings = final_row[0]
    total_defense = final_row[1]
    conn.close()

    # 🛡️ 로그 채널에 현재 경고 및 방어권 횟수 전체가 표시되도록 기록 상세화
    detailed_log_text = f"{log_text} (현재 누적 경고: **{total_warnings}회**, 방어권: **{total_defense}개**)"
    await log_admin_action(interaction.guild, detailed_log_text)

    if count > 0 and total_warnings >= 3:
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
                f"⚠️ 경고가 {total_warnings}회 부여되었으나, 봇의 권한 부족으로 차단에 실패했습니다. (권한을 확인해주세요)\n오류: {e}\n"
                f"⚠️ 대상자 현재 상태 — 경고: **{total_warnings}회**, 방어권: **{total_defense}개**"
            )
    else:
        # ⚠️ 경고 지급 후 그 사람의 경고 횟수를 전부 표시
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
        f"⚙️️ [관리자 설정 완료] 추천인 등록 성공 시 추천인에게 **{amount}코인**을 지급합니다."
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
            content=f"🛡️ [관리자 설정 완료] 이 채널({interaction.channel.mention})이 새로운 관리자 명령어 로그 기록 채널로 변경되었습니다.",
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
            f"🛡️ [관리자 설정 완료] 이 채널({interaction.channel.mention})이 관리자 명령어 로그 기록 채널로 설정되었습니다."
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
    name="명령어", description="봇이 사용할 수 있는 명령어 목록을 확인합니다."
)
async def show_commands(interaction: discord.Interaction):
    embed = discord.Embed(
        title="🤖 봇 명령어 안내",
        description="이 서버에서 사용할 수 있는 명령어입니다. 사용자 권한별로 분류되어 있습니다.",
        color=discord.Color.blue(),
    )
    
    # 👤 일반 사용자용 명령어
    embed.add_field(
        name="👤 일반 사용자용 명령어",
        value=(
            "• `/정보 [유저]` — 본인 또는 다른 유저의 코인 잔액, 음성 접속 시간, 경고 횟수, 방어권을 확인합니다.\n"
            "• `/추천인 [유저]` — 나를 초대해준 사람을 추천인으로 등록합니다. (음성 접속 30분 이상 시 가능)\n"
            "• `/채팅청소 [유저] [수량]` — 최근 채팅을 수량만큼 삭제합니다. **(일반 유저는 본인 채팅만 삭제 가능)**\n"
            "• `/코인순위` — 이 서버의 코인 보유량 상위 10명을 확인합니다.\n"
            "• `/명령어` — 봇의 전체 명령어 안내를 확인합니다."
        ),
        inline=False,
    )
    
    # 🛡️ 관리자 전용 명령어
    embed.add_field(
        name="🛡️ 관리자 전용 명령어",
        value=(
            "• `/채팅청소 [타유저] [수량]` — **관리자 권한**으로 다른 유저의 채팅을 지정한 수량만큼 강제로 청소할 수 있습니다.\n"
            "• `/코인지급 [유저] [수량]` — 특정 유저의 코인을 지급하거나 차감합니다. (차감은 마이너스 입력)\n"
            "• `/경고지급 [유저] [횟수]` — 경고를 부여하거나 차감합니다. (음수 입력 시 경고 우선 차감, 소진 후 방어권 충전, 3회 누적 시 자동 밴)\n"
            "• `/보상설정 [수량]` — 음성 채널 누적 30분 이용 시 지급될 코인 양을 설정합니다.\n"
            "• `/추천보상설정 [수량]` — 추천인 등록 성공 시 추천인에게 지급할 코인 수를 설정합니다.\n"
            "• `/코인초기화` — 서버 내 모든 유저의 코인을 공개 경고창을 통해 0으로 초기화합니다.\n"
            "• `/로그` — 관리자 명령어 실행 기록이 남을 채널을 현재 채널로 설정합니다."
        ),
        inline=False,
    )

    await interaction.response.send_message(embed=embed, ephemeral=True)


if __name__ == "__main__":
    keep_alive() # Flask 서버 가동 (Render 포트 바인딩 해결)
    token = os.getenv("DISCORD_BOT_TOKEN") or os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("DISCORD_TOKEN or DISCORD_BOT_TOKEN must be configured.")
    bot.run(token)

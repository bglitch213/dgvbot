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
from psycopg2.extras import execute_batch  # 대량 쿼리 최적화를 위한 모듈

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

    cursor.execute("""
        ALTER TABLE guild_settings
        ADD COLUMN IF NOT EXISTS slot_rtp INTEGER DEFAULT 85
    """)

    conn.commit()
    cursor.close()
    conn.close()


init_db()

# 일반 유저 음성 및 멤버 인식을 위한 인텐트 설정 강화
intents = discord.Intents.default()
intents.guilds = True
intents.voice_states = True
intents.members = True          # 일반 유저 인식에 필수 (개발자 포털 설정도 필요)
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

    # 봇 구동 시 서버 내 모든 멤버 정보를 미리 불러와 일반 유저 누락 방지
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
# 🚀 최적화된 음성 시간 체크 루프 (2,000명 규모 대응 Bulk Update)
# ==========================================
@tasks.loop(minutes=1)
async def check_voice_time():
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT guild_id, user_id, join_time FROM voice_sessions")
        sessions = cursor.fetchall()
        if not sessions:
            return

        # 서버별 보상 설정 미리 로드
        cursor.execute("SELECT guild_id, voice_reward_rate FROM guild_settings")
        reward_rates = {row[0]: row[1] for row in cursor.fetchall()}

        expired_sessions = []
        valid_sessions = []

        for guild_id, user_id, join_time in sessions:
            guild = bot.get_guild(guild_id)
            if not guild:
                expired_sessions.append((guild_id, user_id))
                continue
            
            member = guild.get_member(user_id)
            # 멤버 캐시에 없으면 fetch_member로 한 번 더 안전하게 조회 시도
            if not member:
                try:
                    member = await guild.fetch_member(user_id)
                except Exception:
                    pass

            if (
                member 
                and member.voice 
                and member.voice.channel 
                and not member.bot
                and not member.voice.self_mute
                and not member.voice.self_deaf
            ):
                valid_sessions.append((guild_id, user_id))
            else:
                expired_sessions.append((guild_id, user_id))

        # 1. 퇴장했거나 유효하지 않은 세션 일괄 삭제
        if expired_sessions:
            execute_batch(cursor, "DELETE FROM voice_sessions WHERE guild_id = %s AND user_id = %s", expired_sessions)

        if valid_sessions:
            # 2. 유효한 유저들의 현재 정보 일괄 조회
            cursor.execute("""
                SELECT guild_id, user_id, coins, voice_minutes 
                FROM users 
                WHERE (guild_id, user_id) IN (%s)
            """ % ",".join(["(%s, %s)" % (g, u) for g, u in valid_sessions]))
            
            user_data_map = {(row[0], row[1]): {"coins": row[2], "voice_minutes": row[3]} for row in cursor.fetchall()}

            missing_users = []
            update_rows = []

            for guild_id, user_id in valid_sessions:
                reward_rate = reward_rates.get(guild_id, 1)
                
                if (guild_id, user_id) not in user_data_map:
                    missing_users.append((guild_id, user_id, 0, 1, 0, 0))
                    new_minutes = 1
                    added_coins = reward_rate if (new_minutes > 0 and new_minutes % VOICE_REWARD_INTERVAL_MINUTES == 0) else 0
                else:
                    current_data = user_data_map[(guild_id, user_id)]
                    new_minutes = current_data["voice_minutes"] + 1
                    added_coins = reward_rate if (new_minutes > 0 and new_minutes % VOICE_REWARD_INTERVAL_MINUTES ==

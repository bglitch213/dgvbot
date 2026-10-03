import os
import discord
from discord.ext import commands
import psycopg2
from flask import Flask
from threading import Thread

# 1. 환경 변수에서 설정값 불러오기
TOKEN = os.getenv("DISCORD_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

# Flask 웹서버 (Render 헬스체크용)
app = Flask('')

@app.route('/')
def home():
    return "Bot is running!"

def run():
    app.run(host='0.0.0.0', port=10000)

def keep_alive():
    t = Thread(target=run)
    t.start()

# 2. 디스코드 봇 설정 (인텐트 및 봇 객체 생성) - 반드시 명령어보다 위에 있어야 합니다!
intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)

# 데이터베이스 연결 함수
def get_db_connection():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL 환경 변수가 설정되지 않았습니다! Render 설정에서 확인해주세요.")
    return psycopg2.connect(DATABASE_URL)

# 데이터베이스 테이블 초기화 함수
def init_db():
    try:
        conn = get_db_connection()
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
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id BIGINT PRIMARY KEY,
                voice_reward_rate INTEGER DEFAULT 1,
                referral_reward INTEGER NOT NULL DEFAULT 30,
                log_channel_id BIGINT DEFAULT NULL
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()
        print("데이터베이스 테이블 확인 및 초기화 완료.")
    except Exception as e:
        print(f"데이터베이스 초기화 에러: {e}")

@bot.event
async def on_ready():
    print(f"로그인 완료: {bot.user} (ID: {bot.user.id})")
    init_db()
    
    # 슬래시 명령어(Slash Commands) 동기화
    try:
        synced = await bot.tree.sync()
        print(f"슬래시 명령어 {len(synced)}개 동기화 완료.")
    except Exception as e:
        print(f"명령어 동기화 에러: {e}")

# 3. 슬래시 명령어: /정보 (자신 또는 다른 유저의 코인 확인)
@bot.tree.command(name="정보", description="자신 또는 다른 유저의 코인 보유량을 확인합니다.")
async def info_command(interaction: discord.Interaction, member: discord.Member = None):
    # 상호작용 지연 응답
    await interaction.response.defer(thinking=True)

    guild_id = interaction.guild_id
    
    # member가 지정되지 않았으면 명령어를 입력한 본인으로 설정
    target_user = member if member else interaction.user
    user_id = target_user.id

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 유저가 데이터베이스에 없으면 새로 생성 (기본 코인 0)
        cursor.execute(
            """
            INSERT INTO users (guild_id, user_id, coins) 
            VALUES (%s, %s, 0) 
            ON CONFLICT (guild_id, user_id) DO NOTHING
            """,
            (guild_id, user_id)
        )
        conn.commit()

        # 코인 조회
        cursor.execute(
            """
            SELECT coins FROM users 
            WHERE guild_id = %s AND user_id = %s
            """,
            (guild_id, user_id)
        )
        result = cursor.fetchone()
        coins = result[0] if result else 0

        cursor.close()
        conn.close()

        # 결과 출력
        await interaction.followup.send(f"💰 {target_user.mention}님의 현재 잔액: **{coins} 코인**")

    except Exception as e:
        print(f"/정보 명령어 에러: {e}")
        await interaction.followup.send("❌ 데이터베이스 처리 중 오류가 발생했습니다.")

# 봇 실행
if __name__ == "__main__":
    keep_alive()
    bot.run(TOKEN)

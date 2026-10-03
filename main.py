import os
import discord
from discord.ext import commands, tasks
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

# 디스코드 봇 설정
intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)

# 데이터베이스 연결 함수 (with 문을 사용하여 자동 commit 및 닫기 보장)
def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

# 데이터베이스 테이블 초기화 함수
def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
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
    except Exception as e:
        print(f"데이터베이스 초기화 에러: {e}")
    finally:
        cursor.close()
        conn.close()

@bot.event
async def on_ready():
    print(f"로그인 완료: {bot.user} (ID: {bot.user.id})")
    init_db()
    print("데이터베이스 테이블 확인 및 초기화 완료.")

# 예시 명령어: 코인 확인 및 적립 (테스트용)
@bot.command(name="코인")
async def check_coins(ctx, amount: int = None):
    guild_id = ctx.guild.id
    user_id = ctx.author.id

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # 유저가 없으면 생성하고, 있으면 조회
        cursor.execute(
            """
            INSERT INTO users (guild_id, user_id, coins) 
            VALUES (%s, %s, 0) 
            ON CONFLICT (guild_id, user_id) DO NOTHING
            """,
            (guild_id, user_id)
        )
        conn.commit()

        if amount is not None:
            # 코인 추가 
            cursor.execute(
                """
                UPDATE users SET coins = coins + %s 
                WHERE guild_id = %s AND user_id = %s
                """,
                (amount, guild_id, user_id)
            )
            conn.commit()
            await ctx.send(f"✅ {ctx.author.mention님에게 {amount} 코인이 추가되었습니다!")
        else:
            # 현재 코인 조회
            cursor.execute(
                """
                SELECT coins FROM users 
                WHERE guild_id = %s AND user_id = %s
                """,
                (guild_id, user_id)
            )
            result = cursor.fetchone()
            coins = result[0] if result else 0
            await ctx.send(f"💰 현재 잔액: **{coins} 코인**")

    except Exception as e:
        print(f"코인 명령어 에러: {e}")
        await ctx.send("❌ 데이터 처리 중 오류가 발생했습니다.")
    finally:
        cursor.close()
        conn.close()

# 봇 실행
if __name__ == "__main__":
    keep_alive()
    bot.run(TOKEN)import os
import discord
from discord.ext import commands, tasks
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

# 디스코드 봇 설정
intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)

# 데이터베이스 연결 함수 (with 문을 사용하여 자동 commit 및 닫기 보장)
def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

# 데이터베이스 테이블 초기화 함수
def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
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
    except Exception as e:
        print(f"데이터베이스 초기화 에러: {e}")
    finally:
        cursor.close()
        conn.close()

@bot.event
async def on_ready():
    print(f"로그인 완료: {bot.user} (ID: {bot.user.id})")
    init_db()
    print("데이터베이스 테이블 확인 및 초기화 완료.")

# 예시 명령어: 코인 확인 및 적립 (테스트용)
@bot.command(name="코인")
async def check_coins(ctx, amount: int = None):
    guild_id = ctx.guild.id
    user_id = ctx.author.id

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # 유저가 없으면 생성하고, 있으면 조회
        cursor.execute(
            """
            INSERT INTO users (guild_id, user_id, coins) 
            VALUES (%s, %s, 0) 
            ON CONFLICT (guild_id, user_id) DO NOTHING
            """,
            (guild_id, user_id)
        )
        conn.commit()

        if amount is not None:
            # 코인 추가 
            cursor.execute(
                """
                UPDATE users SET coins = coins + %s 
                WHERE guild_id = %s AND user_id = %s
                """,
                (amount, guild_id, user_id)
            )
            conn.commit()
            await ctx.send(f"✅ {ctx.author.mention님에게 {amount} 코인이 추가되었습니다!")
        else:
            # 현재 코인 조회
            cursor.execute(
                """
                SELECT coins FROM users 
                WHERE guild_id = %s AND user_id = %s
                """,
                (guild_id, user_id)
            )
            result = cursor.fetchone()
            coins = result[0] if result else 0
            await ctx.send(f"💰 현재 잔액: **{coins} 코인**")

    except Exception as e:
        print(f"코인 명령어 에러: {e}")
        await ctx.send("❌ 데이터 처리 중 오류가 발생했습니다.")
    finally:
        cursor.close()
        conn.close()

# 봇 실행
if __name__ == "__main__":
    keep_alive()
    bot.run(TOKEN)

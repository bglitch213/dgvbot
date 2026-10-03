# 슬래시 명령어: /정보 (자신 또는 다른 유저의 코인 확인)
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

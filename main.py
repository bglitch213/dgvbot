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

    # 🛡️ [수정된 핵심 로직] 경고를 부여하는 경우 (count > 0) 방어권 우선 소모 체크
    if count > 0:
        applied_warnings_count = 0
        used_defense_count = 0
        
        # 부여하려는 횟수만큼 반복하며 방어권이 있으면 방어권으로 방어
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
            action_desc = f"방어권 **{used_defense_count개}**가 소모되어 경고가 방어되었습니다!"
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
            SET warnings = ?, defense_tickets = ?
            WHERE guild_id = ? AND user_id = ?
            """,
            (new_warnings, new_defense, guild_id, target_id)
        )

    else: # 경고를 차감하는 경우 (count < 0)
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

    detailed_log_text = f"{log_text} (현재 누적 경고: **{total_warnings}회**, 방어권: **{total_defense}개**)"
    await log_admin_action(interaction.guild, detailed_log_text)

    # 실제로 경고가 3회 이상이 된 경우에만 밴 처리
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
                f"⚠️ 경고가 부여되었으나, 봇의 권한 부족으로 차단에 실패했습니다. (권한을 확인해주세요)\n오류: {e}\n"
                f"⚠️ 대상자 현재 상태 — 경고: **{total_warnings}회**, 방어권: **{total_defense}개**"
            )
    else:
        await interaction.followup.send(
            f"⚠️ {member.mention}님에게 {action_desc}\n"
            f"⚠️ 대상자 현재 상태 — 경고: **{total_warnings}회**, 방어권: **{total_defense}개**",
            allowed_mentions=discord.AllowedMentions.none()
        )

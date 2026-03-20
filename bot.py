import os
import discord
import asyncpg
import random
import asyncio
from discord import app_commands
from discord.ext import tasks
from datetime import datetime, timedelta

TOKEN = os.environ["DISCORD_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]

print("DB_URL_RAW:", DATABASE_URL)

OWNER_ID = 985188925360443452
GUILD_ID = 1456265813190512763
CHANNEL_ID = 1473183167895830568

pool = None

intents = discord.Intents.default()
intents.message_content = True

bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)


# ---------------- ЛИНИИ ----------------

BASE_LINE_LIMITS = {
    64: 3,
    26: 2,
    31: 2,
    68: 2,
    98: 3,
    3: 2,
    150: 2,
    72: 2,
    1: 1,
    5: 1,
    67: 1,
    28: 2
}

def get_line_limits_for_date(date):
    limits = BASE_LINE_LIMITS.copy()
    if date.weekday() >= 5:
        limits[68] = 1
        limits[28] = 1
    return limits


# ---------------- DATABASE ----------------

async def init_db():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL)

    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS buses (
                bus BIGINT PRIMARY KEY,
                driver1 BIGINT NOT NULL,
                driver2 BIGINT
            );
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS reserves (
                bus BIGINT PRIMARY KEY
            );
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS broken (
                bus BIGINT PRIMARY KEY
            );
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS sick (
                driver BIGINT PRIMARY KEY
            );
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS assigned_reserves (
                broken_bus BIGINT PRIMARY KEY,
                reserve_bus BIGINT NOT NULL
            );
        """)


# ---------------- РОТАЦИЯ ----------------

def get_week_shift(driver1, driver2):
    tomorrow = datetime.now() + timedelta(days=1)
    week_number = tomorrow.isocalendar().week

    if driver2 is None:
        return driver1, None

    return (driver2, driver1) if week_number % 2 == 0 else (driver1, driver2)


# ---------------- READY ----------------

@bot.event
async def on_ready():
    await init_db()
    await tree.sync(guild=discord.Object(id=GUILD_ID))
    auto_naryad.start()
    print(f"Bot ready as {bot.user}")


# ---------------- ADD TITULAR ----------------

@tree.command(name="addtitular", description="Добави титуляр", guild=discord.Object(id=GUILD_ID))
async def addtitular(interaction: discord.Interaction, driver1: int, bus: int, driver2: int | None = None):

    if interaction.user.id != OWNER_ID:
        await interaction.response.send_message("Нямаш право.", ephemeral=True)
        return

    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO buses(bus, driver1, driver2)
            VALUES($1,$2,$3)
            ON CONFLICT (bus)
            DO UPDATE SET driver1=$2, driver2=$3
        """, bus, driver1, driver2)

    await interaction.response.send_message("Записано.")


# ---------------- REMOVE TITULAR ----------------

@tree.command(name="removetitular", description="Премахни титуляр (автобус)", guild=discord.Object(id=GUILD_ID))
async def removetitular(interaction: discord.Interaction, bus: int):

    if interaction.user.id != OWNER_ID:
        await interaction.response.send_message("Нямаш право.", ephemeral=True)
        return

    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM buses WHERE bus=$1", bus)

    if result.endswith("0"):
        await interaction.response.send_message(f"{bus} не е намерен.")
        return

    await interaction.response.send_message(f"{bus} е премахнат.")


# ---------------- DRIVERS ----------------

@tree.command(name="drivers", description="Покажи всички титуляри", guild=discord.Object(id=GUILD_ID))
async def drivers(interaction: discord.Interaction):

    if interaction.user.id != OWNER_ID:
        await interaction.response.send_message("Нямаш право.", ephemeral=True)
        return

    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM buses ORDER BY bus ASC")

    if not rows:
        await interaction.response.send_message("Няма записани автобуси.")
        return

    text = "БУС   | ПЪРВА   | ВТОРА\n"
    text += "-" * 35 + "\n"

    for row in rows:
        text += f"{row['bus']:<6}| {row['driver1']:<8}| {row['driver2'] if row['driver2'] else '-'}\n"

    await interaction.response.send_message(f"```{text}```")


# ---------------- SICK ----------------

@tree.command(name="sick", description="Водач в болничен", guild=discord.Object(id=GUILD_ID))
async def sick_cmd(interaction: discord.Interaction, driver: int):

    if interaction.user.id != OWNER_ID:
        await interaction.response.send_message("Нямаш право.", ephemeral=True)
        return

    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO sick(driver) VALUES($1) ON CONFLICT DO NOTHING",
            driver
        )

    await interaction.response.send_message(f"{driver} е в болничен.")


# ---------------- UNSICK ----------------

@tree.command(name="unsick", description="Махни от болничен", guild=discord.Object(id=GUILD_ID))
async def unsick_cmd(interaction: discord.Interaction, driver: int):

    if interaction.user.id != OWNER_ID:
        await interaction.response.send_message("Нямаш право.", ephemeral=True)
        return

    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM sick WHERE driver=$1", driver)

    if result.endswith("0"):
        await interaction.response.send_message("Не е в болничен.")
        return

    await interaction.response.send_message("Махнат от болничен.")


# ---------------- RESERVE ----------------

@tree.command(name="reserve", description="Резерв", guild=discord.Object(id=GUILD_ID))
async def reserve(interaction: discord.Interaction, bus: int):

    if interaction.user.id != OWNER_ID:
        await interaction.response.send_message("Нямаш право.", ephemeral=True)
        return

    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO reserves(bus) VALUES($1) ON CONFLICT DO NOTHING",
            bus
        )

    await interaction.response.send_message("В резерв.")


# ---------------- ASSIGN RESERVE ----------------

@tree.command(name="assignreserve", description="Закачи резерва към счупен автобус", guild=discord.Object(id=GUILD_ID))
async def assignreserve(interaction: discord.Interaction, reserve_bus: int, broken_bus: int):

    if interaction.user.id != OWNER_ID:
        await interaction.response.send_message("Нямаш право.", ephemeral=True)
        return

    async with pool.acquire() as conn:
        reserve_exists = await conn.fetchval("SELECT 1 FROM reserves WHERE bus=$1", reserve_bus)
        broken_exists = await conn.fetchval("SELECT 1 FROM broken WHERE bus=$1", broken_bus)

        if not reserve_exists:
            await interaction.response.send_message(f"Резервата {reserve_bus} не е записана в резерв.")
            return

        if not broken_exists:
            await interaction.response.send_message(f"Автобус {broken_bus} не е записан в ремонт.")
            return

        await conn.execute("""
            INSERT INTO assigned_reserves(broken_bus, reserve_bus)
            VALUES($1, $2)
            ON CONFLICT (broken_bus)
            DO UPDATE SET reserve_bus=$2
        """, broken_bus, reserve_bus)

    await interaction.response.send_message(f"Резерва {reserve_bus} е закачена към счупения автобус {broken_bus}.")


# ---------------- REMOVE RESERVE ----------------

@tree.command(name="removereserve", description="Махни резерв", guild=discord.Object(id=GUILD_ID))
async def removereserve(interaction: discord.Interaction, bus: int):

    if interaction.user.id != OWNER_ID:
        await interaction.response.send_message("Нямаш право.", ephemeral=True)
        return

    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM reserves WHERE bus=$1", bus)
        await conn.execute("DELETE FROM assigned_reserves WHERE reserve_bus=$1", bus)

    await interaction.response.send_message("Махнат резерв.")


# ---------------- BROKEN ----------------

@tree.command(name="broken", description="В ремонт", guild=discord.Object(id=GUILD_ID))
async def broken_cmd(interaction: discord.Interaction, bus: int):

    if interaction.user.id != OWNER_ID:
        await interaction.response.send_message("Нямаш право.", ephemeral=True)
        return

    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO broken(bus) VALUES($1) ON CONFLICT DO NOTHING",
            bus
        )

    await interaction.response.send_message("В ремонт.")


# ---------------- FIX ----------------

@tree.command(name="fix", description="Поправен", guild=discord.Object(id=GUILD_ID))
async def fix(interaction: discord.Interaction, bus: int):

    if interaction.user.id != OWNER_ID:
        await interaction.response.send_message("Нямаш право.", ephemeral=True)
        return

    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM broken WHERE bus=$1", bus)
        await conn.execute("DELETE FROM assigned_reserves WHERE broken_bus=$1", bus)

    await interaction.response.send_message("Поправен.")


# ---------------- NARYAD ----------------

@tree.command(name="naryad", description="Наряд", guild=discord.Object(id=GUILD_ID))
async def naryad(interaction: discord.Interaction):
    text = await generate_naryad_text()
    await interaction.response.send_message(
        "@everyone\n" + text,
        allowed_mentions=discord.AllowedMentions(everyone=True)
    )


# ---------------- AUTO ----------------

@tasks.loop(minutes=1)
async def auto_naryad():
    now = datetime.now()
    if now.hour == 15 and now.minute == 0:
        channel = bot.get_channel(CHANNEL_ID)
        if channel:
            text = await generate_naryad_text()
            await channel.send(
                "@everyone\n" + text,
                allowed_mentions=discord.AllowedMentions(everyone=True)
            )
        await asyncio.sleep(60)


# ---------------- ГЕНЕРАТОР ----------------

async def generate_naryad_text():

    tomorrow = datetime.now() + timedelta(days=1)
    date_str = tomorrow.strftime("%d.%m.%Y")
    line_limits = get_line_limits_for_date(tomorrow)

    async with pool.acquire() as conn:
        buses = await conn.fetch("SELECT * FROM buses")
        reserves = await conn.fetch("SELECT bus FROM reserves")
        broken = await conn.fetch("SELECT bus FROM broken")
        sick = await conn.fetch("SELECT driver FROM sick")
        assigned_reserves = await conn.fetch("SELECT broken_bus, reserve_bus FROM assigned_reserves")

    if not buses:
        return "Няма записани автобуси."

    broken_set = {r["bus"] for r in broken}
    sick_set = {r["driver"] for r in sick}
    assigned_map = {r["broken_bus"]: r["reserve_bus"] for r in assigned_reserves}

    reserve_list = [r["bus"] for r in reserves]
    reserve_pool = reserve_list.copy()

    for used_reserve in assigned_map.values():
        if used_reserve in reserve_pool:
            reserve_pool.remove(used_reserve)

    random.shuffle(buses)

    by_line = {}
    bus_index = 0

    available_lines = list(line_limits.keys())
    random.shuffle(available_lines)

    for line in available_lines:

        if bus_index >= len(buses):
            break

        limit = line_limits[line]
        assigned = 0

        while assigned < limit and bus_index < len(buses):

            row = buses[bus_index]
            bus_index += 1

            original_bus = row["bus"]
            bus = original_bus
            d1 = row["driver1"]
            d2 = row["driver2"]

            first, second = get_week_shift(d1, d2)

            if original_bus in broken_set and original_bus in assigned_map:
                bus = assigned_map[original_bus]
            elif original_bus in broken_set and reserve_pool:
                bus = reserve_pool.pop(0)

            f1 = f"{first} (БОЛНИЧЕН)" if first in sick_set else str(first)
            f2 = "-"
            if second:
                f2 = f"{second} (БОЛНИЧЕН)" if second in sick_set else str(second)

            by_line.setdefault(line, []).append((assigned + 1, bus, f1, f2))
            assigned += 1

    text = f"📋 НАРЯД ЗА {date_str}\n\n```"
    text += f"{'Линия':<6} | {'Кола':<4} | {'ПС':<6} | {'Водач1':<12} | {'Водач2':<12}\n"
    text += "-" * 75 + "\n"

    for line in sorted(by_line.keys()):
        for car, bus, f1, f2 in by_line[line]:
            text += f"{line:<6} | {car:<4} | {bus:<6} | {f1:<12} | {f2:<12}\n"
        text += "-" * 75 + "\n"

    text += "```"

    text += "\nРЕЗЕРВИ:\n"
    text += ", ".join(map(str, sorted(reserve_list))) if reserve_list else "няма"

    text += "\n\nБОЛНИЧНИ:\n"
    text += ", ".join(map(str, sorted(sick_set))) if sick_set else "няма"

    text += "\n\nВ РЕМОНТ:\n"
    text += ", ".join(map(str, sorted(broken_set))) if broken_set else "няма"

    return text


# ---------------- START ----------------

bot.run(TOKEN)
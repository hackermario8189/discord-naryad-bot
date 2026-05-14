import os
import io
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
REST_ROTATION_START = datetime(2026, 1, 1).date()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
ASSETS_DIR = os.path.join(TEMPLATES_DIR, "assets")
PTEN_CHANNEL_ID = 1484651462721015838  # СМЕНИ С ТВОЯ КАНАЛ

pool = None

intents = discord.Intents.default()
intents.message_content = True

bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)


# ---------------- ЛИНИИ ----------------

BASE_LINE_LIMITS = {
    "X9": 2,
    42: 2,
    63: 2,
    64: 3,
    26: 2,
    31: 2,
    68: 2,
    98: 3,
    183: 2,
    150: 2,
    72: 2, # 6 коли 
    181: 1,
    185: 1,
    67: 1,
    107: 2,
    28: 2,
    111: 4,
    108: 2,
    83: 2,
    260: 2,
    78: 2,   #+ koli
    310: 2,   #+ koli
    280: 2     #+koli

}
LINES_INFO = {
    "X9": ("Ж.К. Лозенец", "Метростанция Акад.А.Т.-Балан"),
    181: ("Гео Милев", "Кокалянско ханче"),
    183: ("Гео Милев", "Долни Пасарел"),
    185: ("Гео Милев", "Долни Лозен"),
    26: ("ж.к. Обеля 1", "Гниляне"),
    28: ("Мрамор", "Локорско"),
    31: ("Сливница", "Голяновци"),
    42: ("Ж.К. Люлин 8", "Края На Кв. Михайлово"),
    63: ("Булевард Цар Борис 3", "Златните Мостове"),
    64: ("Зоопарка", "Център по хигиена"),
    67: ("Симеоново", "Семинарията"),
    68: ("Зоопарка", "Симеоново"),
    72: ("Хотел Плиска", "ж.к. Западен парк"),
    98: ("Зоопарка", "Железница"),
    107: ("Кв. Карпузица", "Боянска Църква"),
    108: ("Хюндай България", "Ж.К. Люлин 5"),
    111: ("Ж.К..Младост1", "Ж.К. Люлин 1,2"),
    150: ("Метростанция Сливница", "Плодохранилище"),
    260: ("Ж.К. Красно Село", "Кв. Горна Баня"),
    280: ("Студентски Град", "СУ Свети Климент Охридски"),
    310: ("Ж.К. ЛЮЛИН 5", "Пл. Сточна Гара"),
    78: ("Централна Гара", "Площада кв. Враждебна"),
    83: ("Зоопарка", "Стадион Локомотив"),
}  

LINE_GROUPS = {
    1: ["X9", 42, 63, 64, 83, 108, 111, 150, 260],
    2: [181, 183, 185, 26, 28, 68, 72, 98, 107, 280, 310, 78],
    7: [31]
}
  #фИЛТЪР ЗА БУСОВЕТЕ
LINE_BUS_PREFIX_PREFERENCES = { 
    26: [27, 20],
    83: [16, 11],
    280: [23]
}

LINE_BUS_PREFIX_ONLY = {
    280: [23],
    83: [11, 16],
    78: [23],
    310: [23]
}

BUS_PREFIX_LINE_ONLY = {
    23: [280, 78, 310],
    11: [83],
    16: [83]
}

def get_allowed_lines_for_bus(bus):
    if 1000 <= bus <= 1999:
        return ["X9", 63, 64, 108, 111, 150, 260]  # линии за автобуси 1000–1999
    elif 2000 <= bus <= 2999:
        return [1, 3, 5, 26, 28, 68, 72, 98, 107, 150]  # линии за автобуси 2000–2999
    else:
        return []  # ако автобусът не е в тези диапазони, няма линии

def get_bus_prefix(bus):
    return bus // 100

def is_bus_allowed_on_line(bus, line):
    prefix = get_bus_prefix(bus)
    prefix_only_lines = BUS_PREFIX_LINE_ONLY.get(prefix)

    if prefix_only_lines is not None:
        return line in prefix_only_lines

    if line not in get_allowed_lines_for_bus(bus):
        return False

    line_only_prefixes = LINE_BUS_PREFIX_ONLY.get(line)
    if line_only_prefixes is not None:
        return prefix in line_only_prefixes

    return True

def sort_buses_for_line(line, candidate_buses):
    preferred_prefixes = LINE_BUS_PREFIX_PREFERENCES.get(line)

    if not preferred_prefixes:
        return candidate_buses

    prefix_order = {prefix: index for index, prefix in enumerate(preferred_prefixes)}

    return sorted(
        candidate_buses,
        key=lambda row: prefix_order.get(get_bus_prefix(row["bus"]), len(preferred_prefixes))
    )

def get_line_limits_for_date(date):
    limits = BASE_LINE_LIMITS.copy()
    if date.weekday() >= 5:
        limits[68] = 0
        limits[28] = 1
    return limits

def get_rest_limit_for_date(date):
    return 4 if date.weekday() >= 5 else 3

def get_all_drivers(buses):
    drivers = set()

    for row in buses:
        drivers.add(row["driver1"])

        if row["driver2"] is not None:
            drivers.add(row["driver2"])

    return sorted(drivers)

def get_rest_drivers_for_date(date, buses, sick_set):
    all_drivers = [driver for driver in get_all_drivers(buses) if driver not in sick_set]

    if not all_drivers:
        return set()

    current_date = REST_ROTATION_START
    current_index = 0

    while current_date < date:
        remaining = len(all_drivers) - current_index
        daily_limit = min(get_rest_limit_for_date(current_date), remaining)
        current_index += daily_limit

        if current_index >= len(all_drivers):
            current_index = 0

        current_date += timedelta(days=1)

    remaining = len(all_drivers) - current_index
    daily_limit = min(get_rest_limit_for_date(date), remaining)

    return set(all_drivers[current_index:current_index + daily_limit])

def format_driver_status(driver, sick_set, rest_set):
    if driver in sick_set:
        return f"{driver} (БОЛНИЧЕН)"

    if driver in rest_set:
        return f"{driver} (ПОЧИВКА)"

    return str(driver)


# ---------------- DATABASE ---------------- НЕ СЕ ПИПА

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

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS ext_info (
                bus BIGINT NOT NULL,
                driver BIGINT NOT NULL,
                naryad_date DATE NOT NULL DEFAULT CURRENT_DATE + INTERVAL '1 day',
                note TEXT NOT NULL,
                PRIMARY KEY (bus, driver, naryad_date)
            );
        """)

        await conn.execute("""
            ALTER TABLE ext_info
            ADD COLUMN IF NOT EXISTS naryad_date DATE NOT NULL DEFAULT CURRENT_DATE + INTERVAL '1 day';
        """)

        await conn.execute("""
            ALTER TABLE ext_info
            DROP CONSTRAINT IF EXISTS ext_info_pkey;
        """)

        await conn.execute("""
            ALTER TABLE ext_info
            ADD PRIMARY KEY (bus, driver, naryad_date);
        """)

        await conn.execute("DELETE FROM ext_info WHERE naryad_date < CURRENT_DATE;")


# ---------------- РОТАЦИЯ ---------------- НЯМА КАК ДА РАЗВАЛИ СКРИПТА!!! ДА НЕ СЕ ПИПА

def get_week_shift(driver1, driver2):
    tomorrow = datetime.now() + timedelta(days=1)
    week_number = tomorrow.isocalendar().week

    if driver2 is None:
        return driver1, None

    return (driver2, driver1) if week_number % 2 == 0 else (driver1, driver2)


# ---------------- ПЪТЕН ЛИСТ ----------------

def generate_trip_sheet(line, car, bus, driver1, driver2):

    start, end = LINES_INFO.get(line, ("???", "???"))

    shifts_1 = [
        ("04:30", "12:30"),
        ("04:45", "12:45"),
        ("05:00", "13:00"),
        ("05:15", "13:15")
    ]

    shifts_2 = [
        ("12:45", "21:30"),
        ("13:00", "21:45"),
        ("13:15", "22:00"),
        ("13:30", "22:15")
    ]

    idx = (car - 1) % 4

    def build_shift(name, start_time, end_time):
        t = datetime.strptime(start_time, "%H:%M")
        end_dt = datetime.strptime(end_time, "%H:%M")

        trips = []
        while t < end_dt:
            dep = t
            arr = t + timedelta(minutes=30)
            trips.append((dep.strftime("%H:%M"), arr.strftime("%H:%M")))
            t += timedelta(minutes=60)

        text = f"\n{name}\n"
        text += f"{start:<20} | ЧАС | {end}\n"
        text += "-" * 45 + "\n"

        for d, a in trips:
            text += f"{start:<20} | {d} | {end} {a}\n"

        return text

    text = f"📋 ПЪТЕН ЛИСТ\n"
    text += f"Линия: {line}\n"
    text += f"Кола: {car}\n"
    text += f"ПС: {bus}\n"
    text += f"Водач1: {driver1}\n"
    text += f"Водач2: {driver2}\n"

    text += build_shift("1-ва смяна", *shifts_1[idx])
    text += build_shift("2-ра смяна", *shifts_2[idx])

    return text

def load_naryad_font(size, bold=False):
    from PIL import ImageFont

    bundled_font = os.path.join(ASSETS_DIR, "arialbd.ttf")
    if os.path.exists(bundled_font):
        return ImageFont.truetype(bundled_font, size)

    font_names = [
        "arialbd.ttf",
        "DejaVuSans-Bold.ttf",
        "NotoSans-Bold.ttf",
        "LiberationSans-Bold.ttf",
    ]

    if not bold:
        font_names.extend([
            "arial.ttf",
            "DejaVuSans.ttf",
            "NotoSans-Regular.ttf",
            "LiberationSans-Regular.ttf",
        ])

    font_dirs = (
        os.path.join(os.environ.get("WINDIR", "C:\\Windows"), "Fonts"),
        "/usr/share/fonts/truetype/dejavu",
        "/usr/share/fonts/truetype/noto",
        "/usr/share/fonts/truetype/liberation",
        "/usr/share/fonts/truetype/liberation2",
        "/usr/share/fonts/dejavu",
        "/usr/share/fonts",
    )

    for font_dir in font_dirs:
        for font_name in font_names:
            path = os.path.join(font_dir, font_name)
            if os.path.exists(path):
                return ImageFont.truetype(path, size)

    for font_dir in font_dirs:
        if not os.path.exists(font_dir):
            continue

        for root, _, files in os.walk(font_dir):
            for filename in files:
                lower = filename.lower()

                if lower.endswith((".ttf", ".otf")) and ("bold" in lower or bold):
                    return ImageFont.truetype(os.path.join(root, filename), size)

    return ImageFont.load_default()


def clean_display_text(value):
    text = str(value)

    if "Ð" not in text and "Ñ" not in text and "Ã" not in text:
        return text

    try:
        fixed = text.encode("latin1").decode("utf-8")
    except UnicodeError:
        return text

    return fixed


def draw_centered_text(draw, box, text, font, fill="#111111"):
    x1, y1, x2, y2 = box
    text = clean_display_text(text)
    text_w, text_h = text_size(draw, text, font)
    draw.text((x1 + (x2 - x1 - text_w) / 2, y1 + (y2 - y1 - text_h) / 2), text, fill=fill, font=font)


def draw_label_box(draw, box, label, value, font, line_width, fill="#ffffff"):
    x1, y1, x2, y2 = box
    text = f"{label}: {clean_display_text(value)}" if label else clean_display_text(value)
    draw.rounded_rectangle(box, radius=max(8, line_width * 3), fill=fill, outline="#2f3a4a", width=line_width)
    draw.text((x1 + line_width * 5, y1 + line_width * 4), text, fill="#111111", font=font)


def text_size(draw, text, font):
    bbox = draw.textbbox((0, 0), clean_display_text(text), font=font)
    return bbox[2] - bbox[0], bbox[3] - bbox[1]


def wrap_text(draw, text, font, max_width):
    words = clean_display_text(text).split()
    if not words:
        return [""]

    lines = []
    line = words[0]

    for word in words[1:]:
        candidate = f"{line} {word}"
        if text_size(draw, candidate, font)[0] <= max_width:
            line = candidate
        else:
            lines.append(line)
            line = word

    lines.append(line)
    return lines


def paste_logo(canvas, logo_name, box):
    from PIL import Image

    path = os.path.join(ASSETS_DIR, logo_name)

    if not os.path.exists(path):
        return

    logo = Image.open(path).convert("RGBA")
    max_w = box[2] - box[0]
    max_h = box[3] - box[1]
    logo.thumbnail((max_w, max_h), Image.LANCZOS)

    x = box[0] + (max_w - logo.width) // 2
    y = box[1] + (max_h - logo.height) // 2
    canvas.alpha_composite(logo, (x, y))


def render_naryad_png(text, by_line):
    from PIL import Image, ImageDraw

    font_scale = 1
    layout_scale = 1
    logo_scale = 1

    def sc(value, scale=layout_scale):
        return int(round(value * scale))

    width = sc(1800)
    margin = sc(50)
    header_h = sc(170)
    title_h = sc(120)
    meta_h = sc(72)
    table_top = margin + header_h + title_h + meta_h + sc(24)

    font_title = load_naryad_font(sc(88, font_scale), bold=True)
    font_header = load_naryad_font(sc(44, font_scale), bold=True)
    font_cell = load_naryad_font(sc(42, font_scale), bold=True)
    font_small = load_naryad_font(sc(38, font_scale), bold=True)

    probe = Image.new("RGBA", (width, sc(200)), "white")
    draw = ImageDraw.Draw(probe)

    columns = [
        ("\u041b\u0438\u043d\u0438\u044f", sc(140)),
        ("\u041a\u043e\u043b\u0430", sc(120)),
        ("\u041f\u0421", sc(150)),
        ("\u0412\u043e\u0434\u0430\u0447 1", sc(255)),
        ("\u0412\u043e\u0434\u0430\u0447 2", sc(255)),
        ("\u0417\u0430\u0431\u0435\u043b\u0435\u0436\u043a\u0430", sc(780)),
    ]

    rows = []

    for line in sorted(by_line.keys(), key=lambda x: str(x)):
        for car, bus, f1, f2, note in by_line[line]:
            rows.append([line, car, bus, f1, f2, note])

    row_heights = []

    for row in rows:
        max_lines = 1

        for value, (_, col_width) in zip(row, columns):
            cell_lines = wrap_text(draw, value, font_cell, col_width - sc(22))
            max_lines = max(max_lines, len(cell_lines))

        row_heights.append(max(sc(88), sc(18) + max_lines * sc(54)))

    table_h = sc(94) + sum(row_heights)
    footer_text = text.split("```")[-1].strip() if "```" in text else ""
    footer_lines = []

    for footer_line in footer_text.splitlines():
        footer_lines.extend(wrap_text(draw, footer_line, font_small, width - margin * 2 - sc(30)))

    footer_h = sc(46) + max(1, len(footer_lines)) * sc(50)
    height = max(sc(980), table_top + table_h + footer_h + margin)

    canvas = Image.new("RGBA", (width, height), "#f6f8fb")
    draw = ImageDraw.Draw(canvas)

    line_w = sc(3)
    draw.rounded_rectangle((margin, margin, width - margin, height - margin), radius=sc(18), fill="#ffffff", outline="#1f2937", width=line_w)
    draw.rounded_rectangle((margin + line_w, margin + line_w, width - margin - line_w, margin + header_h), radius=sc(14), fill="#e8f1ff", outline=None)
    draw.rectangle((margin + line_w, margin + header_h - sc(16), width - margin - line_w, margin + header_h), fill="#e8f1ff")
    draw.rectangle((margin, margin + header_h - sc(8), width - margin, margin + header_h), fill="#2563eb")

    left_logo_w = sc(560, logo_scale)
    logo_h = sc(140, logo_scale)
    right_logo_w = sc(175, logo_scale)
    logo_y = margin + sc(18)

    paste_logo(canvas, "leftlogo.png", (margin + sc(28), logo_y, margin + sc(28) + left_logo_w, logo_y + logo_h))
    paste_logo(canvas, "rightlogo.png", (width - margin - sc(32) - right_logo_w, logo_y, width - margin - sc(32), logo_y + logo_h))

    title = "\u041d\u0410\u0420\u042f\u0414"
    title_w = text_size(draw, title, font_title)[0]
    title_y = margin + header_h + sc(18)
    draw.rounded_rectangle(
        ((width - title_w) / 2 - sc(36), title_y - sc(10), (width + title_w) / 2 + sc(36), title_y + sc(110)),
        radius=sc(18),
        fill="#eef6ff",
        outline="#bfd7ff",
        width=sc(2)
    )
    draw.text(((width - title_w) / 2, title_y), title, fill="#0f172a", font=font_title)

    tomorrow = datetime.now() + timedelta(days=1)
    meta_items = [
        f"\u0414\u0430\u0442\u0430: {tomorrow.strftime('%d.%m.%Y')}",
        "\u0421\u043c\u044f\u043d\u0430:",
        "\u0418\u0437\u0433\u043e\u0442\u0432\u0438\u043b:",
    ]
    meta_y = margin + header_h + title_h
    meta_w = (width - margin * 2 - sc(40)) // 3

    for idx, item in enumerate(meta_items):
        x = margin + sc(20) + idx * (meta_w + sc(20))
        draw_label_box(draw, (x, meta_y, x + meta_w, meta_y + sc(70)), "", item, font_small, sc(2), fill="#f8fbff")

    table_x = margin + sc(20)
    table_y = table_top
    x = table_x

    for label, col_width in columns:
        draw.rectangle((x, table_y, x + col_width, table_y + sc(94)), fill="#1d4ed8", outline="#1e3a8a", width=sc(2))
        draw_centered_text(draw, (x, table_y, x + col_width, table_y + sc(94)), label, font_header, fill="#ffffff")
        x += col_width

    y = table_y + sc(94)

    for row_index, (row, row_h) in enumerate(zip(rows, row_heights)):
        x = table_x
        row_fill = "#ffffff" if row_index % 2 == 0 else "#f4f8ff"

        for value, (_, col_width) in zip(row, columns):
            draw.rectangle((x, y, x + col_width, y + row_h), fill=row_fill, outline="#334155", width=sc(2))
            lines = wrap_text(draw, value, font_cell, col_width - sc(22))

            for line_index, cell_line in enumerate(lines):
                draw.text((x + sc(12), y + sc(18) + line_index * sc(54)), cell_line, fill="#111111", font=font_cell)

            x += col_width

        y += row_h

    if footer_lines:
        y += sc(24)
        draw.rounded_rectangle((table_x, y, width - margin - sc(20), y + footer_h - sc(18)), radius=sc(14), fill="#fff7ed", outline="#9a3412", width=sc(2))

        for index, line in enumerate(footer_lines):
            draw.text((table_x + sc(14), y + sc(14) + index * sc(50)), clean_display_text(line), fill="#111111", font=font_small)

    output = io.BytesIO()
    canvas.convert("RGB").save(output, format="PNG", optimize=True)
    output.seek(0)
    return output


def make_naryad_png_file(text, by_line):
    image = render_naryad_png(text, by_line)
    tomorrow = datetime.now() + timedelta(days=1)
    filename = f"naryad-{tomorrow.strftime('%Y-%m-%d')}.png"
    return discord.File(image, filename=filename)


def build_trip_sheet_rows(line, car):
    start, end = LINES_INFO.get(line, ("???", "???"))

    shifts_1 = [
        ("04:30", "12:30"),
        ("04:45", "12:45"),
        ("05:00", "13:00"),
        ("05:15", "13:15")
    ]

    shifts_2 = [
        ("12:45", "21:30"),
        ("13:00", "21:45"),
        ("13:15", "22:00"),
        ("13:30", "22:15")
    ]

    idx = (car - 1) % 4

    def build_rows(start_time, end_time):
        t = datetime.strptime(start_time, "%H:%M")
        end_dt = datetime.strptime(end_time, "%H:%M")
        rows = []

        while t < end_dt:
            dep = t
            arr = t + timedelta(minutes=30)
            rows.append((dep.strftime("%H:%M"), arr.strftime("%H:%M")))
            t += timedelta(minutes=60)

        return rows

    return (
        clean_display_text(start),
        clean_display_text(end),
        ("1-\u0432\u0430 \u0441\u043c\u044f\u043d\u0430", build_rows(*shifts_1[idx])),
        ("2-\u0440\u0430 \u0441\u043c\u044f\u043d\u0430", build_rows(*shifts_2[idx])),
    )


def render_trip_sheet_png(line, car, bus, driver1, driver2):
    from PIL import Image, ImageDraw

    font_scale = 1
    layout_scale = 1
    logo_scale = 1

    def sc(value, scale=layout_scale):
        return int(round(value * scale))

    start, end, first_shift, second_shift = build_trip_sheet_rows(line, car)
    shifts = [first_shift, second_shift]

    width = sc(1500)
    margin = sc(42)
    header_h = sc(150)
    title_h = sc(100)
    info_h = sc(280)
    shift_title_h = sc(82)
    table_header_h = sc(76)
    row_h = sc(78)
    gap = sc(24)

    font_title = load_naryad_font(sc(76, font_scale), bold=True)
    font_header = load_naryad_font(sc(42, font_scale), bold=True)
    font_cell = load_naryad_font(sc(40, font_scale), bold=True)
    font_small = load_naryad_font(sc(38, font_scale), bold=True)

    max_rows = sum(len(rows) for _, rows in shifts)
    height = margin * 2 + header_h + title_h + info_h + gap * 3
    height += len(shifts) * (shift_title_h + table_header_h) + max_rows * row_h

    canvas = Image.new("RGBA", (width, height), "#f6f8fb")
    draw = ImageDraw.Draw(canvas)

    line_w = sc(2)
    draw.rounded_rectangle((margin, margin, width - margin, height - margin), radius=sc(14), fill="#ffffff", outline="#1f2937", width=line_w)
    draw.rounded_rectangle((margin + line_w, margin + line_w, width - margin - line_w, margin + header_h), radius=sc(12), fill="#e8f1ff", outline=None)
    draw.rectangle((margin + line_w, margin + header_h - sc(14), width - margin - line_w, margin + header_h), fill="#e8f1ff")
    draw.rectangle((margin, margin + header_h - sc(7), width - margin, margin + header_h), fill="#2563eb")

    logo_y = margin + sc(14)
    left_logo_w = sc(470, logo_scale)
    logo_h = sc(135, logo_scale)
    right_logo_w = sc(165, logo_scale)
    paste_logo(canvas, "leftlogo.png", (margin + sc(22), logo_y, margin + sc(22) + left_logo_w, logo_y + logo_h))
    paste_logo(canvas, "rightlogo.png", (width - margin - sc(24) - right_logo_w, logo_y, width - margin - sc(24), logo_y + logo_h))

    title = "\u041f\u042a\u0422\u0415\u041d \u041b\u0418\u0421\u0422"
    title_y = margin + header_h + sc(18)
    title_w = text_size(draw, title, font_title)[0]
    draw.rounded_rectangle(
        ((width - title_w) / 2 - sc(28), title_y - sc(8), (width + title_w) / 2 + sc(28), title_y + sc(100)),
        radius=sc(14),
        fill="#eef6ff",
        outline="#bfd7ff",
        width=line_w
    )
    draw.text(((width - title_w) / 2, title_y), title, fill="#0f172a", font=font_title)

    info_y = margin + header_h + title_h
    box_gap = sc(14)
    box_w = (width - margin * 2 - sc(40) - box_gap) // 2
    left_x = margin + sc(20)
    right_x = left_x + box_w + box_gap
    box_h = sc(76)

    draw_label_box(draw, (left_x, info_y, left_x + box_w, info_y + box_h), "\u041b\u0438\u043d\u0438\u044f", line, font_small, line_w, fill="#f8fbff")
    draw_label_box(draw, (right_x, info_y, right_x + box_w, info_y + box_h), "\u041f\u0421", bus, font_small, line_w, fill="#f8fbff")
    draw_label_box(draw, (left_x, info_y + box_h + sc(10), left_x + box_w, info_y + box_h * 2 + sc(10)), "\u041a\u043e\u043b\u0430", car, font_small, line_w, fill="#f8fbff")
    draw_label_box(draw, (right_x, info_y + box_h + sc(10), right_x + box_w, info_y + box_h * 2 + sc(10)), "\u0414\u0430\u0442\u0430", (datetime.now() + timedelta(days=1)).strftime("%d.%m.%Y"), font_small, line_w, fill="#f8fbff")

    drivers_y = info_y + box_h * 2 + sc(24)
    draw_label_box(draw, (left_x, drivers_y, left_x + box_w, drivers_y + box_h), "\u0412\u043e\u0434\u0430\u0447 1", driver1, font_small, line_w, fill="#fff7ed")
    draw_label_box(draw, (right_x, drivers_y, right_x + box_w, drivers_y + box_h), "\u0412\u043e\u0434\u0430\u0447 2", driver2, font_small, line_w, fill="#fff7ed")

    table_x = margin + sc(20)
    table_w = width - margin * 2 - sc(40)
    col_widths = [sc(520), sc(190), table_w - sc(520) - sc(190)]
    y = margin + header_h + title_h + info_h + gap

    for shift_name, rows in shifts:
        draw.rounded_rectangle((table_x, y, table_x + table_w, y + shift_title_h), radius=sc(10), fill="#1d4ed8", outline="#1e3a8a", width=line_w)
        draw_centered_text(draw, (table_x, y, table_x + table_w, y + shift_title_h), shift_name, font_header, fill="#ffffff")
        y += shift_title_h

        headers = [start, "\u0427\u0410\u0421", end]
        x = table_x

        for header, col_w in zip(headers, col_widths):
            draw.rectangle((x, y, x + col_w, y + table_header_h), fill="#dbeafe", outline="#334155", width=line_w)
            draw_centered_text(draw, (x, y, x + col_w, y + table_header_h), header, font_header, fill="#0f172a")
            x += col_w

        y += table_header_h

        for row_index, (dep, arr) in enumerate(rows):
            x = table_x
            row_fill = "#ffffff" if row_index % 2 == 0 else "#f4f8ff"
            values = [start, dep, f"{end} {arr}"]

            for value, col_w in zip(values, col_widths):
                draw.rectangle((x, y, x + col_w, y + row_h), fill=row_fill, outline="#334155", width=line_w)
                draw.text((x + sc(12), y + sc(16)), clean_display_text(value), fill="#111111", font=font_cell)
                x += col_w

            y += row_h

        y += gap

    output = io.BytesIO()
    canvas.convert("RGB").save(output, format="PNG", optimize=True)
    output.seek(0)
    return output


def make_trip_sheet_png_file(line, car, bus, driver1, driver2):
    image = render_trip_sheet_png(line, car, bus, driver1, driver2)
    tomorrow = datetime.now() + timedelta(days=1)
    safe_line = str(line).replace("/", "-")
    filename = f"puten-list-{tomorrow.strftime('%Y-%m-%d')}-{safe_line}-{car}-{bus}.png"
    return discord.File(image, filename=filename)


# ---------------- SEND TRIP SHEETS ----------------

async def send_trip_sheets(by_line, fallback_channel=None):
    channel = bot.get_channel(PTEN_CHANNEL_ID) or fallback_channel
    if not channel:
        return

    for line in by_line:
        for car, bus, f1, f2, *_ in by_line[line]:
            message = f"Пътен лист - линия {line}, кола {car}, ПС {bus}"

            try:
                file = make_trip_sheet_png_file(line, car, bus, f1, f2)
                await channel.send(message, file=file)
            except Exception as exc:
                print("TRIP_SHEET_PNG_ERROR:", repr(exc))
                sheet = generate_trip_sheet(line, car, bus, f1, f2)
                await channel.send(f"{message}\n```{sheet}```")


# ---------------- READY ----------------

@bot.event
async def on_ready():
    await init_db()
    await tree.sync(guild=discord.Object(id=GUILD_ID))

    if not auto_naryad.is_running():
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


# ---------------- EXT INFO ----------------

@tree.command(name="extinfo", description="Добави забележка към водач в наряда", guild=discord.Object(id=GUILD_ID))
@app_commands.rename(bus="бус", driver="шофьор", note="забележка")
async def extinfo(interaction: discord.Interaction, bus: int, driver: int, note: str):

    if interaction.user.id != OWNER_ID:
        await interaction.response.send_message("Нямаш право.", ephemeral=True)
        return

    note = note.strip()

    if not note:
        await interaction.response.send_message("Напиши текст за забележката.", ephemeral=True)
        return

    naryad_date = (datetime.now() + timedelta(days=1)).date()

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT driver1, driver2 FROM buses WHERE bus=$1",
            bus
        )

        if row is None:
            await interaction.response.send_message(f"Бус {bus} не е намерен.", ephemeral=True)
            return

        if driver not in (row["driver1"], row["driver2"]):
            await interaction.response.send_message(
                f"Шофьор {driver} не е записан към бус {bus}.",
                ephemeral=True
            )
            return

        await conn.execute("""
            INSERT INTO ext_info(bus, driver, naryad_date, note)
            VALUES($1, $2, $3, $4)
            ON CONFLICT (bus, driver, naryad_date)
            DO UPDATE SET note=$4
        """, bus, driver, naryad_date, note)

    await interaction.response.send_message(f"Добавена е забележка за {bus} / {driver}.")


# ---------------- NARYAD ----------------

@tree.command(name="naryad", description="Наряд", guild=discord.Object(id=GUILD_ID))
async def naryad(interaction: discord.Interaction):
    text, by_line = await generate_naryad_text(return_data=True)

    try:
        file = make_naryad_png_file(text, by_line)
        await interaction.response.send_message(
            "@everyone\nНарядът е готов. Пътните листове се пускат след него.",
            file=file,
            allowed_mentions=discord.AllowedMentions(everyone=True)
        )
    except Exception as exc:
        print("NARYAD_PNG_ERROR:", repr(exc))
        await interaction.response.send_message(
            "@everyone\n" + text,
            allowed_mentions=discord.AllowedMentions(everyone=True)
        )

    await send_trip_sheets(by_line, interaction.channel)


# ---------------- TRIPSHEETS ----------------

@tree.command(name="tripsheets", description="Пусни само пътни листове", guild=discord.Object(id=GUILD_ID))
async def tripsheets(interaction: discord.Interaction):
    if interaction.user.id != OWNER_ID:
        await interaction.response.send_message("Нямаш право.", ephemeral=True)
        return

    _, by_line = await generate_naryad_text(return_data=True)
    await send_trip_sheets(by_line, interaction.channel)
    await interaction.response.send_message("Пътните листове са пуснати.")


# ---------------- ROADINFO ----------------

@tree.command(name="roadinfo", description="Наряд + пътни листове", guild=discord.Object(id=GUILD_ID))
async def roadinfo(interaction: discord.Interaction):
    if interaction.user.id != OWNER_ID:
        await interaction.response.send_message("Нямаш право.", ephemeral=True)
        return

    text, by_line = await generate_naryad_text(return_data=True)
    try:
        file = make_naryad_png_file(text, by_line)
        await interaction.response.send_message(
            "Нарядът е готов. Пътните листове се пускат след него.",
            file=file
        )
    except Exception as exc:
        print("ROADINFO_NARYAD_PNG_ERROR:", repr(exc))
        await interaction.response.send_message(text)
    await send_trip_sheets(by_line, interaction.channel)


# ---------------- AUTO ----------------

@tasks.loop(minutes=1)
async def auto_naryad():
    now = datetime.now()
    if now.hour == 15 and now.minute == 0:
        channel = bot.get_channel(CHANNEL_ID)
        if channel:
            text, by_line = await generate_naryad_text(return_data=True)

            try:
                file = make_naryad_png_file(text, by_line)
                await channel.send(
                    "@everyone\nНарядът е готов. Пътните листове се пускат след него.",
                    file=file,
                    allowed_mentions=discord.AllowedMentions(everyone=True)
                )
            except Exception as exc:
                print("AUTO_NARYAD_PNG_ERROR:", repr(exc))
                await channel.send(
                    "@everyone\n" + text,
                    allowed_mentions=discord.AllowedMentions(everyone=True)
                )

            await send_trip_sheets(by_line, channel)

        await asyncio.sleep(60)


# ---------------- ГЕНЕРАТОР ----------------

async def generate_naryad_text(return_data=False):
    tomorrow = datetime.now() + timedelta(days=1)
    date_str = tomorrow.strftime("%d.%m.%Y")

    # стабилен random
    random.seed(tomorrow.toordinal())

    line_limits = get_line_limits_for_date(tomorrow)

    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM ext_info WHERE naryad_date < $1",
            tomorrow.date()
        )
        buses = await conn.fetch("SELECT * FROM buses")
        reserves = await conn.fetch("SELECT bus FROM reserves")
        broken = await conn.fetch("SELECT bus FROM broken")
        sick = await conn.fetch("SELECT driver FROM sick")
        assigned_reserves = await conn.fetch(
            "SELECT broken_bus, reserve_bus FROM assigned_reserves"
        )
        ext_infos = await conn.fetch(
            "SELECT bus, driver, note FROM ext_info WHERE naryad_date=$1",
            tomorrow.date()
        )

    if not buses:
        return (
            "Няма записани автобуси."
            if not return_data
            else ("Няма записани автобуси.", {})
        )

    broken_set = {r["bus"] for r in broken}
    sick_set = {r["driver"] for r in sick}
    rest_set = get_rest_drivers_for_date(tomorrow.date(), buses, sick_set)
    assigned_map = {r["broken_bus"]: r["reserve_bus"] for r in assigned_reserves}
    ext_info_map = {(r["bus"], r["driver"]): r["note"] for r in ext_infos}

    reserve_list = [r["bus"] for r in reserves]

    # FIX: махаме счупени резерви
    reserve_pool = [
        b for b in reserve_list
        if b not in assigned_map.values() and b not in broken_set
    ]

    # --- групиране на автобуси ---
    buses_1xxx = [b for b in buses if 1000 <= b["bus"] <= 1999]
    buses_2xxx = [b for b in buses if 2000 <= b["bus"] <= 2999]

    random.shuffle(buses_1xxx)
    random.shuffle(buses_2xxx)

    buses = buses_1xxx + buses_2xxx

    by_line = {}
    available_lines = list(line_limits.keys())
    random.shuffle(available_lines)

    for line in available_lines:
        limit = line_limits[line]
        assigned = 0

        # FIX: филтрираме директно от buses всеки път
        while assigned < limit:
            valid_buses = [
                b for b in buses
                if is_bus_allowed_on_line(b["bus"], line)
            ]
            valid_buses = sort_buses_for_line(line, valid_buses)

            if not valid_buses:
                break

            row = valid_buses[0]
            buses.remove(row)

            original_bus = row["bus"]
            bus = original_bus

            d1 = row["driver1"]
            d2 = row["driver2"]

            first, second = get_week_shift(d1, d2)

            # --- счупен автобус ---
            if original_bus in broken_set:
                assigned_reserve = assigned_map.get(original_bus)

                if assigned_reserve and is_bus_allowed_on_line(assigned_reserve, line):
                    bus = assigned_reserve
                else:
                    reserve_index = next(
                        (
                            index
                            for index, reserve_bus in enumerate(reserve_pool)
                            if is_bus_allowed_on_line(reserve_bus, line)
                        ),
                        None
                    )

                    if reserve_index is None:
                        continue

                    bus = reserve_pool.pop(reserve_index)

            # маркиране (само визуално)
            f1 = format_driver_status(first, sick_set, rest_set)
            f2 = "-"

            if second:
                f2 = format_driver_status(second, sick_set, rest_set)

            note_parts = []

            for note_driver in (first, second):
                if note_driver is None:
                    continue

                note = ext_info_map.get((original_bus, note_driver))

                if note:
                    note_parts.append(f"{note_driver}: {note}")

            note_text = "; ".join(note_parts) if note_parts else "-"

            by_line.setdefault(line, []).append(
                (assigned + 1, bus, f1, f2, note_text)
            )

            assigned += 1

    # ---------------- ТЕКСТ ----------------

    text = f"📋 НАРЯД ЗА {date_str}\n\n```"
    text += f"{'Линия':<6} | {'Кола':<4} | {'ПС':<6} | {'Водач1':<12} | {'Водач2':<12} | {'Забележка':<20}\n"
    text += "-" * 100 + "\n"

    for line in sorted(by_line.keys(), key=lambda x: str(x)):
        for car, bus, f1, f2, note in by_line[line]:
            text += f"{line:<6} | {car:<4} | {bus:<6} | {f1:<12} | {f2:<12} | {note:<20}\n"
        text += "-" * 100 + "\n"

    text += "```"

    # --- резерви ---
    text += "\nРЕЗЕРВИ:\n"
    text += ", ".join(map(str, sorted(reserve_list))) if reserve_list else "няма"

    # --- болнични ---
    text += "\n\nБОЛНИЧНИ:\n"
    text += ", ".join(map(str, sorted(sick_set))) if sick_set else "няма"

    # --- почивка ---
    text += "\n\nПОЧИВКА:\n"
    text += ", ".join(map(str, sorted(rest_set))) if rest_set else "няма"

    # --- ремонт ---
    text += "\n\nВ РЕМОНТ:\n"
    text += ", ".join(map(str, sorted(broken_set))) if broken_set else "няма"

    if return_data:
        return text, by_line

    return text

bot.run(TOKEN)

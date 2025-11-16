
import logging
import re
from typing import Optional
from fastapi import FastAPI, Query
from fastapi.responses import RedirectResponse, JSONResponse
from sqlalchemy import create_engine
import pandas as pd

# безопасный импорт langdetect
try:
    from langdetect import detect
except Exception:
    def detect(text: str) -> str:
        return "en"

from nlp.intent_detector import detect_intent
from sql.query_templates import get_sql_by_intent
from nlp.sql_generator import sql_by_llm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
app = FastAPI()

# Настройка подключения к MySQL
engine = create_engine(
    "mysql+mysqlconnector://case:1234@localhost:3306/nurayCase",
    pool_pre_ping=True
)

# --- helpers: распознаём месяц/день/год/город/карточку ---

RU_MONTHS_STEMS = {
    "январь": 1, "февраль": 2, "март": 3, "апрель": 4, "май": 5, "июнь": 6, "июль": 7,
    "август": 8, "сентябрь": 9, "октябрь": 10, "ноябрь": 11, "декабрь": 12
}
KZ_MONTHS_STEMS = {
    "қаңтар": 1, "ақпан": 2, "наурыз": 3, "сәуір": 4, "мамыр": 5, "маусым": 6, "шілде": 7,
    "тамыз": 8, "қыркүйек": 9, "қазан": 10, "қараша": 11, "желтоқсан": 12
}
EN_MONTHS = {
    "january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12
}

def detect_language(text: str) -> str:
    try:
        return detect(text)
    except Exception:
        return "en"

def extract_top_n(query: str, default_n: int = 5) -> int:
    q = query.lower()
    m = re.search(r"\bтоп[-\s]?(\d+)\b", q) or re.search(r"\btop[-\s]?(\d+)\b", q)
    if m:
        try:
            return max(1, int(m.group(1)))
        except:
            return default_n
    return default_n

def extract_month_year(query: str):
    """Ловим месяц (EN полные, RU/KZ по стемам) + опц. год"""
    q = query.lower()
    # EN
    for name, num in EN_MONTHS.items():
        if name in q:
            y = None
            ym = re.search(r"(19|20)\d{2}", q)
            if ym:
                try: y = int(ym.group(0))
                except: pass
            return num, y
    # RU/KZ
    for stems in (RU_MONTHS_STEMS, KZ_MONTHS_STEMS):
        for stem, num in stems.items():
            if stem in q:
                y = None
                ym = re.search(r"(19|20)\d{2}", q)
                if ym:
                    try: y = int(ym.group(0))
                    except: pass
                return num, y
    return None, None

def extract_specific_date(query: str):
    """
    Ищем паттерны 'December 15 [2023]' / '15 декабря 2023' / '15 қазан 2023' и т.п.
    Возвращаем (month, day, year|None) если нашли конкретный ДЕНЬ.
    """
    q = query.lower()

    # EN формат: <month name> <day> [year]
    for name, num in EN_MONTHS.items():
        m = re.search(rf"\b{name}\s+([0-3]?\d)\b", q)
        if m:
            day = int(m.group(1))
            y = None
            ym = re.search(r"(19|20)\d{2}", q)
            if ym:
                try: y = int(ym.group(0))
                except: pass
            return num, day, y

    # RU/KZ формат: <day> <месяц-стем>
    m = re.search(r"\b([0-3]?\d)\b", q)
    if m:
        day_candidate = int(m.group(1))
        for stems in (RU_MONTHS_STEMS, KZ_MONTHS_STEMS):
            for stem, num in stems.items():
                if stem in q:
                    y = None
                    ym = re.search(r"(19|20)\d{2}", q)
                    if ym:
                        try: y = int(ym.group(0))
                        except: pass
                    return num, day_candidate, y

    return None, None, None

def extract_city(query: str) -> Optional[str]:
    """
    Ищем город после 'в городе' / 'город' / 'in'.
    ВАЖНО: без IGNORECASE и с требованием заглавной буквы у города —
    это отсечёт 'by total revenue'.
    """
    # Только 'in', 'город', 'в городе'; убираем 'by', чтобы не было ложных срабатываний
    m = re.search(r"(?:в городе|город|in)\s+([A-Z][\w\-\s]+)", query)
    if m:
        candidate = m.group(1).strip()
        # простая защита от "Total Revenue"
        bad = {"Total", "Revenue", "total", "revenue"}
        if candidate.split()[0] in bad:
            return None
        return candidate
    return None

def extract_card_id(query: str) -> Optional[int]:
    m = re.search(r"\b(?:cid|card[\s\-_]?id)\s*[:#]?\s*(\d+)\b", query, flags=re.IGNORECASE)
    if m:
        try: return int(m.group(1))
        except: return None
    return None

def is_single_row_aggregate(sql: str) -> bool:
    s = sql.lower()
    has_agg = any(fn in s for fn in ("avg(", "sum(", "count(", "min(", "max("))
    has_group = " group by " in s
    return has_agg and not has_group

@app.get("/")
def root():
    return RedirectResponse(url="/docs")

@app.get("/ask")
def ask(
    query: str = Query(..., description="User question"),
    limit: int = Query(100, description="Max rows to return if SQL has no LIMIT")
):
    """
    Поддерживает: день (transactions_on_date), месяц (transactions_in_month / average_amount_in_month), базовые метрики/топы.
    """
    try:
        # 1) Параметры из текста — сперва пытаемся вытащить КОНКРЕТНУЮ ДАТУ (month+day)
        month_day = extract_specific_date(query)   # (month, day, year|None)
        month, day, year = month_day

        # если день не найден — откат к "только месяц/год"
        if day is None:
            month2, year2 = extract_month_year(query)
            if month is None:
                month = month2
            if year is None:
                year = year2

        city = extract_city(query)
        card_id = extract_card_id(query)
        top_n = extract_top_n(query, default_n=limit)

        # 2) Язык
        lang = detect_language(query)
        logger.info(f"Query: {query} | lang={lang} | month={month}, day={day}, year={year}")

        # 3) Интент (передаём month/day/year внутрь)
        intent = detect_intent(query, lang=lang, month=month, year=year, day=day)
        logger.info(f"Detected intent: {intent}")

        # 4) SQL
        sql = get_sql_by_intent(
            intent=intent,
            top_n=top_n,
            month=month,
            year=year,
            day=day,
            city=city,
            card_id=card_id
        )
        if not sql:
            sql = sql_by_llm(query, lang=lang)
            if not sql:
                return JSONResponse(status_code=400, content={"error": f"Could not generate SQL for intent: {intent}"})

        # не дописываем LIMIT к однострочным агрегатам/уже ограниченным
        if " limit " not in sql.lower() and not is_single_row_aggregate(sql):
            sql = sql.rstrip().rstrip(";") + f" LIMIT {int(limit)}"

        logger.info(f"SQL: {sql}")
        df = pd.read_sql(sql, con=engine)

        return {
            "query": query,
            "language": lang,
            "intent": intent,
            "params": {"top_n": top_n, "month": month, "day": day, "year": year, "city": city, "card_id": card_id, "limit": limit},
            "sql": sql,
            "count": len(df),
            "result": df.to_dict(orient="records")
        }

    except Exception as e:
        logger.exception("Error in /ask endpoint")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/health")
def health():
    try:
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        return {"status": "healthy"}
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "unhealthy", "error": str(e)})


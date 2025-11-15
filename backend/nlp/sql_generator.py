
# nlp/sql_generator.py
import os
import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)

_OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

SCHEMA = """
TABLE transactions (
    transaction_id TEXT,
    transaction_timestamp DATETIME,
    card_id INT,
    expiry_date TEXT,
    issuer_bank_name TEXT,
    merchant_id INT,
    merchant_mcc INT,
    mcc_category TEXT,
    merchant_city TEXT,
    transaction_type TEXT,
    transaction_amount_kzt DECIMAL,
    transaction_currency TEXT,
    original_amount TEXT,
    acquirer_country_iso TEXT,
    pos_entry_mode TEXT,
    wallet_type TEXT
);
"""

SYSTEM_MSG = (
    "You are an assistant that converts a short natural-language analytics question "
    "into a SINGLE MySQL SELECT query for the given schema. "
    "Rules: "
    "1) OUTPUT ONLY the SQL inside a ```sql code block. "
    '2) Use table and column names exactly as in the schema. '
    "3) NEVER use UPDATE/DELETE/INSERT/CREATE/ALTER/DROP. SELECT only. "
    "4) If month mentioned, use MONTH(transaction_timestamp) and optional YEAR(). "
    "5) If user asks for top N, use ORDER BY and LIMIT N. "
)

USER_TEMPLATE = """Schema:
{schema}

Task:
Convert this request into one safe MySQL SELECT query.

Request: {query}
"""

def _import_openai():
    try:
        from openai import OpenAI  # official SDK v1.x
        return OpenAI
    except Exception as e:
        logger.warning(f"OpenAI SDK not available: {e}")
        return None

def _extract_sql(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(r"```sql\s*(.*?)\s*```", text, flags=re.IGNORECASE | re.DOTALL)
    candidate = m.group(1).strip() if m else text.strip()
    candidate = candidate.split(";")[0].strip()  # первая инструкция
    if not re.match(r"(?is)^\s*select\b", candidate):
        return None
    return candidate

def sql_by_llm(query: str, lang: str = "en") -> Optional[str]:
    """
    Вернёт SELECT или None, если:
      - нет OPENAI_API_KEY,
      - нет SDK,
      - модель не вернула валидный SELECT.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.warning("OPENAI_API_KEY not set — LLM SQL generation disabled.")
        return None

    OpenAI = _import_openai()
    if OpenAI is None:
        return None

    try:
        client = OpenAI(api_key=api_key)
        user_msg = USER_TEMPLATE.format(schema=SCHEMA, query=query)

        resp = client.chat.completions.create(
            model=_OPENAI_MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_MSG},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.0,
        )

        content = resp.choices[0].message.content if resp and resp.choices else ""
        sql = _extract_sql(content)
        if not sql:
            logger.warning("LLM returned no valid SELECT SQL.")
            return None

        # защита от опасных операторов
        if re.search(r"(?is)\b(update|delete|insert|create|alter|drop|truncate|grant|revoke)\b", sql):
            logger.warning("LLM SQL contained a banned keyword.")
            return None

        return sql

    except Exception:
        logger.exception("LLM SQL generation failed")
        return None


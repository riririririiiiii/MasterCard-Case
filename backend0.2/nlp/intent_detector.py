
import logging
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_classifier = None
_labels = [
    "count_transactions",
    "top_cities",
    "average_amount",
    "average_amount_in_month",
    "transactions_in_month",
    "transactions_on_date",
    "unknown"
]

MERCHANT_WORDS = ("merchant", "merchants", "мерчант", "мерчанты")
REVENUE_WORDS = ("revenue", "доход", "табыс", "выручка", "total revenue", "sum", "amount", "сумма")
TOP_WORDS = ("top", "топ", "ең", "best", "most")

DECLINE_WORDS = ("decline", "declined", "decline rate", "отказ", "отклон", "деклайн", "reject", "rejected")
CID_WORDS = ("cid", "card id", "card_id")

def _load_classifier():
    global _classifier
    if _classifier is None:
        try:
            from transformers import pipeline
            _classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")
            logger.info("Classifier loaded successfully")
        except Exception as e:
            logger.warning(f"Failed to load classifier: {e}")
            _classifier = None
    return _classifier

def detect_intent(query: str,
                  lang: Optional[str] = "en",
                  month: Optional[int] = None,
                  year: Optional[int] = None,
                  day: Optional[int] = None) -> str:
    """
    Простые правила + (опционально) HF zero-shot.
    Если есть day -> transactions_on_date.
    Если есть month (без day) -> *_in_month.
    """
    q = query.lower()

    # День указан -> транзакции за конкретную дату
    if day is not None and month is not None:
        return "transactions_on_date"

    # Decline rate по карте (пример)
    if any(w in q for w in DECLINE_WORDS) and any(w in q for w in CID_WORDS):
        return "decline_rate_by_card"
    

    # Top-N merchants by revenue
    if any(t in q for t in TOP_WORDS) and any(m in q for m in MERCHANT_WORDS):
        # если в тексте явно про выручку/сумму — точно мерчанты по revenue
        if any(r in q for r in REVENUE_WORDS):
            return "top_merchants_by_revenue"
        # даже без слова revenue, "Top N merchants" логично воспринимать как топ по сумме
        return "top_merchants_by_revenue"



    # Средний чек ЗА месяц
    if month is not None and any(k in q for k in ("average", "avg", "средн", "орташа")):
        return "average_amount_in_month"

    # Все транзакции ЗА месяц
    if month is not None and any(k in q for k in ("в", "за", "in", "during", "ай", "айында")) and any(
        k in q for k in ("transaction", "transactions", "транзакц", "операц", "all", "все")
    ):
        return "transactions_in_month"

    # Базовые
    if any(k in q for k in ("total", "count", "сколько", "саны", "число", "всего")):
        return "count_transactions"
    if any(k in q for k in ("top", "топ", "cities", "город", "қала", "лучших")):
        return "top_cities"
    if any(k in q for k in ("average", "avg", "средн", "орташа", "amount", "сумм")):
        return "average_amount"

    # HF fallback
    clf = _load_classifier()
    if clf:
        try:
            res = clf(query, _labels)
            return res["labels"][0]
        except Exception as e:
            logger.warning(f"Classifier error: {e}")

    return "unknown"

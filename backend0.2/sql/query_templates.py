
from typing import Optional

def _safe_int(v, default=None):
    try:
        return int(v)
    except:
        return default

def build_transactions_in_month_sql(month: int, year: Optional[int] = None, limit: Optional[int] = None) -> str:
    where = [f"MONTH(transaction_timestamp) = {_safe_int(month, 1)}"]
    if year:
        where.append(f"YEAR(transaction_timestamp) = {_safe_int(year)}")
    base = f"""
        SELECT 
            transaction_id,
            transaction_timestamp,
            merchant_city,
            transaction_type,
            transaction_amount_kzt,
            wallet_type,
            pos_entry_mode,
            mcc_category
        FROM transactions
        WHERE {' AND '.join(where)}
        ORDER BY transaction_timestamp
    """
    if limit:
        base += f" LIMIT {_safe_int(limit, 100)}"
    return base

def build_transactions_on_date_sql(month: int, day: int, year: Optional[int] = None, limit: Optional[int] = None) -> str:
    where = [f"MONTH(transaction_timestamp) = {_safe_int(month, 1)}",
             f"DAY(transaction_timestamp) = {_safe_int(day, 1)}"]
    if year:
        where.append(f"YEAR(transaction_timestamp) = {_safe_int(year)}")
    base = f"""
        SELECT 
            transaction_id,
            transaction_timestamp,
            merchant_city,
            transaction_type,
            transaction_amount_kzt,
            wallet_type,
            pos_entry_mode,
            mcc_category
        FROM transactions
        WHERE {' AND '.join(where)}
        ORDER BY transaction_timestamp
    """
    if limit:
        base += f" LIMIT {_safe_int(limit, 100)}"
    return base

def get_sql_by_intent(
    intent: str,
    top_n: int = 10,
    month: Optional[int] = None,
    year: Optional[int] = None,
    day: Optional[int] = None,
    city: Optional[str] = None,
    card_id: Optional[int] = None
) -> Optional[str]:

    if intent == "count_transactions":
        return """
            SELECT COUNT(*) AS total_transactions
            FROM transactions
        """

    if intent == "average_amount":
        return """
            SELECT
                ROUND(AVG(transaction_amount_kzt), 2) AS average_amount
            FROM transactions
            WHERE transaction_amount_kzt IS NOT NULL
        """

    if intent == "average_amount_in_month" and month:
        where = [f"MONTH(transaction_timestamp) = {int(month)}", "transaction_amount_kzt IS NOT NULL"]
        if year:
            where.append(f"YEAR(transaction_timestamp) = {int(year)}")
        return f"""
            SELECT
                ROUND(AVG(transaction_amount_kzt), 2) AS average_amount
            FROM transactions
            WHERE {' AND '.join(where)}
        """

    if intent == "top_cities":
        return f"""
            SELECT merchant_city, COUNT(*) AS transaction_count
            FROM transactions
            WHERE merchant_city IS NOT NULL AND merchant_city <> ''
            GROUP BY merchant_city
            ORDER BY transaction_count DESC
            LIMIT {int(top_n)}
        """

    if intent == "transactions_in_month" and month:
        return build_transactions_in_month_sql(month=month, year=year, limit=None)

    if intent == "transactions_on_date" and month and day:
        return build_transactions_on_date_sql(month=month, day=day, year=year, limit=None)

    if intent == "top_merchants_by_revenue":
        return f"""
            SELECT
                merchant_id,
                SUM(transaction_amount_kzt) AS total_revenue,
                COUNT(*) AS tx_count
            FROM transactions
            WHERE merchant_id IS NOT NULL
            GROUP BY merchant_id
            ORDER BY total_revenue DESC
            LIMIT {int(top_n)}
        """

    if intent == "decline_rate_by_card" and card_id:
        # ПРИМЕЧАНИЕ: адаптируй под свою схему статусов
        where = [f"card_id = {int(card_id)}"]
        if month:
            where.append(f"MONTH(transaction_timestamp) = {int(month)}")
        if day:
            where.append(f"DAY(transaction_timestamp) = {int(day)}")
        if year:
            where.append(f"YEAR(transaction_timestamp) = {int(year)}")
        where_sql = " AND ".join(where)
        return f"""
            SELECT
                SUM(CASE WHEN auth_status = 'Declined' THEN 1 ELSE 0 END) AS declined_count,
                COUNT(*) AS attempt_count,
                ROUND(100.0 * SUM(CASE WHEN auth_status = 'Declined' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS decline_rate_pct
            FROM transactions
            WHERE {where_sql}
        """

    return None

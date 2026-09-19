"""조정 계산 — 미국 계획 03 §2.

**조정된 값을 저장하지 않는다.** 원시값과 이벤트를 저장하고 읽을 때 계산한다.
기준 시점 ``T``를 바꾸는 것만으로 어느 시점 기준의 계열이든 다시 만든다.

```
price_adj(t; T)  = close(t)  × Π ( for_factor / to_factor )
volume_adj(t; T) = volume(t) × Π ( to_factor / for_factor )
      단 s 는  t < ex_date(s) ≤ T  인 모든 분할
```

**거래량은 반대 방향이다.** 7:1이면 가격은 1/7, 주식 수는 7배다. 같은 계수를
걸면 거래대금이 49분의 1이 되는데 **값이 그럴듯해서 눈으로는 안 잡힌다**
(06 §2.2).

누적곱은 DuckDB의 ``product()`` 윈도우로 낸다. ``exp(sum(ln(x)))``로 풀면
7:1 같은 유리수에서 부동소수점 오차가 붙는다.
"""

from __future__ import annotations

SPLIT_KIND = "split"


def adjusted_prices_sql(
    *,
    prices: str = "prices_daily",
    corp_actions: str = "corp_actions",
    as_of: str,
) -> str:
    """``prices``에 ``adj_factor``·``close_adj``·``volume_adj``를 붙이는 SQL.

    ``as_of``가 기준 시점 ``T``다. 그날까지 일어난 분할만 반영한다 —
    **미래 분할로 과거를 조정하면 그 시점에 몰랐던 정보가 들어간다.**

    ``ex_date == t`` 인 분할은 ``t`` 에 적용하지 않는다 (식이 ``t < ex_date``다).
    분할 당일 가격은 이미 분할 뒤 값이기 때문이다.
    """
    return f"""
    WITH sp AS (
        SELECT symbol,
               ex_date,
               CAST(for_factor AS DOUBLE) / CAST(to_factor AS DOUBLE) AS f
        FROM {corp_actions}
        WHERE kind = '{SPLIT_KIND}'
          AND ex_date <= DATE '{as_of}'
          AND to_factor > 0
          AND for_factor > 0
    ),
    upto AS (
        -- ex_date 이하 분할의 누적곱. ASOF로 각 거래일이 자기 이하 최신 것을 집는다.
        SELECT symbol,
               ex_date,
               product(f) OVER (
                   PARTITION BY symbol ORDER BY ex_date
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               ) AS cum
        FROM sp
    ),
    tot AS (
        SELECT symbol, product(f) AS total FROM sp GROUP BY symbol
    ),
    joined AS (
        SELECT p.*, COALESCE(u.cum, 1.0) AS cum_upto
        FROM {prices} p
        ASOF LEFT JOIN upto u
          ON p.symbol = u.symbol AND p.date >= u.ex_date
    )
    SELECT j.*,
           COALESCE(t.total, 1.0) / j.cum_upto                      AS adj_factor,
           CAST(j.open  AS DOUBLE) * (COALESCE(t.total, 1.0) / j.cum_upto) AS open_adj,
           CAST(j.high  AS DOUBLE) * (COALESCE(t.total, 1.0) / j.cum_upto) AS high_adj,
           CAST(j.low   AS DOUBLE) * (COALESCE(t.total, 1.0) / j.cum_upto) AS low_adj,
           CAST(j.close AS DOUBLE) * (COALESCE(t.total, 1.0) / j.cum_upto) AS close_adj,
           -- 반대 방향이다. 같은 계수를 걸면 거래대금이 망가진다.
           CAST(j.volume AS DOUBLE) / (COALESCE(t.total, 1.0) / j.cum_upto) AS volume_adj
    FROM joined j
    LEFT JOIN tot t ON j.symbol = t.symbol
    """

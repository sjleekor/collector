"""미국 시장 수집.

경로는 :class:`collector.lake.DataRoot`로 조립한다 — 이 패키지는 자체 경로
타입을 두지 않는다. 평소에는 ``DataRoot.resolve(market=MARKET)``, 변형 lake를
읽을 때는 ``DataRoot(base=...)``다.
"""

MARKET = "us"

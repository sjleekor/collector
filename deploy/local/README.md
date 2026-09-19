# deploy/local — 미국 상시 운영

**미국은 sj2-server로 안 간다** (미국 계획 D15). 이 맥에서 `launchd`로 돈다.
서버 쪽에는 래퍼도 Cronicle 이벤트도 만들지 않는다.

```bash
deploy/local/install-launchd.sh install     # 매일 15:00 KST
deploy/local/install-launchd.sh status
deploy/local/install-launchd.sh uninstall
```

`plist`는 저장소에 없다. 경로가 기계마다 다르고 이 저장소는 public이라
`install-launchd.sh`가 `~/Library/LaunchAgents/`에 만든다.

| 무엇 | 어디 |
|---|---|
| 실행 로그 | `~/Library/Logs/collector-us/us-daily.log` |
| launchd 자체 로그 | 같은 디렉터리의 `launchd.{out,err}.log` |
| 산출물 | **로그가 아니다.** `$STOCK_DATA_ROOT/us/` 에 있다 |

## 왜 15:00인가

DoltHub가 **05:30 UTC = 14:30 KST**에 전일 데이터를 커밋한다. 그 뒤에 돌린다.
미국 장 마감(05:00\~06:00 KST)과 겹치지 않는다 — 전일 데이터라 급하지 않다.

## 맥이 꺼져 있던 날

**따로 메꾸지 않는다.** `collector us-daily run`이 할 일을 일정이 아니라
`raw/`에 무엇이 있나로 만든다. 마지막으로 받은 것부터 어제까지가 저절로
대상이 된다. 며칠치가 밀리면 `--budget-seconds` 안에서 되는 만큼 하고
나머지는 다음 실행이 이어서 한다.

```bash
direnv exec . uv run collector us-daily run --dry-run   # 할 일만 센다
```

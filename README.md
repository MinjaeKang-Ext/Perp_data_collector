# SK hynix perpetual and spot-data collector

SK하이닉스 본주(KRX 000660)와 ADR(Nasdaq SKHY)을 추종하는 무기한 선물의 실시간 시장 데이터를 KST 기준 Parquet으로 저장한다.

**운영 환경: AWS 아시아 태평양(도쿄) 리전 (`ap-northeast-1`)의 서버를 이용한 데이터 수집 파이프라인.** 수집 데이터는 한국 시간(KST)을 기준으로 저장한다. 아래의 WAL 부하 측정치는 별도로 표시한 로컬 환경의 결과이며 AWS 서버의 성능 측정치가 아니다.

## 프로젝트 구성

- `main.py`, `streaming_collector.py`, `kiwoom_collector.py`: 시장 데이터 수집 실행 및 거래소별 스트림 처리
- `reference_price_collector.py`: 독립적인 기준가격 수집
- `stream_parquet_store.py`, `stream_wal.py`, `parquet_store.py`: 원본 저장과 복구
- `process_raw_data.py`, `reconcile_parquet.py`: 수집 데이터 가공 및 사후 정리
- `binance_fetcher.py`, `hyperliquid_fetcher.py`: 거래소 REST 데이터 조회
- `tests/`, `smoke_test.py`, `kiwoom_smoke_test.py`, `wal_stress_test.py`: 오프라인 검증과 선택적인 실시간·부하 테스트
- `.github/workflows/`: 수동 실행하는 5분 수집 작업 (GitHub Actions 실행 환경)

수집 데이터, 로그, 로컬 환경설정과 인증 키는 저장소에 포함하지 않는다.

| Underlying | Binance USDⓈ-M | Hyperliquid HIP-3 |
| --- | --- | --- |
| SK하이닉스 본주 | `SKHYNIXUSDT` | `xyz:SKHX` |
| SK하이닉스 ADR | `SKHYUSDT` | `xyz:SKHY` |

## 수집 범위

| 거래소 | 데이터셋 | 원천 | 저장 단위 |
| --- | --- | --- | --- |
| Binance | `agg_trades` | `@aggTrade` | 100ms 집계 체결 이벤트 |
| Binance | `l2_updates` | `@depth@100ms` | 100ms 증분 L2 변경 레벨 |
| Binance | `l2_snapshots` | REST depth | 시작·L2 재연결 직후 및 30초마다 L2 앵커 스냅샷 |
| Binance | `bbo` | `@bookTicker` | 최우선 bid/ask 변경 이벤트 |
| Binance | `index_prices` | `reference_price_collector.py`의 `@markPrice@1s` | 1초 단위 외부 구성가격 기준 `indexPrice` |
| Binance | `mark_prices` | `reference_price_collector.py`의 `@markPrice@1s` | 1초 단위 청산 기준 `markPrice`와 예상 펀딩 정보 |
| Binance | `funding` | REST funding history | 확정 펀딩비 이력 및 주기적 갱신 |
| Hyperliquid | `trades` | `trades` WebSocket | 개별 체결 이벤트 |
| Hyperliquid | `l2_snapshots` | `l2Book` WebSocket | 블록 기반 L2 스냅샷 (약 500ms) |
| Hyperliquid | `bbo` | `bbo` WebSocket | BBO 변경 이벤트 |
| Hyperliquid | `oracle_prices` | `reference_price_collector.py`의 `activeAssetCtx` | 오라클 가격 변경 이벤트 |
| Hyperliquid | `mark_prices` | `reference_price_collector.py`의 `activeAssetCtx` | 마크가격 변경 이벤트 및 mid/funding/OI 문맥 |
| Hyperliquid | `funding` | REST funding history | 시간당 확정 펀딩비 이력 및 주기적 갱신 |
| Kiwoom KRX | `trades` | `0B` WebSocket | 개별 현물 체결 이벤트 |
| Kiwoom KRX | `l2_snapshots` | `0D` WebSocket | 10단계 호가잔량 스냅샷 |
| Kiwoom KRX | `bbo` | `0C` WebSocket | 최우선 bid/ask 변경 이벤트 |

Binance L2는 스냅샷이 아니라 **증분 변경 데이터**다. `l2_updates`의 update ID를 사용해 `l2_snapshots` 앵커 이후의 호가창을 재구성한다. 수량 `0`은 해당 가격대 삭제다. Binance L2/BBO WebSocket이 연결 또는 재연결되어 구독을 완료할 때마다 REST depth 앵커를 즉시 하나 저장하고, 이후에도 기본 30초 주기를 유지한다. Hyperliquid L2는 완결된 스냅샷이며 `l2Book` 재구독 직후 거래소가 보내는 최신 스냅샷을 그대로 저장한다.

기준가격은 한 파일에 섞지 않는다. Binance `@markPrice@1s` 한 메시지는
`index_prices`와 `mark_prices`에 같은 거래소 시각으로 각각 기록된다. Hyperliquid
`activeAssetCtx`는 고정 주기가 아닌 변경 이벤트이며, 문서상 거래소 시각을 제공하지
않는다. 따라서 그 `oracle_prices`/`mark_prices`의 `event_time`은 정확한 수신 시각이고
`event_time_source=received_at`으로 명시된다. 두 시장 모두 `event_time`과
`received_at`을 보존하므로 장중·장외 기준가격 갱신 패턴을 별도로 분석할 수 있다.

## 설치 및 실행

```bash
python3 -m pip install -r requirements.txt
TRADING_DATA_DIR="$HOME/trading_data" \
TRADING_STREAM_FLUSH_SECONDS=5 \
python3 main.py
```

`main.py`는 오직 불변 원본(raw) part만 기록한다. 따라서 수집 중에 한 시간치 L2를
메모리로 합치지 않으며, 종료 시에도 대형 병합을 수행하지 않는다.

저장 병목을 확인할 수 있도록 수집기는 60초마다 `Stream store:` 계측 로그를 남긴다.
로그에는 비교하기 쉽도록 `kst=` 시각도 포함된다.
`buffered`/`peak_buffered`는 현재/최대 메모리 버퍼 행 수,
`max_append_lock_wait_ms`는 WebSocket 콜백이 저장 잠금을 기다린 최장 시간,
`max_write_ms`는 한 번의 raw Parquet 변환·쓰기 최장 시간이다. 값은 프로세스 시작
이후 전체가 아닌 **직전 약 60초 구간**의 합계/최대치다. 기본 설정에서는 기존
저장 방식과 5초·1만 행 기준을 유지한다.

실험적인 저장 스레드 분리 경로는 기본적으로 **꺼져 있다**(`TRADING_ASYNC_PARQUET_WRITES=0`).
`1`로 켜면 콜백은 Parquet을 직접 쓰지 않고, 별도 저장 스레드가 5초 주기 또는 행 수
도달 신호에 따라 raw Parquet part를 기록한다. 활성·대기·저장 중 메모리 행은
`TRADING_ASYNC_MAX_PENDING_ROWS`와 `TRADING_ASYNC_MAX_PENDING_BYTES`(추정 크기)를
함께 적용한다. 넘친 행은 raw 루트의 `.stream_spool/*.json`에 원자적으로 기록하고,
저장 스레드가 Parquet 반영 후 파일을 제거한다. 디스크 스풀은 바이트·파일 수 상한을
적용하며, 재시작하면 남은 스풀 배치를 고정 part ID로 다시 처리한다. **스풀로
넘긴 행만 강제 종료 후 재처리 가능하다. 일반 메모리 버퍼의 행은 강제 종료 시
유실될 수 있다.** 또한 콜백은 넘침 시 스풀의 JSON 직렬화·디스크 동기화를 기다리므로
디스크가 느리면 WebSocket 처리도 지연될 수 있다. 바이트 수는 Python 객체 크기의
추정치이지 프로세스 RSS의 엄격한 상한은 아니다.

스풀 할당량·디스크 기록 실패는 `COLLECTION GAP / STORAGE FATAL` 로그와
`.stream_spool/gaps/` 표식을 남기고 수집기를 종료한다. 디스크 자체가 꽉 찬 경우
표식 기록도 실패할 수 있으므로 CRITICAL 로그를 함께 보관해야 한다. 표식 또는
미완성 `.tmp` 파일이 있으면 다음 실행은 자동 시작하지 않는다. 원인을 조사하고
누락 구간을 확인한 뒤 별도로 처리해야 하며, 파일을 무심코 삭제해 재시작하면 안 된다.
정상 종료(`Ctrl+C`/`SIGTERM`)는 모든 생산 스레드가 멈출 때까지 기다린 뒤 메모리와
스풀을 최종 기록한다. `SIGKILL`·전원 장애에서는 일반 메모리 버퍼를 보장하지 않는다.
**실서버 적용은 다음 단계의 장시간 부하·복구 검증 전까지 보류한다.**

### 3.5단계: WAL 우선 수집(실험적)

`TRADING_WAL_FIRST=1`은 기존의 메모리 우선/넘침 스풀 경로를 **대체하는 별도 모드**다.
`TRADING_ASYNC_PARQUET_WRITES=1`과 함께 켤 수 없다. 기본값은 여전히 `0`이므로 현재
서버 명령은 변경되지 않는다. WebSocket 콜백은 수신 시각과 원본 프레임을
`raw/.stream_wal/`의 길이·체크섬이 있는 회전 로그에 먼저 기록하고 매 프레임 디스크
동기화가 끝난 후 반환한다. 별도 저장 스레드는 완성된 로그 세그먼트를 파싱하여 기존
거래소/자산/데이터셋/KST 파티션의 Parquet part를 만든다. 출력 파일과 상위 폴더가
동기화된 뒤에만 원본 세그먼트를 삭제한다. 재시작 시 남은 로그를 같은 part ID로 다시
처리하므로 Parquet 쓰기 중단 후에도 같은 행을 중복 part로 늘리지 않는다. 기존
5초 flush는 이 모드에서 로그 세그먼트를 마감·가공하는 주기이지 미기록 메모리의
보존 한계가 아니다.

Binance/Hyperliquid 시장·기준가격, Kiwoom `REAL` 프레임이 원본 메시지 WAL을 쓴다.
Kiwoom 로그인/PING 제어 메시지와 토큰은 기록하지 않는다. Binance REST L2 앵커는
원본 WebSocket 프레임이 없으므로 정규화된 행 묶음을 같은 WAL에 기록한다. 펀딩비는
기존의 별도 REST 이력·재조회 경로를 유지하며 이 WAL의 대상이 아니다. 기존
`.stream_spool`에 미처리 파일이 있으면 WAL 모드 시작을 막으므로 먼저 이전 수집을
안전하게 종료·복구해야 한다.

이 모드는 **수신 콜백이 매번 WAL 디스크 동기화를 기다린다**. 디스크 지연이나
WAL 용량 한도 때문에 콜백이 밀리면 WebSocket 공백이 생길 수 있으며, 그런 실패는
치명 오류로 알려준다. 이미 WAL에 기록된 데이터의 프로세스 강제 종료 복구는 가능하지만,
프로세스가 WAL 기록 *전*에 죽었거나 서버가 꺼져 있던 동안 거래소가 보내지 않은
이벤트까지 복구할 수는 없다. 체크섬이 맞지 않거나 끝이 잘린 로그는 자동으로
버리지 않고 시작을 중단한다. 미처리 WAL이 남아 있으면 `flush_rows`, 시간 파티션
유예값, 가공 버전이 달라진 설정으로 재시작할 수 없다. 파서의 출력 형식을 바꾸는
코드 수정 시 `WAL_PROJECTION_VERSION`을 올리고 기존 WAL을 먼저 원래 버전으로
비워야 한다. **로컬 4단계 검증만으로 운영 서버 개장 직후 안정성이 입증된 것은 아니다.**

### 4단계: WAL 부하 검증과 서버 시험 절차

`wal_stress_test.py`는 거래소에 연결하지 않고 실제 수집기 콜백에 합성 메시지를
주입한다. 시장 수집기(Binance, Hyperliquid, 키움)와 별도 기준가격 수집기의 WAL과
Parquet 경로를 동시에 사용한다. 인자 없는 실행은 임시 폴더를 만들고 시험 후
삭제한다. 서버 디스크에서 측정할 때는 **비어 있는 시험 전용 폴더**를 지정한다.

```bash
python3 wal_stress_test.py --root "$HOME/perp_wal_benchmark" \
  --sustained-fps 50 --sustained-seconds 20 \
  --burst-fps 200 --burst-seconds 10
```

2026-10-29 로컬 macOS 시험에서는 3,000개 프레임에서 예상·실제 Parquet 행이
모두 23,250개로 일치했고, 마지막 WAL 잔량은 0이었다. 전체 콜백 진입→WAL 기록
완료 지연은 P50 0.40ms, P95 1.93ms, **P99 91.62ms, 최대 579.85ms**였다.
가장 오래된 행의 Parquet 완료 시간은 part 기준 P99 약 7.13초, WAL 최대 사용량은
약 0.64MB, 생산 종료 후 WAL 해소는 4.13초, 프로세스 RSS 최고값은 약 133MB였다.
같은 부하를 60초 지속+30초 급증으로 연장한 시험도 9,000개 프레임과 69,750개
Parquet 행이 정확히 일치하고 WAL 잔량 0·gap 표식 0이었다. 이때 전체 콜백
지연 P99는 97.90ms, 최대는 690.23ms, WAL 최고 사용량은 약 0.74MB,
생산 종료 후 해소는 2.26초였다.
Parquet part마다 100ms 지연을 주입한 짧은 시험도 1,249개 프레임의 행 수가
일치하고 WAL이 비워졌다. WAL 한도를 1KiB로 줄인 고장 주입에서는 수집기가
`COLLECTION GAP`을 기록하고 종료 코드 1과 gap 표식 1개를 남겼다. 이미 기록에
성공한 행은 모두 Parquet에 반영됐다. 이 수치는 **합성 30초 시험과 이 로컬 디스크**의
결과일 뿐 실제 개장 10분·AWS 디스크·거래소 연결 상태의 성능을 나타내지 않는다.
특히 P99 꼬리가 크므로 현재 결과만으로 운영 전환을 승인할 수 없다.

서버 시험을 한다면 기존 수집을 정상 종료하고 이전 코드·데이터 폴더는 그대로
보존한다. 새 릴리스 파일을 별도 실행 폴더에 복사한 뒤 같은 서버 디스크에서 위
오프라인 시험을 먼저 돌린다. 이후 `TRADING_DATA_DIR`과
`TRADING_REFERENCE_DATA_DIR`을 서로 다른 **새 시험용 경로**로 지정해 두 수집기를
시작한다. 키움 계정의 중복 로그인 정책을 확인하기 전에는 구버전·신버전 키움
수집기를 동시에 실행하지 않는다. 시험 중에는 60초 간격 저장 로그의
`wal_ack_p95_ms`, `wal_ack_p99_ms`, `wal_ack_max_ms`, `wal_bytes`와 디스크 여유,
프로세스 메모리, 재연결·`COLLECTION GAP`을 확인한다. 지연 꼬리가 지속적으로
100ms를 넘거나 WAL 잔량이 계속 증가하거나 gap/치명 오류가 나타나면 시험을
중단하고 원인을 조사한다. 위 지연 백분위는 각 보고 구간의 **최근 최대 20,000개**
WAL 쓰기 표본이며 수신 콜백 전체 시간은 아니다.

되돌릴 때는 새 프로세스에 `Ctrl+C`/`SIGTERM`을 보내 종료·flush가 끝난 것을
확인하고, 보존해 둔 이전 코드와 원래 데이터 경로에서 기존 프로세스를 재시작한다.
시험 경로의 WAL·Parquet은 삭제하거나 기존 경로에 덮어쓰지 말고, 시간·이벤트 ID로
별도 검정한 뒤 합친다. **서버 배포와 실시간 개장 검증은 별도 승인 및 서버 접근이
필요하다.**

기존 수집을 중단하지 않고 기준가격만 추가하려면, **별도 tmux 창**에서 아래
전용 프로세스를 실행한다. 기본값은 기존 데이터 루트의 형제 폴더
`trading_data_reference_prices`이며, 명시적인 별도 경로를 권장한다. 이 프로세스는
체결·L2·BBO·펀딩·키움에 연결하지 않으므로 기존 `main.py`와 안전하게 병행할 수 있다.
또한 전용 실행 파일은 기존 `streaming_collector.py`를 import하거나 수정하지 않는다.
이미 서버 프로젝트에 있는 범용 Parquet 저장 모듈만 재사용한다.

```bash
TRADING_REFERENCE_DATA_DIR="$HOME/trading_reference_data" \
python3 reference_price_collector.py
```

Binance와 Hyperliquid 수집에는 API key가 필요 없다. 키움 현물 수집은 `KIWOOM_APP_KEY`와 `KIWOOM_APP_SECRET`이 필요하다. Binance 서버 위치가 지역 제한(HTTP 451)에 걸리면 Binance REST 앵커·펀딩 작업은 재시도 로그를 남기며 실패할 수 있고, Hyperliquid 스트림은 별도로 계속 동작한다. Binance WebSocket도 같은 네트워크 정책의 영향을 받을 수 있으므로 서버에서 사전에 짧게 확인해야 한다.

기본 경로는 `data/trading_data_<login>_<hostname>`이다. 공유 서버에서는 사용자 홈 아래의 고유 경로를 명시한다.

```bash
TRADING_DATA_DIR="$HOME/trading_data" \
TRADING_STREAM_FLUSH_SECONDS=5 \
TRADING_HOURLY_FINALIZATION_GRACE_SECONDS=300 \
TRADING_FUNDING_POLL_SECONDS=300 \
python3 main.py
```

원본 가공은 **별도 프로세스**로 실행한다. 기본적으로 완료된 시간보다 330초 뒤에
처리하므로, 5분 지연 허용 구간과 5초 flush가 끝난 뒤의 일반 raw part만 10분 단위
`data.parquet`으로 만든다.

```bash
python3 process_raw_data.py \
  --raw-root "$HOME/trading_data/raw" \
  --processed-root "$HOME/trading_data/processed" \
  --interval-seconds 300 \
  --delay-seconds 330
```

기준가격 데이터도 별도 트리이므로 같은 처리기를 별도 프로세스로 실행한다.

```bash
python3 process_raw_data.py \
  --raw-root "$HOME/trading_reference_data/raw" \
  --processed-root "$HOME/trading_reference_data/processed" \
  --interval-seconds 300 \
  --delay-seconds 330
```

| 환경 변수 | 기본값 | 설명 |
| --- | ---: | --- |
| `TRADING_DATA_DIR` | `data/trading_data_<login>_<hostname>` | 사용자 전용 데이터 루트 |
| `TRADING_REFERENCE_DATA_DIR` | `TRADING_DATA_DIR`의 형제 `*_reference_prices` | 기준가격 전용 프로세스의 독립 데이터 루트. `TRADING_DATA_DIR`와 같게 설정 불가 |
| `TRADING_STREAM_FLUSH_ROWS` | `10000` | 데이터셋별 즉시 flush 행 수 |
| `TRADING_STREAM_FLUSH_SECONDS` | `5` | 메모리 버퍼 flush 주기 |
| `TRADING_ASYNC_PARQUET_WRITES` | `0` | 실험적 저장 스레드 분리(`1`), 4단계 부하 검증 전 운영 사용 보류 |
| `TRADING_WAL_FIRST` | `0` | 실험적 원본 WAL 우선 수집(`1`), 넘침 전용 비동기 모드와 동시 사용 불가 |
| `TRADING_WAL_MAX_BYTES` | `4294967296` | 미처리 WAL 디스크 상한(기본 4 GiB) |
| `TRADING_WAL_SEGMENT_BYTES` | `8388608` | 한 WAL 세그먼트의 목표 크기(기본 8 MiB); 5초마다도 마감 |
| `TRADING_ASYNC_MAX_PENDING_ROWS` | `50000` | 분리 경로의 활성·대기·저장 중 메모리 행 수 상한. 초과분은 디스크 스풀 기록 |
| `TRADING_ASYNC_MAX_PENDING_BYTES` | `67108864` | 실험적 분리 경로의 메모리 행 추정 바이트 상한(기본 64 MiB) |
| `TRADING_ASYNC_MAX_SPOOL_BYTES` | `4294967296` | 복구 가능한 디스크 스풀의 최대 크기(기본 4 GiB) |
| `TRADING_ASYNC_MAX_SPOOL_FILES` | `20000` | 디스크 스풀 파일 수 상한 |
| `TRADING_HOURLY_FINALIZATION_GRACE_SECONDS` | `300` | 이 시간보다 늦게 수신된 과거 이벤트를 raw `late_parts`로 보낼 기준 |
| `TRADING_PROCESS_SECONDS` | `300` | `process_raw_data.py`의 처리 검사 주기 |
| `TRADING_PROCESS_DELAY_SECONDS` | `330` | 정상 raw 시간 파티션을 가공하기 전 대기 시간. grace+flush보다 커야 함 |
| `TRADING_PROCESS_BATCH_ROWS` | `65536` | Arrow 가공기의 최대 메모리 배치 행 수 |
| `TRADING_RAW_RETENTION_DAYS` | `3` | raw 삭제를 명시적으로 켰을 때의 최소 보관 기간 |
| `TRADING_BINANCE_L2_SNAPSHOT_SECONDS` | `30` | Binance L2 REST 앵커 주기 |
| `TRADING_WS_RECONNECT_INITIAL_SECONDS` | `0.5` | WebSocket 연결 실패 직후 첫 재시도 기준 대기 시간(초) |
| `TRADING_WS_RECONNECT_MAX_SECONDS` | `30` | WebSocket 재시도 대기 시간 상한(초) |
| `TRADING_WS_RECONNECT_RESET_SECONDS` | `60` | 이 시간 이상 안정적으로 연결된 뒤에만 재시도 지연을 0.5초로 초기화(초) |
| `TRADING_WS_RECONNECT_JITTER_FRACTION` | `0.2` | 동시 재접속 집중을 피하는 ±무작위 지연 비율 (`0` 이상 `1` 미만) |
| `TRADING_FUNDING_POLL_SECONDS` | `300` | 확정 펀딩비 확인 주기 |
| `TRADING_FUNDING_BACKFILL_START` | `2026-06-02T00:00:00Z` | 첫 실행 펀딩 이력 시작 시각 |
| `TRADING_WS_CA_BUNDLE` | 미설정 | 사내 TLS 프록시를 쓰는 서버의 CA PEM 경로. TLS 검증은 계속 유지 |
| `KIWOOM_APP_KEY` / `KIWOOM_APP_SECRET` | 미설정 | 둘 다 있으면 KRX `000660` 수집 활성화. 코드·Git에 저장 금지 |
| `KIWOOM_MODE` | `real` | `real` 또는 `demo` |
| `KIWOOM_STOCK_CODES` | `000660` | 쉼표로 구분한 KRX 종목 코드 |

`Ctrl+C` 또는 `SIGTERM`으로 정상 종료하면 생산 스레드가 멈춘 후 메모리와 남은 스풀을 flush한다. 서버에서는 수집기와 가공기를 각각 `systemd`나 `tmux`로 실행해야 하며, GitHub Actions는 장기 WebSocket 연결용으로 적합하지 않다.

모든 WebSocket(키움을 포함한 메인 수집기와 기준가격 전용 수집기)은 끊긴 뒤 기본적으로 `0.5 → 1 → 2 → 4 → 8 → 16 → 30초`의 지수 백오프로 재연결한다. 각 대기 시간에는 기본 ±20% jitter가 들어가므로 동시에 복구되는 여러 프로세스가 한 번에 접속하지 않는다. 연결이 60초 이상 유지된 경우에만 다음 실패의 재시도 시간을 다시 0.5초부터 시작한다.

## 저장 구조

모든 시간 열과 파티션은 KST(`Asia/Seoul`)다. 수집기는 5초마다 작은 불변 raw part를 쓴다. 가공기는 Arrow 배치(기본 65,536행)로 원본을 읽어 10분 단위 `data.parquet`을 원자적으로 생성한다. 따라서 pandas로 한 시간치 파일들을 한꺼번에 읽지 않는다. 일반 출력이 완료된 뒤 발견된 원본은 기존 파일을 다시 쓰지 않고 processed `late_parts`에 보존한다.

```text
trading_data/                                      # TRADING_DATA_DIR
  raw/                                              # main.py가 append만 수행
    binance/SKHYNIXUSDT/l2_updates/
      date=YYYY-MM-DD/hour=HH/parts/part-*.parquet
      date=YYYY-MM-DD/hour=HH/late_parts/part-*.parquet
    hyperliquid/xyz_SKHX/trades/date=.../hour=.../parts/part-*.parquet
    kiwoom/000660/l2_snapshots/date=.../hour=.../parts/part-*.parquet
    binance/SKHYNIXUSDT/funding/date=YYYY-MM-DD/data.parquet
  processed/                                        # process_raw_data.py
    binance/SKHYNIXUSDT/l2_updates/
      date=YYYY-MM-DD/hour=HH/minute=00/data.parquet
      date=YYYY-MM-DD/hour=HH/minute=00/late_parts/part-*.parquet
    hyperliquid/xyz_SKHX/trades/date=.../hour=.../minute=10/data.parquet
    kiwoom/000660/l2_snapshots/date=.../hour=.../minute=20/data.parquet
    _manifests/.../normal.json                      # 입력·출력 행 수 검증 기록
trading_reference_data/                             # 별도 기준가격 수집·가공 트리
  raw/...
  processed/...
```

고빈도 가공 경로는 정확히 `거래소 / 자산 / 데이터셋 / KST 날짜 / KST 시간 / KST 10분 / Parquet` 순서다. 각 manifest는 source row 수와 output row 수가 같은지 확인한 뒤에만 생성된다. 과거 시각 이벤트가 매우 늦게 오면 그 10분의 `data.parquet`를 재작성하지 않고 `late_parts`에 남긴다. 분석할 때는 해당 10분의 `data.parquet`와 `late_parts/*.parquet`를 함께 읽는다. 확정 펀딩비는 저빈도 일별 raw 파일로 보존된다.

### 서버 복사와 로컬 재정리

수집 프로세스를 멈추지 않고 raw·processed 트리를 복사할 수 있다. 각 파일은 완성 후 원자적으로 노출된다. 최근 raw는 재가공을 위해 보관하고, 장기 보관은 processed를 기준으로 한다. raw 삭제는 기본적으로 실행되지 않는다. 충분히 검증한 뒤에만 아래처럼 명시적으로 켠다.

```bash
python3 process_raw_data.py \
  --raw-root "$HOME/trading_data/raw" \
  --processed-root "$HOME/trading_data/processed" \
  --delete-processed-raw \
  --raw-retention-days 3
```

기존 형식의 복사본을 시간별 파일 하나로 합치려면 별도 로컬 경로에서 다음 명령을 실행한다.

```bash
python3 reconcile_parquet.py \
  --source-root /local/copied/trading_data \
  --output-root /local/reconciled/trading_data
```

원본 서버 데이터와 이미 존재하는 출력 폴더는 절대 덮어쓰지 않는다. 이 도구도 Arrow 배치로 시간별 `data.parquet`, `parts`, `late_parts`를 합치며, 행을 제거하거나 전체 시간 구간을 정렬하지 않는다. 중복 제거·전역 정렬은 분석 단계에서 명시적으로 수행한다.

`event_time`은 거래소 제공 시각, `received_at`은 수신 시각이다. 지연 분석에는 두 열의 차이를 사용한다.

## SK하이닉스 현물(KRX `000660`) 데이터

키움 REST API WebSocket으로 KRX `000660`을 수집한다. `0B`(체결), `0C`(우선호가), `0D`(10단계 호가잔량)를 구독하며, BBO는 `0C`, microprice용 수량은 `0D`에서 얻는다. `KIWOOM_APP_KEY`와 `KIWOOM_APP_SECRET`이 모두 설정된 경우에만 Kiwoom 수집기가 시작된다.

전체 호가 원장 또는 상업적 재배포 목적의 원시 실시간 데이터를 원하면 KRX/KOSCOM의 라이선스 데이터 피드를 별도로 계약해야 한다. OpenAPI로는 수신을 시작한 이후의 데이터를 직접 적재해 이력화한다.

키움 App Key/Secret은 실행 명령에 직접 쓰지 말고, 서버의 secret manager 또는 권한 `0600`의 systemd credential/file에서 서비스 프로세스에만 주입한다. 환경변수는 Git과 Parquet에 남지 않지만, 같은 사용자·관리자 권한의 프로세스 조회나 잘못된 디버그 로그에 노출될 수 있어 완전한 비밀 저장소가 아니다.

## 검증

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -B -c "from pathlib import Path; [compile(Path(path).read_text(), path, 'exec') for path in ('main.py', 'reference_price_collector.py', 'stream_parquet_store.py', 'process_raw_data.py', 'kiwoom_collector.py')]"
python3 -m unittest discover -s tests -v
```

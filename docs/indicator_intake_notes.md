# Indicator Intake Notes

이 문서는 외부 Pine Script 지표를 우리 자동매매 봇에 이식할 때,
"전체 복제"가 아니라 "실전 로직으로 가져올 핵심만" 정리하는 메모다.

## Intake Rules

- 차트 시각화 전용 로직은 가져오지 않는다.
- 미래 데이터 누수 가능성이 있는 처리(`lookahead`, 확정 전 pivot 남용)는 주의한다.
- 봇에는 "판단용 feature"로만 넣고, 직접 진입 조건은 별도 검증 후 연결한다.
- 우선순위는 `signal quality 개선 -> missed opportunity 감소 -> false positive 억제` 순서다.

## 1. LuxAlgo Smart Money Concepts

### 가져올 후보

1. BOS / CHoCH
- 구조 전환 감지용
- 현재 `higher timeframe bias`를 단순 EMA/VWAP 기반에서 구조 기반으로 보강 가능

2. EQH / EQL
- 유동성 풀, 스윕, 반전 컨텍스트 판단용
- 지금 놓치는 early reversal 자리에 도움 가능

3. FVG
- 되돌림 진입, continuation 재진입, inefficiency 복귀 판단용

4. Premium / Discount
- 방향 생성용이 아니라 자리 품질 필터용
- 예: 롱은 discount 우대, 숏은 premium 우대

5. Order Block
- 전체 이식보다 단순화해서 "최근 유효 공급/수요 구간"만 계산하는 방식이 적합

### 주의

- TradingView 박스/라벨/라인 렌더링은 불필요
- `request.security(... lookahead_on)` 류는 미래 데이터 누수 위험이 있어 그대로 금지
- 실전 이식 1순위는 `BOS/CHoCH + EQH/EQL + FVG`

## 2. BigBeluga Swing Profile

### 코드의 핵심 의미

- 스윙 고점/저점 사이 한 레그를 잘라서
- 그 구간의 가격대별 거래량 분포를 쌓고
- PoC, buy/sell volume, delta volume을 보여주는 구조

### 가져올 후보

1. Swing Leg Volume Profile
- 최근 완료된 swing leg 내부에서 가격대별 거래량 집중 구간 계산
- "어디서 많이 거래됐는가"를 레그 단위로 볼 수 있음

2. PoC (Point of Control)
- 최근 스윙 레그 내 최대 거래량 가격대
- 진입 후 장애물/자석 구간 판단에 활용 가능

3. Buy/Sell Volume Split
- 단순 총 거래량 말고 레그 단위 매수/매도 우세 판단 가능
- 현재 `volume_ratio`보다 더 입체적임

4. Delta Volume
- 레그 단위 체결 방향성 요약
- exploratory entry에서 추세 지속/반전 압력 확인용으로 좋음

5. Swing-by-Swing Context
- 단일 캔들 지표가 아니라 "한 레그가 어떻게 끝났는지"를 판단하는 feature로 사용 가능

### 봇 이식 시 후보 feature

- `swing_poc_price`
- `swing_poc_distance_pct`
- `swing_volume_total`
- `swing_volume_delta_pct`
- `swing_buy_sell_ratio`
- `swing_profile_acceptance`
- `swing_profile_rejection`

### 실전 해석 방향

1. 가격이 swing POC 위에서 안착 + delta 양수
- continuation long 가산점

2. 가격이 swing POC 아래에서 rejection + delta 음수
- continuation short 가산점

3. discount/profit zone + bullish CHoCH + positive swing delta
- early reversal long 보강 조건 후보

4. premium zone + bearish CHoCH + negative swing delta
- early reversal short 보강 조건 후보

### 주의

- Pine 코드의 box/polyline/label 생성은 전부 불필요
- 현재 구현은 시각화 비중이 높아 그대로 이식 가치 낮음
- 핵심은 "스윙 구간 내 가격대별 거래량 분포"만 계산하는 것
- 계산 비용이 커질 수 있으니 우선은 최근 완료된 swing leg 1~2개만 대상으로 제한

## 현재까지 우선 구현 후보 정리

### Tier 1

- BOS / CHoCH
- EQH / EQL
- FVG

### Tier 2

- swing POC
- swing delta volume
- swing buy/sell ratio
- squeeze on/off
- squeeze momentum slope
- session VWAP stdev position
- session VWAP band stretch/reclaim

### Tier 3

- simplified order block
- premium / discount zones

## 추후 실제 반영 순서 제안

1. `smc_features.py`
- BOS
- CHoCH
- EQH/EQL
- FVG proximity

2. `swing_profile_features.py`
- last completed swing leg
- POC
- delta
- buy/sell split

3. `volatility_features.py`
- squeeze on/off
- squeeze release
- momentum histogram slope
- expansion follow-through

4. strategy / ai context 연결
- rule score
- exploratory score
- opportunity review 원인 분석 필드

## 3. LazyBear Squeeze Momentum

### 코드의 핵심 의미

- 볼린저 밴드와 켈트너 채널의 포함 관계로
  `변동성 압축(squeeze)`과 `압축 해제(release)`를 본다.
- 동시에 선형회귀 기반 히스토그램으로
  압축 이후 어느 방향으로 힘이 붙는지 본다.

### 가져올 후보

1. Squeeze On / Off
- 변동성이 눌려 있는지, 이제 확장되기 시작했는지 판단 가능
- 지금 봇의 `resume candle`이나 `breakout`보다 조금 더 빠른 준비 신호가 될 수 있음

2. Momentum Histogram Direction
- 단순 양/음뿐 아니라 증가 중인지 감소 중인지가 중요
- long/short continuation 진입 품질 보강 가능

3. Expansion Follow-through
- squeeze 해제 직후 2~4개 봉 동안 실제 follow-through가 붙는지 판단
- exploratory entry를 너무 빨리 여는 문제를 줄이는 데 도움 가능

### 봇 이식 시 후보 feature

- `squeeze_on`
- `squeeze_off`
- `squeeze_fired_recently`
- `squeeze_momentum_value`
- `squeeze_momentum_slope`
- `squeeze_long_bias`
- `squeeze_short_bias`

### 실전 해석 방향

1. squeeze on + 구조 전환 없음
- 대기 또는 watch 강화

2. squeeze off + momentum slope 양수 + reclaim 계열 setup
- long continuation / reversal 초기 진입 가산점

3. squeeze off + momentum slope 음수 + reject 계열 setup
- short continuation / reversal 초기 진입 가산점

4. squeeze on 상태에서 억지 진입
- 신호 품질 감점

### 주의

- 이 지표 단독으로 방향 결정하면 whipsaw가 많을 수 있음
- 반드시 구조(BOS/CHoCH/FVG) 또는 개별 모멘텀과 결합해서 써야 함
- "압축"은 진입 신호라기보다 "곧 움직일 가능성"에 더 가까움

## 4. VWAP Stdev Bands

### 코드의 핵심 의미

- 일간 세션 기준 VWAP를 만들고
- 그 위아래 표준편차 밴드를 여러 단계로 나눠
- 현재 가격이 세션 평균에서 얼마나 멀리 이탈했는지 본다.

### 가져올 후보

1. Session VWAP Position Grade
- 단순 `VWAP 위/아래`가 아니라
  `중심`, `1차 이탈`, `2차 이탈`, `극단 이탈`로 등급화 가능

2. VWAP Band Stretch
- 밴드 바깥으로 얼마나 과하게 뻗었는지 측정 가능
- mean reversion / exhaustion / breakout 실패 구간 판단에 도움 가능

3. VWAP Reclaim / Reject
- 밴드 바깥까지 갔다가 세션 VWAP 또는 1차 밴드를 다시 회복하는지 확인 가능
- 현재 봇의 `vwap reclaim`을 더 정교하게 바꿀 수 있음

4. Previous Session VWAP
- 이전 세션 VWAP를 참고 축으로 사용 가능
- intraday 지지/저항 또는 수급 기준선으로 유용할 수 있음

### 봇 이식 시 후보 feature

- `session_vwap`
- `session_vwap_std`
- `session_vwap_zscore`
- `session_vwap_band_rank`
- `session_vwap_extreme_long`
- `session_vwap_extreme_short`
- `session_vwap_reclaim`
- `session_vwap_reject`
- `prev_session_vwap_distance_pct`

### 실전 해석 방향

1. `VWAP 아래`만으로 롱 차단하지 말고
- `-0.5σ`, `-1.0σ`, `-2.0σ`처럼 위치를 등급화해서
- 과매도 반등인지 단순 약세 continuation인지 나눠서 봐야 함

2. `-2σ 이하` + bullish CHoCH + positive delta
- early reversal long 가산점 후보

3. `+2σ 이상` + bearish CHoCH + negative delta
- early reversal short 가산점 후보

4. `VWAP reclaim`
- reclaim 이후 1차 밴드 위 안착이면 continuation 점수 가산 가능

5. `VWAP reject`
- reclaim 실패 후 다시 VWAP 아래/위로 밀리면 short/long reject setup으로 활용 가능

### 주의

- 세션 초기에는 표준편차 값이 불안정할 수 있음
- 밴드 이탈 자체를 신호로 쓰면 역추세 남발 위험 있음
- 반드시 구조 신호, 거래량, 미시 유동성과 결합해야 함

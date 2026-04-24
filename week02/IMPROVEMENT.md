# DORA Metrics 개선안

## 현재 판단

현재 저장소는 운영 배포 환경과 production deployment 기록이 아직 없으므로 DORA 지표를 GitHub 저장소 활동 기반의 프록시 지표로 수집한다. 이 방식은 과제 자동화에는 적합하지만, 실제 운영 성능을 대표하려면 배포 이벤트와 장애 복구 기록을 추가로 연결해야 한다.

## 지표별 개선 방향

| 지표 | 현재 수집 방식 | 개선 방향 |
| --- | --- | --- |
| Lead Time | PR의 첫 commit부터 merge까지의 시간 | PR을 작게 쪼개고 리뷰 대기 시간을 줄인다. |
| Deployment Frequency | 기본 브랜치의 성공한 workflow run 수 | 실제 배포 workflow 또는 GitHub deployment 이벤트로 전환한다. |
| MTTR | 실패 workflow 이후 다음 성공 workflow까지의 시간 | 장애 issue label과 복구 commit을 연결해 실제 복구 시간을 측정한다. |
| Change Failure Rate | 실패/취소/타임아웃 workflow 비율 | 배포 후 rollback, hotfix, incident issue를 함께 반영한다. |

## 다음 작업

1. 배포 workflow가 생기면 `deployment_status=success` 이벤트를 기준으로 Deployment Frequency를 계산한다.
2. 장애 또는 실패를 `incident`, `rollback`, `hotfix` label로 관리한다.
3. PR template에 검증 결과와 영향 범위를 작성하게 해 Lead Time 분석에 필요한 맥락을 남긴다.
4. 주간 workflow 결과를 artifact로 저장하고, 보고서가 자동 생성되는지 확인한다.
5. 대시보드는 현재 SVG/Chart.js 시안에서 시작하고, 데이터가 쌓이면 추세 그래프를 추가한다.

## 예상 성과 및 일정표

| 단계 | 기간 | 목표 | 예상 성과 |
| --- | --- | --- | --- |
| 1 | 1주 이내 | DORA workflow 안정화 | 수동 개입 없이 JSON artifact와 보고서 생성 |
| 2 | 2주 이내 | PR/Actions 데이터 누적 | Lead Time과 workflow 실패율의 초기 기준선 확보 |
| 3 | 3주 이내 | issue/label 기반 장애 기록 도입 | MTTR과 Change Failure Rate 해석 정확도 개선 |
| 4 | 4주 이내 | 대시보드 추세 그래프 추가 | 지표 변화와 병목 지점을 주간 단위로 파악 |
| 5 | 배포 workflow 도입 후 | deployment event 기반 지표 전환 | production에 가까운 Deployment Frequency 측정 |

## 주의사항

- 이 문서는 공개 가능한 저장소 운영 지표 개선안만 다룬다.
- ESG 지표 산식, 계산 규칙, 원본 데이터, 정답 데이터는 포함하지 않는다.
- 실제 production 장애가 없는 상태에서는 MTTR과 Change Failure Rate가 제한적으로 해석되어야 한다.

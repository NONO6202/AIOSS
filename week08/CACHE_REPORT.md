# Workflow Optimization Report

## Comparison Target

| 구분 | Run | Workflow | Commit | Result | Duration |
| --- | --- | --- | --- | --- | ---: |
| Before | [24886395644](https://github.com/NONO6202/AIOSS/actions/runs/24886395644) | Week 07 CI Pipeline | `7d1794d` | success | 34s |
| After | [24886706800](https://github.com/NONO6202/AIOSS/actions/runs/24886706800) | Week 08 Optimized CI Pipeline | `6cc70e3` | success | 65s |

이 표는 Week 07 기본 CI와 Week 08 확장 CI의 구조 차이를 보여준다. Week 08은 matrix가 4개 조합에서 9개 조합으로 확장되었기 때문에 단순 wall-clock 비교에서는 시간이 증가한다. 캐시 개선률은 아래의 동일 조건 cache on/off 비교를 기준으로 판단한다.

## Baseline Measurement

Week 07 기준 실행은 `2026-04-24T11:08:38Z`에 생성되고 `2026-04-24T11:09:12Z`에 완료되어 전체 실행 시간은 34초였다.

| Job | Duration |
| --- | ---: |
| Build | 7s |
| Test (ubuntu-latest, Node 20) | 7s |
| Test (ubuntu-latest, Node 22) | 10s |
| Test (macos-latest, Node 20) | 11s |
| Test (macos-latest, Node 22) | 14s |
| Deploy artifact preview | 5s |

## Optimization Changes

| 항목 | Before | After |
| --- | --- | --- |
| Node setup 중복 | build/test job에 반복 작성 | composite action `.github/actions/setup-node-project/action.yml`로 분리 |
| Lint/Test 로직 중복 | workflow 내부 steps에 직접 작성 | reusable workflow `.github/workflows/reusable-node-check.yml`로 분리 |
| Matrix | 2 OS x 2 Node = 4 combinations | 3 OS x 3 Node = 9 combinations |
| Cache | `actions/setup-node` npm cache 사용 | composite action에서 동일 cache를 중앙화 |
| Deploy 조건 | `main` 또는 수동 실행 | `main` 또는 수동 실행 + 변경 파일 감지 결과 |
| 변경 파일 감지 | 없음 | `detect-changes` job과 `scripts/detect_changed_files.mjs` |

## Improvement Formula

동일한 workflow, 동일한 commit, 동일한 3 OS x 3 Node matrix 조건에서 dependency cache만 끄고 켠 뒤 아래 식으로 개선률을 계산한다.

```text
improvement_rate = (before_seconds - after_seconds) / before_seconds * 100
```

측정값:

```text
before_seconds = 80  # dependency-cache=false
after_seconds = 77   # dependency-cache=true
improvement_rate = 3.8%
```

## Cache Measurement

| 구분 | Run | Trigger | Commit | Cache | Result | Duration |
| --- | --- | --- | --- | --- | --- | ---: |
| Cache off | [24890033951](https://github.com/NONO6202/AIOSS/actions/runs/24890033951) | `workflow_dispatch` | `603f36e` | disabled | success | 80s |
| Cache on | [24890097305](https://github.com/NONO6202/AIOSS/actions/runs/24890097305) | `workflow_dispatch` | `603f36e` | enabled | success | 77s |

동일 commit에서 `workflow_dispatch` 입력 `dependency-cache=false`와 `dependency-cache=true`만 바꾸어 측정했다. 전체 workflow 기준 개선폭은 3초이며, 개선률은 3.8%다.

| Job | Cache off | Cache on |
| --- | ---: | ---: |
| Detect changed files | 4s | 5s |
| Build / build (ubuntu-latest, Node 20) | 12s | 11s |
| Test (ubuntu-latest, Node 20) | 11s | 15s |
| Test (ubuntu-latest, Node 22) | 10s | 9s |
| Test (ubuntu-latest, Node 24) | 14s | 10s |
| Test (macos-latest, Node 20) | 10s | 9s |
| Test (macos-latest, Node 22) | 9s | 9s |
| Test (macos-latest, Node 24) | 14s | 12s |
| Test (windows-latest, Node 20) | 29s | 23s |
| Test (windows-latest, Node 22) | 31s | 41s |
| Test (windows-latest, Node 24) | 47s | 23s |
| Deploy artifact preview | 4s | 7s |

## After Measurement

Week 08 최적화 실행은 `2026-04-24T11:16:41Z`에 생성되고 `2026-04-24T11:17:46Z`에 완료되어 전체 실행 시간은 65초였다.

| Job | Duration |
| --- | ---: |
| Detect changed files | 6s |
| Build / build (ubuntu-latest, Node 20) | 9s |
| Test (ubuntu-latest, Node 20) | 11s |
| Test (ubuntu-latest, Node 22) | 6s |
| Test (ubuntu-latest, Node 24) | 20s |
| Test (macos-latest, Node 20) | 12s |
| Test (macos-latest, Node 22) | 14s |
| Test (macos-latest, Node 24) | 13s |
| Test (windows-latest, Node 20) | 25s |
| Test (windows-latest, Node 22) | 26s |
| Test (windows-latest, Node 24) | 36s |
| Deploy artifact preview | 4s |

## Interpretation

Week 08은 matrix를 4개 조합에서 9개 조합으로 확장하므로 전체 wall-clock 시간이 반드시 줄어드는 형태의 최적화만 목표로 하지 않는다. 이번 최적화의 핵심은 다음이다.

- 반복 setup 코드 제거
- 캐시 설정 중앙화
- reusable workflow로 유지보수성 개선
- 변경 파일 감지를 통한 불필요한 deploy skip
- artifact handoff 검증 유지
- Linux/macOS/Windows와 Node 20/22/24 조합으로 검증 범위 확대

따라서 첫 실행 기준 전체 시간은 34초에서 65초로 증가했다. 이는 테스트 조합이 4개에서 9개로 늘어난 영향이 크며, Week 08의 개선 지표는 단순 시간 단축보다 중복 제거, 캐시 중앙화, 조건부 deploy, cross-platform matrix 확장에 둔다.

캐시 on/off만 비교한 동일 조건 측정에서는 전체 workflow가 80초에서 77초로 줄어 3.8% 개선되었다. Windows Node 24 job은 47초에서 23초로 크게 줄었지만, runner 상태와 네트워크 편차 때문에 일부 job은 증가했다. 따라서 리포트에서는 전체 workflow 기준 개선률과 job별 편차를 함께 기록한다.

## Conditional Deploy Result

이번 run은 workflow, script, week08 문서가 함께 변경되었기 때문에 `detect-changes` 결과상 deploy가 필요한 변경으로 판단되어 `Deploy artifact preview` job이 실행되었다.

문서만 변경되거나 배포와 무관한 파일만 변경되는 경우에는 `scripts/detect_changed_files.mjs` 결과에 따라 deploy job을 건너뛰도록 구성했다.

## Evidence Links

- Optimized workflow: [.github/workflows/ci.yml](../.github/workflows/ci.yml)
- Reusable workflow: [.github/workflows/reusable-node-check.yml](../.github/workflows/reusable-node-check.yml)
- Composite action: [.github/actions/setup-node-project/action.yml](../.github/actions/setup-node-project/action.yml)
- Change detection script: [scripts/detect_changed_files.mjs](../scripts/detect_changed_files.mjs)
- Before run: [24886395644](https://github.com/NONO6202/AIOSS/actions/runs/24886395644)
- After run: [24886706800](https://github.com/NONO6202/AIOSS/actions/runs/24886706800)
- Cache off run: [24890033951](https://github.com/NONO6202/AIOSS/actions/runs/24890033951)
- Cache on run: [24890097305](https://github.com/NONO6202/AIOSS/actions/runs/24890097305)

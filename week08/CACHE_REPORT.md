# Workflow Optimization Report

## Comparison Target

| 구분 | Run | Workflow | Commit | Result | Duration |
| --- | --- | --- | --- | --- | ---: |
| Before | [24886395644](https://github.com/NONO6202/AIOSS/actions/runs/24886395644) | Week 07 CI Pipeline | `7d1794d` | success | 34s |
| After | First Week 08 optimized run | Optimized CI Pipeline | pending push | pending | pending |

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

최적화 후 첫 Actions 실행이 완료되면 아래 식으로 개선률을 계산한다.

```text
improvement_rate = (before_seconds - after_seconds) / before_seconds * 100
```

현재 기준값:

```text
before_seconds = 34
after_seconds = pending
improvement_rate = pending
```

## Expected Interpretation

Week 08은 matrix를 4개 조합에서 9개 조합으로 확장하므로 전체 wall-clock 시간이 반드시 줄어드는 형태의 최적화만 목표로 하지 않는다. 이번 최적화의 핵심은 다음이다.

- 반복 setup 코드 제거
- 캐시 설정 중앙화
- reusable workflow로 유지보수성 개선
- 변경 파일 감지를 통한 불필요한 deploy skip
- artifact handoff 검증 유지

## Post-Push Update Rule

Week 08 workflow를 push한 뒤 첫 성공 run의 duration을 `After` 행에 기록하고, 개선률을 수치로 갱신한다.

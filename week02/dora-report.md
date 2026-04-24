# DORA Metrics Report

Generated at: 2026-04-24T05:55:51.190Z

Repository: `NONO6202/AIOSS`  
Branch: `main`  
Window: 30 days

## Summary

| Metric | Value | Collection Rule |
| --- | ---: | --- |
| Lead Time | N/A hours | merged PR 기준 첫 commit 시각부터 merge 시각까지의 중앙값. PR commit 조회 실패 시 PR 생성 시각을 사용한다. |
| Deployment Frequency | 0.0 / week | 지정 기간 동안 기본 브랜치에서 성공한 workflow run 수를 주 단위로 환산한 값. |
| MTTR | N/A hours | 실패한 workflow run 이후 같은 workflow와 branch에서 다음 성공 run까지 걸린 평균 시간. |
| Change Failure Rate | N/A% | 완료된 workflow run 중 failure, cancelled, timed_out, action_required 결론의 비율. |

## Counts

| Signal | Count |
| --- | ---: |
| Local commits | 7 |
| Merged PRs | 0 |
| Completed workflow runs | 0 |
| Successful workflow runs | 0 |
| Failed workflow runs | 0 |
| Recovered failures | 0 |

## Notes

- 이 저장소는 운영 배포 시스템이 없으므로 GitHub 활동 데이터 기반의 DORA 프록시 지표를 수집한다.
- Deployment Frequency는 production deployment가 없을 때 기본 브랜치의 성공한 workflow run 수로 대체한다.
- MTTR은 실패한 workflow run 이후 같은 workflow/branch에서 다음 성공 run까지의 시간으로 계산한다.
- Change Failure Rate는 완료된 workflow run 중 실패/취소/타임아웃 비율로 계산한다.
- 로컬 실행에는 GitHub token이 없어 local git commit 정보만 fallback으로 사용했다.
- GitHub Actions 실행 시에는 GITHUB_TOKEN으로 PR과 workflow run 데이터를 수집한다.

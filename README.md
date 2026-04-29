# AIOSS

AI Open Source Software 과제 저장소입니다.

이 저장소는 1주차부터 15주차까지의 과제 산출물을 관리하는 공간입니다.

## Upstream Reference

이 저장소는 과제 제출을 위해 개인 GitHub에서 관리하며, 로컬 Git remote에는 원본 프로젝트 참조가 `upstream`으로 등록되어 있습니다.

- Origin: https://github.com/NONO6202/AIOSS
- Upstream: https://github.com/ESGAgent/ESG_Agent

```bash
git remote -v
```

```text
origin    git@github.com:NONO6202/AIOSS.git (fetch)
origin    git@github.com:NONO6202/AIOSS.git (push)
upstream  https://github.com/ESGAgent/ESG_Agent.git (fetch)
upstream  https://github.com/ESGAgent/ESG_Agent.git (push)
```

## 학기 프로젝트

학기 프로젝트 주제는 **ESG 지표산정 자동화 Agent**입니다.

이 프로젝트는 기존에 수기로 수행되던 기업 건전성/ESG 지표 산정 과정을 자동화하는 것을 목표로 합니다. 전년도 수행 결과를 인계받아 개선하는 상황이며, LLM에만 의존하지 않고 **Rule-Based 수집 및 계산 -> Agent 보완 수집 -> Agent 검증 보조 -> 리포트 작성** 흐름으로 설계합니다.

공개 가능한 프로젝트 증빙 자료는 [project/](project/README.md)에 정리했습니다.

| Path | 설명 |
| --- | --- |
| [project/README.md](project/README.md) | 공개 제출용 프로젝트 증빙 자료 안내 |
| [project/Rulebased_code_redacted/](project/Rulebased_code_redacted/README.md) | Rule-Based 구현 구조를 보여주는 검열된 pseudocode 성격 자료 |
| [project/Agent_plan_redacted/](project/Agent_plan_redacted/README.md) | Agent 보완 수집, 검증 보조, 리포트 작성 계획 |

`project/Rulebased_code_redacted/`는 실행 가능한 원본 코드가 아니라 공개 안전성을 위해 검열한 구조 증빙 자료입니다. 실제 산식, 임계값, 가중치, 등급표, 원본 데이터, 정답 데이터, 기관/기업 식별 정보, 원천 URL, 내부 설정은 포함하지 않으며, 민감한 숫자와 등급은 `1`, `2`, `3`, `4` 또는 placeholder로 통일했습니다.

## OSS 기본 구조

이 저장소는 AIOSS 과제 제출을 위한 public OSS 형식 저장소이며, 다음 기본 문서를 포함합니다.

- [LICENSE](LICENSE): MIT License
- [CONTRIBUTING.md](CONTRIBUTING.md): 기여 범위, 브랜치 전략, 커밋 규칙, PR 리뷰 규칙
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md): 협업 행동 규범

라이선스는 이 과제 저장소의 공개 산출물에 적용되며, 별도 비공개 프로젝트의 ESG 산식, 내부 계산 규칙, 원본 데이터, 정답 데이터에는 적용하지 않습니다.

## 과제 목록

- [Week 01](week01/README.md)
- [Week 02](week02/README.md)
- [Week 03](week03/README.md)
- [Week 04](week04/README.md)
- [Week 05](week05/README.md)
- [Week 06](week06/README.md)
- [Week 07](week07/README.md)
- [Week 08](week08/README.md)

## 생성형 AI 활용

이 저장소의 문서 작성 및 정리 과정에는 생성형 AI를 활용했습니다. 최종 내용은 직접 검토하고 수정했습니다.

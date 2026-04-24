# 환경 설정 증빙

## 개발 환경 체크리스트

| 항목 | 상태 | 증빙 |
| --- | --- | --- |
| Git 설치 및 설정 | 완료 | `git config user.name`, `git config user.email` 확인 |
| GitHub 계정 생성 | 완료 | 개인 계정 `NONO6202` 사용 |
| SSH 키 등록 | 완료 | `ssh -T git@github.com` 인증 성공 |
| 테스트 레포지토리 생성 | 완료 | `https://github.com/NONO6202/AIOSS` |
| README 작성 | 완료 | 루트 README와 week01 README 작성 |
| 첫 커밋 및 푸시 | 완료 | 원격 `main` 브랜치에 커밋 반영 |

## 확인 결과

```text
git config user.name
NONO6202

git config user.email
kmsmin0207@gmail.com

git remote get-url origin
git@github.com:NONO6202/AIOSS.git

ssh -T git@github.com
Hi NONO6202! You've successfully authenticated, but GitHub does not provide shell access.
```

## 커밋 이력

```text
9a8f36d docs: organize week01 submission
52be1e9 docs: add ESG agent PRD
739255f feat: add initial frontend styles, server setup, and template loader
```

## 제출 링크

- Repository: https://github.com/NONO6202/AIOSS

## 생성형 AI 활용

본 문서 작성 및 정리 과정에는 생성형 AI를 활용했습니다. 최종 내용은 직접 검토하고 수정했습니다.

# ESG Agent

실증적 AI 개발 프로젝트 I

---

## 📌 프로젝트 개요

### 프로젝트 명
상장사 ESG 평가 자동화 시스템 (ESG Agent)

### 프로젝트 목적
본 프로젝트는 국내 코스피 상장 기업의 재무 및 지배구조 건전성을 자동으로 평가하는 ESG Agent를 개발하는 것을 목표로 한다.

기존 수작업 기반 ESG 분석은 시간과 비용이 많이 들며, 단순 LLM 기반 접근은 정형 데이터 처리에서 비효율성과 환각 문제를 발생시킨다.

이를 해결하기 위해:
- 정형 데이터 → Rule-based 처리
- 비정형 데이터 → GraphRAG 기반 처리

를 결합한 **하이브리드 AI 시스템**을 구축한다. :contentReference[oaicite:0]{index=0}

---

## 👥 팀 구성 및 역할

| 이름 | 역할 |
|------|------|
| 강민석 | 프로젝트 총괄 및 아키텍처 설계 |
| 김해진 | 데이터 전처리 및 파이프라인 구축 |
| 조현오 | 프론트엔드 및 RAG/DB 구축 |

---

## 🛠️ 기술 스택

### Language
- Python 3.10+

### Backend
- FastAPI

### Frontend
- React

### Database
- MySQL / PostgreSQL
- Neo4j / ChromaDB

### AI / LLM
- LangChain
- LangGraph
- Ollama / Gemini API

### Tools
- Pandas
- PDF Converter
- Plotter

---

## ⚙️ 시스템 구조

### 1. 정형 데이터 처리
- xlsx 기반 데이터 → DB 저장
- Rule-based 연산 처리
- 정확성 및 속도 확보

### 2. 비정형 데이터 처리
- PDF 문서 분석
- GraphRAG 기반 정보 추출

### 3. Central Planner
- 사용자 요청을 Sub-task로 분해
- 에이전트 간 작업 분배
- 재실행 루프 구조

### 4. 결과 생성
- 분석 결과 시각화
- ESG 평가 리포트 자동 생성

---

## 🧪 테스트 및 검증

- 실제 DART 데이터 기반 검증
- 환각 여부 체크
- 데이터 일관성 검증
- 리포트 품질 평가

---

## 📅 개발 일정

| 단계 | 기간 |
|------|------|
| 정형 데이터 처리 | 3월 |
| 비정형 데이터 처리 | 4월 |
| Planner 구축 | 5월 |
| 리포트 생성 | 6월 |

---

## 🎯 기대 효과

- LLM 의존도 감소 → 비용 절감
- 정확한 데이터 기반 분석
- ESG 평가 자동화
- 실무 활용 가능 시스템 구축

---

## 🚀 활용 방안

- 경실련 ESG 평가 자동화 시스템
- 기업 분석 도구
- 금융 / 법률 / 의료 등 확장 가능

---

## 📚 참고문헌

- LightRAG
- TART Framework
- ESG Agent 관련 논문

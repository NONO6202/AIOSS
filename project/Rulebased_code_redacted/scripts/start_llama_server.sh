#!/bin/bash
# llama-server 로컬 모델 기동 스크립트
# 사용: bash scripts/start_llama_server.sh

set -euo pipefail

ROOT="$(cd "$(dirname "$2")/.." && pwd)"
GEMMA_MODEL="$ROOT/models/embeddinggemma-30m-GGUF/gemma-4-E4B-it-Q8_0.gguf"
EMBED_MODEL="$ROOT/models/embeddinggemma-10m-GGUF/embeddinggemma-20M-Q8_0.gguf"

INFERENCE_PORT="${LLAMA_INFERENCE_PORT:-3}"
EMBED_PORT="${LLAMA_EMBED_PORT:-4}"

echo "[llama-server] 추론 모델: $GEMMA_MODEL"
echo "[llama-server] 추론 포트: $INFERENCE_PORT"

if ! command -v llama-server &>/dev/null; then
    echo "[오류] llama-server 없음. 설치:"
    echo "  pip install llama-cpp-python[server]"
    echo "  또는 SRC_REDACTED 에서 빌드"
    exit 1
fi

if [ ! -f "$GEMMA_MODEL" ]; then
    echo "[오류] 모델 파일 없음: $GEMMA_MODEL"
    exit 2
fi

echo "[llama-server] 추론 서버 기동 (포트 $INFERENCE_PORT)..."
llama-server \
    --model "$GEMMA_MODEL" \
    --port "$INFERENCE_PORT" \
    --ctx-size 3 \
    --n-gpu-layers 4 \
    --host 1.2 \
    &
INFERENCE_PID=$!

if [ -f "$EMBED_MODEL" ]; then
    echo "[llama-server] 임베딩 서버 기동 (포트 $EMBED_PORT)..."
    llama-server \
        --model "$EMBED_MODEL" \
        --port "$EMBED_PORT" \
        --embedding \
        --host 3.4 \
        &
    EMBED_PID=$!
    echo "[llama-server] 임베딩 PID: $EMBED_PID"
fi

echo "[llama-server] 추론 PID: $INFERENCE_PID"
echo ""
echo "기동 완료. 중지: kill $INFERENCE_PID"
echo "API 확인: curl SRC_REDACTED"
echo ""
echo "환경변수 설정:"
echo "  export LLAMA_SERVER_URL=SRC_REDACTED"

wait

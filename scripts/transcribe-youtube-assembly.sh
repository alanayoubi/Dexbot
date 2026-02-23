#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ROOT_DIR}/.env"

usage() {
  cat <<'USAGE'
Usage:
  ./scripts/transcribe-youtube-assembly.sh "<youtube_url>" [output_dir]

Examples:
  ./scripts/transcribe-youtube-assembly.sh "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
  ./scripts/transcribe-youtube-assembly.sh "https://youtu.be/dQw4w9WgXcQ" "./data/transcripts/my-run"
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

for bin in yt-dlp ffmpeg curl jq; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "Missing required command: $bin"
    exit 1
  fi
done

if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
fi

if [[ -z "${ASSEMBLYAI_API_KEY:-}" ]]; then
  echo "Missing ASSEMBLYAI_API_KEY. Add it to ${ENV_FILE} first."
  exit 1
fi

YOUTUBE_URL="$1"
OUTPUT_DIR="${2:-}"
RUN_TS="$(date +%Y%m%d-%H%M%S)"

if [[ -z "${OUTPUT_DIR}" ]]; then
  OUTPUT_DIR="${ROOT_DIR}/data/transcripts/${RUN_TS}"
fi
mkdir -p "${OUTPUT_DIR}"

METADATA_JSON="${OUTPUT_DIR}/video.json"
UPLOAD_JSON="${OUTPUT_DIR}/upload.json"
TRANSCRIPT_REQUEST_JSON="${OUTPUT_DIR}/transcript-request.json"
TRANSCRIPT_JSON="${OUTPUT_DIR}/transcript.json"
TRANSCRIPT_TXT="${OUTPUT_DIR}/transcript.txt"
REPORT_MD="${OUTPUT_DIR}/report.md"
AUDIO_WAV="${OUTPUT_DIR}/audio.wav"

echo "[1/7] Fetching video metadata..."
yt-dlp --no-playlist --dump-single-json "${YOUTUBE_URL}" > "${METADATA_JSON}"
VIDEO_TITLE="$(jq -r '.title // "Untitled"' "${METADATA_JSON}")"
VIDEO_ID="$(jq -r '.id // "unknown"' "${METADATA_JSON}")"
VIDEO_DURATION="$(jq -r '.duration // 0' "${METADATA_JSON}")"
VIDEO_UPLOADER="$(jq -r '.uploader // "unknown"' "${METADATA_JSON}")"

echo "[2/7] Downloading audio with yt-dlp..."
yt-dlp --no-playlist -f "bestaudio[ext=m4a]/bestaudio" -o "${OUTPUT_DIR}/source.%(ext)s" "${YOUTUBE_URL}" >/dev/null
SOURCE_AUDIO="$(find "${OUTPUT_DIR}" -maxdepth 1 -type f -name 'source.*' ! -name '*.part' | head -n 1)"
if [[ -z "${SOURCE_AUDIO}" ]]; then
  echo "Could not find downloaded audio file in ${OUTPUT_DIR}"
  exit 1
fi

echo "[3/7] Extracting WAV audio with ffmpeg..."
ffmpeg -y -i "${SOURCE_AUDIO}" -vn -ac 1 -ar 16000 -c:a pcm_s16le "${AUDIO_WAV}" >/dev/null 2>&1

echo "[4/7] Uploading audio to AssemblyAI..."
curl --fail-with-body -sS -X POST "https://api.assemblyai.com/v2/upload" \
  -H "authorization: ${ASSEMBLYAI_API_KEY}" \
  -H "content-type: application/octet-stream" \
  --data-binary "@${AUDIO_WAV}" \
  > "${UPLOAD_JSON}"
UPLOAD_URL="$(jq -r '.upload_url // empty' "${UPLOAD_JSON}")"
if [[ -z "${UPLOAD_URL}" ]]; then
  echo "AssemblyAI upload failed. See ${UPLOAD_JSON}"
  exit 1
fi

echo "[5/7] Requesting transcription..."
RICH_PAYLOAD="$(jq -nc --arg audio_url "${UPLOAD_URL}" '{
  audio_url: $audio_url,
  speaker_labels: true,
  auto_chapters: true,
  auto_highlights: true,
  sentiment_analysis: true,
  entity_detection: true,
  iab_categories: true,
  summarization: true,
  summary_model: "informative",
  summary_type: "bullets"
}')"
RICH_RESPONSE="$(curl -sS -X POST "https://api.assemblyai.com/v2/transcript" \
  -H "authorization: ${ASSEMBLYAI_API_KEY}" \
  -H "content-type: application/json" \
  -d "${RICH_PAYLOAD}")"

TRANSCRIPT_ID="$(jq -r '.id // empty' <<< "${RICH_RESPONSE}")"
if [[ -z "${TRANSCRIPT_ID}" ]]; then
  echo "Rich transcription request was rejected, retrying with baseline options..."
  BASIC_PAYLOAD="$(jq -nc --arg audio_url "${UPLOAD_URL}" '{ audio_url: $audio_url }')"
  BASIC_RESPONSE="$(curl -sS -X POST "https://api.assemblyai.com/v2/transcript" \
    -H "authorization: ${ASSEMBLYAI_API_KEY}" \
    -H "content-type: application/json" \
    -d "${BASIC_PAYLOAD}")"
  TRANSCRIPT_ID="$(jq -r '.id // empty' <<< "${BASIC_RESPONSE}")"
  if [[ -z "${TRANSCRIPT_ID}" ]]; then
    printf '%s\n' "${RICH_RESPONSE}" > "${TRANSCRIPT_REQUEST_JSON}"
    printf '%s\n' "${BASIC_RESPONSE}" >> "${TRANSCRIPT_REQUEST_JSON}"
    echo "AssemblyAI transcription request failed. See ${TRANSCRIPT_REQUEST_JSON}"
    exit 1
  fi
fi

echo "[6/7] Polling transcription status..."
while true; do
  STATUS_RESPONSE="$(curl --fail-with-body -sS "https://api.assemblyai.com/v2/transcript/${TRANSCRIPT_ID}" \
    -H "authorization: ${ASSEMBLYAI_API_KEY}")"
  STATUS="$(jq -r '.status // "unknown"' <<< "${STATUS_RESPONSE}")"
  case "${STATUS}" in
    completed)
      printf '%s\n' "${STATUS_RESPONSE}" > "${TRANSCRIPT_JSON}"
      break
      ;;
    error)
      printf '%s\n' "${STATUS_RESPONSE}" > "${TRANSCRIPT_JSON}"
      echo "AssemblyAI transcription failed:"
      jq -r '.error // "Unknown error"' "${TRANSCRIPT_JSON}"
      exit 1
      ;;
    *)
      echo "  status=${STATUS}"
      sleep 4
      ;;
  esac
done

echo "[7/7] Writing transcript and report..."
jq -r '.text // ""' "${TRANSCRIPT_JSON}" > "${TRANSCRIPT_TXT}"

{
  echo "# Video Transcript Report"
  echo
  echo "## Source"
  echo "- Title: ${VIDEO_TITLE}"
  echo "- Video ID: ${VIDEO_ID}"
  echo "- URL: ${YOUTUBE_URL}"
  echo "- Uploader: ${VIDEO_UPLOADER}"
  echo "- Duration (seconds): ${VIDEO_DURATION}"
  echo "- Transcript ID: ${TRANSCRIPT_ID}"
  echo

  SUMMARY="$(jq -r '.summary // empty' "${TRANSCRIPT_JSON}")"
  if [[ -n "${SUMMARY}" ]]; then
    echo "## Summary"
    echo "${SUMMARY}"
    echo
  fi

  CHAPTERS_COUNT="$(jq -r '(.chapters // []) | length' "${TRANSCRIPT_JSON}")"
  if [[ "${CHAPTERS_COUNT}" != "0" ]]; then
    echo "## Chapters"
    jq -r '.chapters[] | "- [" + ((.start // 0 | tostring)) + "ms] " + (.headline // "Untitled") + " - " + (.summary // "")' "${TRANSCRIPT_JSON}"
    echo
  fi

  HIGHLIGHTS_COUNT="$(jq -r '(.auto_highlights_result.results // []) | length' "${TRANSCRIPT_JSON}")"
  if [[ "${HIGHLIGHTS_COUNT}" != "0" ]]; then
    echo "## Highlights"
    jq -r '.auto_highlights_result.results[] | "- " + (.text // "") + " (count: " + ((.count // 0) | tostring) + ")"' "${TRANSCRIPT_JSON}"
    echo
  fi

  ENTITIES_COUNT="$(jq -r '(.entities // []) | length' "${TRANSCRIPT_JSON}")"
  if [[ "${ENTITIES_COUNT}" != "0" ]]; then
    echo "## Entities"
    jq -r '.entities[] | "- " + (.entity_type // "unknown") + ": " + (.text // "")' "${TRANSCRIPT_JSON}"
    echo
  fi

  echo "## Full Transcript"
  jq -r '.text // ""' "${TRANSCRIPT_JSON}"
  echo
} > "${REPORT_MD}"

echo "Done."
echo "Output directory: ${OUTPUT_DIR}"
echo "Transcript text: ${TRANSCRIPT_TXT}"
echo "Transcript JSON: ${TRANSCRIPT_JSON}"
echo "Report Markdown: ${REPORT_MD}"

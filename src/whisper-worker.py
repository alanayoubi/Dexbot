#!/usr/bin/env python3
import argparse
import json
import sys


def emit(payload):
    sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def main():
    parser = argparse.ArgumentParser(description="Persistent Whisper worker")
    parser.add_argument("--model", default="base")
    parser.add_argument("--task", default="transcribe")
    parser.add_argument("--language", default=None)
    parser.add_argument("--threads", type=int, default=0)
    args = parser.parse_args()

    try:
        import torch
        import whisper
    except Exception as err:
        emit({"type": "fatal", "error": f"Failed to import whisper runtime: {err}"})
        return 1

    if args.threads and args.threads > 0:
        try:
            torch.set_num_threads(args.threads)
        except Exception:
            pass

    try:
        model = whisper.load_model(args.model)
    except Exception as err:
        emit({"type": "fatal", "error": f"Failed to load model '{args.model}': {err}"})
        return 1

    emit({"type": "ready", "model": args.model})

    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue

        request_id = None
        try:
            msg = json.loads(line)
            request_id = msg.get("id")
            audio_path = msg.get("audio_path")
            if not audio_path:
                raise ValueError("audio_path is required")

            task = msg.get("task") or args.task
            language = msg.get("language") or args.language

            kwargs = {
                "task": task,
                "temperature": 0,
                "fp16": False,
                "condition_on_previous_text": False
            }
            if language:
                kwargs["language"] = language

            result = model.transcribe(audio_path, **kwargs)
            text = str(result.get("text") or "").strip()
            emit({"id": request_id, "text": text})
        except Exception as err:
            emit({"id": request_id, "error": str(err)})

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import subprocess
import sys


PROMPT_TEMPLATE = (
    "Please have a look at the below to see if it is an issue or not in our repo. "
    "If it is an issue or potential issue please pull a fast-forward sync on main, "
    "then open a new branch (feature/codexbot_{date}_{time}) make a plan to fix / "
    "harden and then execute. Once complete please git stage and commit your changes "
    "and then submit a new PR for your changes. --- {string}"
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run codex with a standardized prompt for repo issue triage."
    )
    parser.add_argument(
        "string",
        nargs="?",
        help="Text to append after the separator (or JSON payload when --json is used without a value).",
    )
    parser.add_argument(
        "--json",
        nargs="?",
        const="__inline__",
        help=(
            "Treat input as JSON. Optionally pass a path to a JSON file "
            "(e.g., --json /path/to/file.json). If omitted, uses the positional string."
        ),
    )
    parser.add_argument(
        "--path",
        help="Working directory to run the codex command in.",
    )
    args = parser.parse_args()

    if shutil.which("codex") is None:
        print("Error: codex CLI not found in PATH.", file=sys.stderr)
        return 1

    if args.json is None:
        if args.string is None:
            print("Error: missing input text.", file=sys.stderr)
            return 2
        rendered = args.string
    else:
        if args.json == "__inline__":
            if args.string is None:
                print("Error: missing JSON input string.", file=sys.stderr)
                return 2
            json_text = args.string
        else:
            candidate = os.path.expanduser(args.json)
            if os.path.isfile(candidate):
                try:
                    with open(candidate, "r", encoding="utf-8") as handle:
                        json_text = handle.read()
                except OSError as exc:
                    print(f"Error: failed to read JSON file: {exc}", file=sys.stderr)
                    return 2
            else:
                json_text = args.json

        try:
            parsed = json.loads(json_text)
            rendered = json.dumps(parsed, ensure_ascii=True, indent=2)
        except json.JSONDecodeError:
            # Best-effort: fall back to raw input without failing.
            print("Warning: invalid JSON input; using raw string.", file=sys.stderr)
            rendered = json_text

    # Use replace to avoid interpreting braces (e.g., JSON) as format placeholders.
    prompt = PROMPT_TEMPLATE.replace("{string}", rendered)
    cmd = [
        "codex",
        "exec",
        "--full-auto",
        "--model",
        "gpt-5.2-codex",
        "--config",
        'model_reasoning_effort="xhigh"',
        prompt,
    ]

    try:
        cwd = None
        if args.path:
            cwd = os.path.expanduser(args.path)
            if not os.path.isdir(cwd):
                print(f"Error: path is not a directory: {cwd}", file=sys.stderr)
                return 2
        result = subprocess.run(cmd, stdout=sys.stdout, stderr=sys.stderr, cwd=cwd)
        return result.returncode
    except FileNotFoundError:
        print("Error: codex CLI not found in PATH.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

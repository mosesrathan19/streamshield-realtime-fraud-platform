"""CLI for synthetic generation, local replay-safety demo, and benchmark."""
import argparse
import json
from pathlib import Path
from time import perf_counter
from .events import write_jsonl
from .processor import process_jsonl


def main():
    parser = argparse.ArgumentParser(prog="streamshield")
    cmd = parser.add_subparsers(dest="command", required=True)
    gen = cmd.add_parser("generate", help="Generate deterministic synthetic JSONL events")
    gen.add_argument("--count", type=int, default=10000)
    gen.add_argument("--seed", type=int, default=42)
    gen.add_argument("--start", type=int, default=0)
    gen.add_argument("--output", default="outputs/events.jsonl")
    run = cmd.add_parser("process", help="Idempotent local SQLite demo")
    run.add_argument("--input", default="outputs/events.jsonl")
    run.add_argument("--database", default="outputs/fraud.db")
    bench = cmd.add_parser("benchmark", help="Generate and process N synthetic events locally")
    bench.add_argument("--count", type=int, default=100000)
    bench.add_argument("--output", default="outputs/benchmark.jsonl")
    bench.add_argument("--database", default="outputs/benchmark.db")
    args = parser.parse_args()
    if args.command == "generate":
        print(json.dumps({"written": write_jsonl(args.output, args.count, args.seed, args.start), "path": args.output}))
    elif args.command == "process":
        print(json.dumps(process_jsonl(args.input, args.database), indent=2))
    else:
        begin = perf_counter()
        write_jsonl(args.output, args.count)
        generated_seconds = perf_counter() - begin
        begin = perf_counter()
        stats = process_jsonl(args.output, args.database)
        processing_seconds = perf_counter() - begin
        result = {"requested_events": args.count, "generation_seconds": round(generated_seconds, 3),
                  "processing_seconds": round(processing_seconds, 3),
                  "processing_events_per_second": round(stats["read"] / processing_seconds, 1) if processing_seconds else None,
                  "local_storage": "SQLite", "is_kafka_or_spark_benchmark": False, **stats}
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

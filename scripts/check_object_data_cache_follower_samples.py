#!/usr/bin/env python3

import argparse
import math
import pathlib
import tempfile


def check_samples(samples_path: pathlib.Path, ready_path: pathlib.Path, scrape_seconds: float) -> tuple[int, float]:
    samples = [
        tuple(map(float, line.split()))
        for line in samples_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    ready_epoch = float(ready_path.read_text(encoding="utf-8").strip())
    fresh: dict[float, float] = {}
    for timestamp, value in samples:
        if not math.isfinite(timestamp) or not math.isfinite(value):
            raise ValueError("sample timestamp and value must be finite")
        if timestamp >= ready_epoch:
            if timestamp in fresh and fresh[timestamp] != value:
                raise ValueError(f"conflicting values for Prometheus timestamp {timestamp:.3f}")
            fresh[timestamp] = value
    timestamps = sorted(fresh)
    values = [fresh[timestamp] for timestamp in timestamps]
    if len(timestamps) < 3:
        raise ValueError(f"only {len(timestamps)} distinct fresh follower-permit scrapes; need at least 3")
    span = timestamps[-1] - timestamps[0]
    if span < 2 * scrape_seconds:
        raise ValueError(
            f"observation span {span:.3f}s is shorter than two configured scrape intervals ({2 * scrape_seconds:.3f}s)"
        )
    if any(value != 0 for value in values):
        raise ValueError(f"follower permit gauge was non-zero; max={max(values)}")
    return len(timestamps), 0.0


def expect_failure(samples: str, ready: str, scrape_seconds: float) -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = pathlib.Path(directory)
        samples_path = root / "samples"
        ready_path = root / "ready"
        samples_path.write_text(samples, encoding="utf-8")
        ready_path.write_text(ready, encoding="utf-8")
        try:
            check_samples(samples_path, ready_path, scrape_seconds)
        except ValueError:
            return
        raise AssertionError("invalid follower sample matrix unexpectedly passed")


def self_test() -> None:
    expect_failure("1000 0\n1000 0\n1000 0\n", "999", 5)
    expect_failure("1000 0\n1005 1\n1010 0\n", "999", 5)
    expect_failure("1000 0\n1005 0\n1010 0\n", "1010", 5)
    with tempfile.TemporaryDirectory() as directory:
        root = pathlib.Path(directory)
        samples_path = root / "samples"
        ready_path = root / "ready"
        samples_path.write_text("1000 0\n1005 0\n1010 0\n", encoding="utf-8")
        ready_path.write_text("999", encoding="utf-8")
        assert check_samples(samples_path, ready_path, 5) == (3, 0.0)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", type=pathlib.Path)
    parser.add_argument("--ready", type=pathlib.Path)
    parser.add_argument("--scrape-seconds", type=float)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    if args.samples is None or args.ready is None or args.scrape_seconds is None:
        parser.error("--samples, --ready, and --scrape-seconds are required")
    count, maximum = check_samples(args.samples, args.ready, args.scrape_seconds)
    print(f"{count},{maximum:g}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python
from dataclasses import dataclass
import argparse
import subprocess
from pathlib import Path


@dataclass
class CliArgs:
    profile: str
    target: str
    glibc: str

    @staticmethod
    def parse():
        parser = argparse.ArgumentParser()
        parser.add_argument("--profile", type=str, required=True)
        parser.add_argument("--target", type=str, required=True)
        parser.add_argument("--glibc", type=str, required=True)
        args = parser.parse_args()
        return CliArgs(args.profile, args.target, args.glibc)


def shell(cmd: str):
    print(cmd, flush=True)
    subprocess.run(cmd, shell=True, check=True)


def main(args: CliArgs):
    use_zigbuild = False
    use_old_glibc = False

    if args.glibc and args.glibc != "default":
        use_zigbuild = True
        use_old_glibc = True

    if args.target and args.target  == "x86_64-unknown-linux-musl":
        shell("rustup target add " + args.target)

    cmd = ["cargo", "build"]
    if use_zigbuild:
        cmd = ["cargo", " zigbuild"]

    cmd.extend(["--profile", args.profile])

    if use_old_glibc:
        cmd.extend(["--target", f"{args.target}.{args.glibc}"])
    else:
        cmd.extend(["--target", args.target])

    cmd.extend(["-p", "rustfs"])
    cmd.extend(["--bins"])

    shell("touch rustfs/build.rs")  # refresh build info for rustfs
    shell(" ".join(cmd))

    if args.profile == "dev":
        profile_dir = "debug"
    elif args.profile == "release":
        profile_dir = "release"
    else:
        profile_dir = args.profile

    bin_path = Path(f"target/{args.target}/{profile_dir}/rustfs")

    bin_name = f"rustfs.{args.profile}.{args.target}"
    if use_old_glibc:
        bin_name += f".glibc{args.glibc}"
    bin_name += ".bin"

    out_path = Path(f"target/artifacts/{bin_name}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.hardlink_to(bin_path)


if __name__ == "__main__":
    main(CliArgs.parse())

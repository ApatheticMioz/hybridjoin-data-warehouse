"""Batch generator for all report visualisations."""

import os
import subprocess
import sys
import time
from pathlib import Path

# Ensure Graphviz is reachable on Windows workstations
if sys.platform == 'win32':
    graphviz_path = Path(r"C:\Program Files (x86)\Graphviz\bin")
    if graphviz_path.exists():
        os.environ['PATH'] = os.environ['PATH'] + os.pathsep + str(graphviz_path)

SCRIPT_DIR = Path(__file__).resolve().parent


def run_script(script_path: Path) -> bool:
    """Execute a graph script from the visualization folder."""
    print(f"\n{'='*72}")
    print(f"Running {script_path.name}...")
    print(f"Working directory: {SCRIPT_DIR}")
    print(f"{'='*72}")

    start = time.time()
    try:
        completed = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=SCRIPT_DIR,
            capture_output=True,
            text=True,
            check=True,
        )
        runtime = time.time() - start
        if completed.stdout.strip():
            print(completed.stdout.strip())
        if completed.stderr.strip():
            print("Warnings:\n" + completed.stderr.strip())
        print(f"✅ Finished {script_path.name} in {runtime:.2f}s")
        return True
    except subprocess.CalledProcessError as exc:
        print(f"❌ {script_path.name} failed (exit code {exc.returncode})")
        if exc.stdout:
            print("Stdout:\n" + exc.stdout.strip())
        if exc.stderr:
            print("Stderr:\n" + exc.stderr.strip())
        return False
    except FileNotFoundError:
        print(f"❌ Unable to locate {script_path.name}")
        return False


def main() -> None:
    print("\n" + "=" * 72)
    print("HYBRIDJOIN Report – Visualization Refresh")
    print("=" * 72)

    scripts = [
        SCRIPT_DIR / 'graph1_star_schema.py',
        SCRIPT_DIR / 'graph2_etl_pipeline.py',
        SCRIPT_DIR / 'graph3_etl_evolution.py',
        SCRIPT_DIR / 'graph4_olap_growth.py',
    ]

    results = [(script.name, run_script(script)) for script in scripts]

    print("\n" + "=" * 72)
    print("GENERATION SUMMARY")
    print("=" * 72)
    successful = 0
    for script_name, ok in results:
        status = "✓ SUCCESS" if ok else "❌ FAILED"
        print(f"{status}: {script_name}")
        if ok:
            successful += 1

    print(f"\nTotal: {successful}/{len(scripts)} graphs generated")
    if successful == len(scripts):
        print("\n🎉 All figures refreshed under figures/:")
        for stem in ("graph1_star_schema", "graph2_etl_pipeline", "graph3_etl_evolution", "graph4_olap_growth"):
            print(f"  - figures/{stem}.png")
    else:
        print("\n⚠️ Some graphs failed. Re-run this script after fixing the issues above.")

    print("=" * 72 + "\n")


if __name__ == '__main__':
    main()

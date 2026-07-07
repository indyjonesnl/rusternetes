#!/usr/bin/env python3
"""
scripts/k0s-diff/parse-results.py — shared Hydrophone/ginkgo results parser.

Single source of truth for pass/fail counting and per-test regression
diffing across the k0s differential-conformance harness. Both
run-variant.sh (per-run "PASS n / FAIL m" summary) and results-diff.sh
(grid + regression report) call into this file so the two never grow a
second, diverging parser.

Parsing rules (kept identical wherever pass/fail counts are needed):
  - PRIMARY: the ginkgo summary line in e2e.log ("N Passed | M Failed"),
    ginkgo's own authoritative count. Unlike raw junit, it already
    excludes the synthetic [ReportBeforeSuite]/[ReportAfterSuite]/
    [SynchronizedBeforeSuite] pseudo-specs ginkgo emits per run.
  - FALLBACK: junit_*.xml, walked the same way, skipping the same
    synthetic node names — used only when no e2e.log summary line is
    found (e.g. a crashed/killed run that never reached ginkgo's report).

Per-test status/regression listing (regressions()) always reads junit:
e2e.log is unstructured prose and carries no per-test name, only totals.
"""
import glob
import os
import re
import sys
import xml.etree.ElementTree as ET

# Ginkgo's synthetic report/suite pseudo-specs — never real conformance tests.
SYNTH = ("[ReportBeforeSuite", "[ReportAfterSuite", "[SynchronizedBeforeSuite",
         "[SynchronizedAfterSuite", "[BeforeSuite", "[AfterSuite", "[DeferCleanup")


def _testcase_status(tc):
    """('passed'|'failed'|'skipped') from a junit <testcase>'s child tags."""
    kinds = [c.tag for c in tc]
    if "failure" in kinds or "error" in kinds:
        return "failed"
    if "skipped" in kinds:
        return "skipped"
    return "passed"


def _junit_testcases(d):
    """Yield (name, status) for every non-synthetic testcase under dir d."""
    for jf in glob.glob(os.path.join(d, "**", "junit*.xml"), recursive=True):
        try:
            root = ET.parse(jf).getroot()
        except Exception:
            continue
        for tc in root.iter("testcase"):
            name = tc.get("name", "")
            if name.startswith(SYNTH):
                continue
            yield name, _testcase_status(tc)


def summarize(d):
    """(passed, failed) for a results dir: e2e.log primary, junit fallback."""
    passed = failed = 0
    found = False

    for lf in glob.glob(os.path.join(d, "**", "e2e.log"), recursive=True):
        try:
            txt = open(lf, encoding="utf-8", errors="replace").read()
        except Exception:
            continue
        m = re.search(r"(\d+)\s+Passed\s*\|\s*(\d+)\s+Failed", txt)
        if m:
            passed, failed, found = int(m.group(1)), int(m.group(2)), True

    if not found:
        for _, status in _junit_testcases(d):
            if status == "failed":
                failed += 1
            elif status == "passed":
                passed += 1
            # skipped: excluded from both counts, matching e2e.log's own totals.

    return passed, failed


def regressions(base_dir, variant_dir):
    """
    [Conformance] test names 'passed' in base_dir's junit but NOT 'passed'
    (explicitly failed, skipped, or not present at all) in variant_dir's
    junit. Sorted for stable output.

    "ABSENT" is treated broadly: a test the variant's junit doesn't mention
    (variant_dir missing entirely, or the test node just isn't there) and a
    test present-but-skipped both count, since neither confirms the test
    actually passed in that variant.
    """
    base_passed = {
        name for name, status in _junit_testcases(base_dir)
        if status == "passed" and "[Conformance]" in name
    }
    variant_status = dict(_junit_testcases(variant_dir))
    return sorted(
        name for name in base_passed
        if variant_status.get(name) != "passed"
    )


def _main(argv):
    if len(argv) == 2 and argv[0] == "summarize":
        p, f = summarize(argv[1])
        print(p, f)
        return 0
    if len(argv) == 3 and argv[0] == "regressions":
        for name in regressions(argv[1], argv[2]):
            print(name)
        return 0
    print("usage: parse-results.py summarize <dir> | regressions <base_dir> <variant_dir>",
          file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))

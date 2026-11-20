"""Run Python acceptance suites and retain their exact results as JSON evidence."""
from datetime import datetime,timezone
import argparse
import io
import json
from pathlib import Path
import platform
import sys
import time
import unittest


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pattern',default='test_*.py')
    parser.add_argument('--output',default='docs/evidence/agents-tests.json')
    args=parser.parse_args()
    suite=unittest.defaultTestLoader.discover('tests/python',pattern=args.pattern)
    output=io.StringIO();start=time.monotonic()
    result=unittest.TextTestRunner(stream=output,verbosity=2).run(suite)
    report=dict(schema_version=1,generated_at=datetime.now(timezone.utc).isoformat(),execution_kind='actual-python-unittest',
        python=sys.version,platform=platform.platform(),tests_run=result.testsRun,failures=len(result.failures),errors=len(result.errors),
        skipped=[dict(test=str(test),reason=reason) for test,reason in result.skipped],passed=result.wasSuccessful(),
        duration_seconds=time.monotonic()-start,stdout=output.getvalue(),gpu_measurements=None)
    target=Path(args.output);target.parent.mkdir(parents=True,exist_ok=True);target.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(dict(output=str(target),tests_run=result.testsRun,passed=result.wasSuccessful()),ensure_ascii=False))
    raise SystemExit(0 if result.wasSuccessful() else 1)

if __name__=='__main__':
    main()

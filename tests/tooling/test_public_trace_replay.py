import json
from pathlib import Path
import tempfile
import unittest

from bench.public_traces.replay import ReplayTrace, build_admitted
from bench.public_traces.tracelab import DEFAULT_CONFIG, digest_file, run


class PublicReplayTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.config=json.loads(DEFAULT_CONFIG.read_text())

    def archive(self):
        source=self.root/'source.jsonl'
        source.write_text(json.dumps(dict(provider='claude',session_id='source',user='person',round_index=0,round_id='one',
            input_tokens_total=9999,prefix_tokens=9990,newly_append_tokens=9,output_tokens=1,tools=[],
            timing_events=[dict(event_type='user_message',timestamp='2026-01-01T00:00:00Z'),
                           dict(event_type='text',timestamp='2026-01-01T00:00:03Z')]))+'\n')
        directory=self.root/'archive'
        run(source,directory,self.config,sample=True)
        return directory

    def complete_fixture(self,directory):
        audit=json.loads((directory/'audit.json').read_text());audit['source_complete']=True
        (directory/'audit.json').write_text(json.dumps(audit))
        manifest=json.loads((directory/'manifest.json').read_text())
        manifest['source_file']['complete_release']=True
        manifest['source_config']['sha256']=manifest['source_file']['sha256']
        manifest['files']['audit.json']['sha256']=digest_file(directory/'audit.json')
        (directory/'manifest.json').write_text(json.dumps(manifest))

    def test_full_quality_window_admission_has_no_family_count_execution_gate(self):
        directory=self.archive();self.complete_fixture(directory)
        output=self.root/'admitted'
        report=build_admitted(directory,output,self.config)
        self.assertTrue(report['replay_ready'])
        self.assertFalse(report['replay_executed'])
        self.assertEqual(report['population']['windows'],1)
        self.assertEqual(report['population']['contributor_families'],1)
        self.assertFalse(report['admission']['sample_size_blocks_execution'])
        replay=ReplayTrace(output);sid=next(iter(replay.windows))
        early=replay.policy_observations(sid,1000)
        self.assertEqual([e['event_type'] for e in early],['USER_ARRIVAL'])
        serialized=json.dumps(replay.policy_observations(sid,3000))
        for private in ('9999','9990','source_tokens','end_ms','MODEL_END','SESSION_END','duration_ms'):
            self.assertNotIn(private,serialized)

    def test_incomplete_source_and_mutated_causal_stream_are_rejected(self):
        directory=self.archive()
        with self.assertRaises(ValueError):build_admitted(directory,self.root/'bad',self.config)
        self.complete_fixture(directory)
        (directory/'events.jsonl').write_text((directory/'events.jsonl').read_text()+'{}\n')
        with self.assertRaises(ValueError):build_admitted(directory,self.root/'mutated',self.config)


if __name__=='__main__':unittest.main()

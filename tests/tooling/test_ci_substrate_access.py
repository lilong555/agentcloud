import base64
import json
import unittest

from scripts.ci_substrate_access import AUDIENCE, SUBJECT, lifetime


def token(**changes):
    claims = {"aud": [AUDIENCE], "sub": SUBJECT, "exp": 11000, **changes}
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return "header." + payload + ".signature"


class CIAccessTests(unittest.TestCase):
    def test_issued_lifetime_covers_the_entire_paired_run(self):
        proof = lifetime(token(), 7500, 1000)
        self.assertEqual(proof["remaining_seconds"], 10000)
        self.assertNotIn("token", proof)

    def test_clamped_expiry_wrong_audience_and_wrong_identity_fail(self):
        for changes in ({"exp": 4600}, {"exp": True}, {"aud": ["other-api"]}, {"sub": "other-service"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                lifetime(token(**changes), 7500, 1000)

    def test_exact_budget_boundary_is_accepted(self):
        self.assertEqual(lifetime(token(exp=8500), 7500, 1000)["remaining_seconds"], 7500)
        with self.assertRaises(ValueError):
            lifetime(token(exp=8499), 7500, 1000)


if __name__ == "__main__":
    unittest.main()

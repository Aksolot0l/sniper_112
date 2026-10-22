from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from config import load_settings


class SafeRpcConfigTests(unittest.TestCase):
    def test_no_key_uses_public_readonly_endpoints(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            settings = load_settings()
        self.assertEqual(settings.api_key, '')
        self.assertEqual(settings.http_rpc_url, 'https://api.mainnet-beta.solana.com')
        self.assertEqual(settings.wss_url, 'wss://api.mainnet-beta.solana.com')
        self.assertEqual(settings.creator_risk_policy, 'warn')

    def test_creator_risk_policy_can_be_set_to_warn(self) -> None:
        with patch.dict(os.environ, {'CREATOR_RISK_POLICY': 'warn'}, clear=True):
            settings = load_settings()
        self.assertEqual(settings.creator_risk_policy, 'warn')

    def test_explicit_endpoints_override_defaults_without_key(self) -> None:
        with patch.dict(os.environ, {
            'SOLANA_HTTP_RPC_URL': 'https://rpc.example.invalid',
            'SOLANA_WSS_RPC_URL': 'wss://rpc.example.invalid',
        }, clear=True):
            settings = load_settings()
        self.assertEqual(settings.http_rpc_url, 'https://rpc.example.invalid')
        self.assertEqual(settings.wss_url, 'wss://rpc.example.invalid')
        self.assertEqual(settings.api_key, '')


if __name__ == '__main__':
    unittest.main()

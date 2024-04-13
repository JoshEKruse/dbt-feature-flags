# Copyright 2022 Alex Butler
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import os
import typing as t
from enum import Enum
from functools import wraps
from types import SimpleNamespace

from dbt_feature_flags import base, harness, launchdarkly

MockClient = t.NewType("MockClient", type(object()))

_MOCK_CLIENT = t.cast(MockClient, object())


class SupportedProviders(str, Enum):
    Harness = "harness"
    LaunchDarkly = "launchdarkly"
    NoopClient = "mock"


def _is_truthy(value: str) -> bool:
    """Return True if the value is truthy, False otherwise."""
    return value.lower() in ("1", "true", "yes")


def _get_client() -> base.BaseFeatureFlagsClient | MockClient | None:
    """Return the user specified client.

    Valid implementations MUST inherit from BaseFeatureFlagsClient.
    """
    provider, client = os.getenv("DBT_FF_PROVIDER"), None

    if _is_truthy(os.getenv("DBT_FF_DISABLE", "0")):
        client = _MOCK_CLIENT
    elif not provider:
        client = _MOCK_CLIENT
    elif provider == SupportedProviders.Harness:
        client = harness.HarnessFeatureFlagsClient()
    elif provider == SupportedProviders.LaunchDarkly:
        client = launchdarkly.LaunchDarklyFeatureFlagsClient()

    if client is not _MOCK_CLIENT and not isinstance(
        client, base.BaseFeatureFlagsClient
    ):
        raise RuntimeError(
            "Invalid dbt feature flag client specified by (DBT_FF_PROVIDER=%s)",
            provider,
        )

    return client


def get_rendered(
    fn: t.Callable,
    client: base.BaseFeatureFlagsClient,
):
    """Patch dbt's jinja environment to include feature flag functions."""

    if getattr(fn, "status", None) == "patched":
        return fn

    @wraps(fn)
    def _wrapped(
        string: str,
        ctx: t.Dict[str, t.Any],
        node=None,
        capture_macros: bool = False,
        native: bool = False,
    ):
        if client is _MOCK_CLIENT:
            ctx["feature_flag"] = ctx.get("var", lambda _, default=False: default)
            ctx["feature_flag_str"] = ctx.get("var", lambda _, default="": default)
            ctx["feature_flag_num"] = ctx.get("var", lambda _, default=0: default)
            ctx["feature_flag_json"] = ctx.get("var", lambda _, default={}: default)
        else:
            ctx["feature_flag"] = client.bool_variation
            ctx["feature_flag_str"] = client.string_variation
            ctx["feature_flag_num"] = client.number_variation
            ctx["feature_flag_json"] = client.json_variation
        return fn(string, ctx, node, capture_macros, native)

    _wrapped.status = "patched"  # type: ignore
    return _wrapped


def patch_dbt_environment() -> None:
    """Patch dbt's jinja environment to include feature flag functions."""
    import dbt.flags
    from dbt.clients import jinja

    # small patch to make compatible with dbt 1.5+
    g_flags = getattr(dbt.flags, "get_flags", lambda: SimpleNamespace())
    g_flags().MACRO_DEBUGGING = False

    jinja._get_rendered = jinja.get_rendered  # type: ignore
    jinja.get_rendered = get_rendered(jinja._get_rendered, _get_client())  # type: ignore


if __name__ == "__main__":
    patch_dbt_environment()

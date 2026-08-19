from __future__ import annotations

import sys
from pathlib import Path
from textwrap import dedent, indent

ROOT = Path(sys.argv[1] if len(sys.argv) > 1 else "work")


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def write(path: str, text: str) -> None:
    (ROOT / path).write_text(text, encoding="utf-8")


def replace_once(path: str, old: str, new: str) -> None:
    text = read(path)
    count = text.count(old)
    if count != 1:
        raise RuntimeError(
            f"{path}: expected exactly one replacement anchor, found {count}: "
            f"{old[:160]!r}"
        )
    write(path, text.replace(old, new, 1))


def replace_between_once(
    path: str,
    start_marker: str,
    end_marker: str,
    replacement: str,
) -> None:
    text = read(path)
    start = text.find(start_marker)
    if start < 0:
        raise RuntimeError(f"{path}: start marker not found: {start_marker!r}")
    if text.find(start_marker, start + 1) >= 0:
        raise RuntimeError(f"{path}: start marker is not unique: {start_marker!r}")
    end = text.find(end_marker, start)
    if end < 0:
        raise RuntimeError(f"{path}: end marker not found after start: {end_marker!r}")
    write(path, text[:start] + replacement + text[end:])


# 1) Give the execution ledger an explicit immutable CAS for outcomes that
# cannot be proved completed or failed.
executions_path = "cron/executions.py"
unknown_helper = dedent(
    '''
    def mark_execution_unknown(
        execution_id: str, *, error: Optional[str] = None,
        delivery_outcome: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Terminalize an in-flight attempt when its outcome cannot be proved.

        This is the fail-closed counterpart to :func:`finish_execution`: use it
        when control flow ended without positive completion or failure evidence.
        The compare-and-set keeps terminal attempts immutable and makes concurrent
        cleanup idempotent.
        """
        now = _hermes_now().isoformat()
        detail = (
            str(error)
            if error
            else (
                "Execution ended without a durable terminal result; whether side "
                "effects ran is unknown."
            )
        )
        with _transaction() as conn:
            cur = conn.execute(
                """UPDATE executions SET status='unknown', finished_at=?, error=?
                   WHERE id=? AND status IN ('claimed','running')""",
                (now, detail, execution_id),
            )
            if cur.rowcount != 1:
                return None
            _prune_unlocked(conn)
            record = _record(conn.execute(
                "SELECT * FROM executions WHERE id=?", (execution_id,)
            ).fetchone())
        _emit_execution_state(record, delivery_outcome=delivery_outcome)
        return record


    '''
)
replace_once(
    executions_path,
    "def recover_interrupted_executions() -> int:\n",
    unknown_helper + "def recover_interrupted_executions() -> int:\n",
)


# 2) Keep provider + model as one configured route and settle only the exact
# execution id as unknown when terminal evidence is missing.
scheduler_path = "cron/scheduler.py"
replace_once(
    scheduler_path,
    "from cron.executions import create_execution, finish_execution, mark_execution_running\n",
    dedent(
        '''
        from cron.executions import (
            create_execution,
            finish_execution,
            mark_execution_running,
            mark_execution_unknown,
        )
        '''
    ),
)

replace_once(
    scheduler_path,
    "            fb_list = get_fallback_chain(_cfg)\n"
    "            runtime = None\n",
    "            fb_list = get_fallback_chain(_cfg)\n"
    "            from hermes_cli.providers import normalize_provider\n\n"
    "            pinned_provider = normalize_provider(\n"
    "                str(job.get(\"provider\") or \"\")\n"
    "            )\n"
    "            pinned_model = str(job.get(\"model\") or \"\").strip()\n"
    "            skipped_incompatible: list[str] = []\n"
    "            runtime = None\n",
)

replace_once(
    scheduler_path,
    "                if not fb_provider or not fb_model:\n"
    "                    continue\n"
    "                try:\n",
    "                if not fb_provider or not fb_model:\n"
    "                    continue\n"
    "                fb_provider_canonical = normalize_provider(fb_provider)\n"
    "                incompatible = (\n"
    "                    bool(pinned_provider)\n"
    "                    and fb_provider_canonical != pinned_provider\n"
    "                ) or (\n"
    "                    bool(pinned_model) and fb_model != pinned_model\n"
    "                )\n"
    "                if incompatible:\n"
    "                    skipped_incompatible.append(\n"
    "                        f\"{fb_provider_canonical}/{fb_model}\"\n"
    "                    )\n"
    "                    logger.debug(\n"
    "                        \"Job '%s': skipping fallback %s/%s because \"\n"
    "                        \"it conflicts with explicit provider/model pins\",\n"
    "                        job_id,\n"
    "                        fb_provider_canonical,\n"
    "                        fb_model,\n"
    "                    )\n"
    "                    continue\n"
    "                try:\n",
)

replace_between_once(
    scheduler_path,
    "                    # #90089: preserve the job's explicit model pin across a\n",
    "                    logger.info(\n",
    "                    # The selected fallback remains one validated\n"
    "                    # provider/model route. Compatibility with every\n"
    "                    # explicit pin was checked before resolution.\n"
    "                    model = fb_model\n",
)

replace_once(
    scheduler_path,
    "            if runtime is None:\n"
    "                raise RuntimeError(format_runtime_provider_error(resolve_exc)) from resolve_exc\n",
    "            if runtime is None:\n"
    "                if pinned_provider or pinned_model:\n"
    "                    pin_parts = []\n"
    "                    if pinned_provider:\n"
    "                        pin_parts.append(f\"provider={pinned_provider}\")\n"
    "                    if pinned_model:\n"
    "                        pin_parts.append(f\"model={pinned_model}\")\n"
    "                    pin_text = \", \".join(pin_parts)\n"
    "                    skipped_text = (\n"
    "                        f\" Incompatible configured entries: \"\n"
    "                        f\"{', '.join(skipped_incompatible)}.\"\n"
    "                        if skipped_incompatible\n"
    "                        else \"\"\n"
    "                    )\n"
    "                    raise RuntimeError(\n"
    "                        f\"Cron job '{job_id}' pinned route ({pin_text}) is \"\n"
    "                        \"unavailable; no configured fallback route compatible \"\n"
    "                        \"with every explicit pin resolved successfully.\"\n"
    "                        f\"{skipped_text}\"\n"
    "                    ) from resolve_exc\n"
    "                raise RuntimeError(\n"
    "                    format_runtime_provider_error(resolve_exc)\n"
    "                ) from resolve_exc\n",
)

new_safety = indent(
    dedent(
        '''
        finally:
            # Safety net (#90089): if this exact execution remains in-flight
            # after body control flow exits, a terminal write was lost. That is
            # not positive failure evidence: external side effects may already
            # have run. Settle through the ledger's immutable CAS as ``unknown``
            # so admission is unblocked without making a retry look safe.
            try:
                _unknown_record = mark_execution_unknown(
                    execution_id,
                    error=(
                        "Execution was left in a non-terminal state after the "
                        "job body completed; whether side effects ran is unknown. "
                        "See #90089."
                    ),
                )
                if _unknown_record is not None:
                    logger.error(
                        "Job '%s': execution %s left in-flight after run_one_job "
                        "body completed — safety net marked outcome unknown",
                        job["id"],
                        execution_id,
                    )
            except Exception:
                pass
        '''
    ),
    "    "
)
replace_between_once(
    scheduler_path,
    "    finally:\n        # Safety net (#90089):",
    "\n\n\ndef _notify_provider_jobs_changed() -> None:\n",
    new_safety,
)


# 3) Replace the model-fallback assertions with pair-aware witnesses.
model_test_path = "tests/cron/test_model_pin_fallback_90089.py"
replace_once(
    model_test_path,
    "        except Exception as exc:\n"
    "            return False, \"\", str(exc), captured_agent_kwargs.get(\"model\", \"\")\n\n"
    "    return success, output, error, captured_agent_kwargs.get(\"model\", \"\")\n",
    "        except Exception as exc:\n"
    "            return (\n"
    "                False,\n"
    "                \"\",\n"
    "                str(exc),\n"
    "                captured_agent_kwargs.get(\"model\", \"\"),\n"
    "                captured_agent_kwargs.get(\"provider\", \"\"),\n"
    "            )\n\n"
    "    return (\n"
    "        success,\n"
    "        output,\n"
    "        error,\n"
    "        captured_agent_kwargs.get(\"model\", \"\"),\n"
    "        captured_agent_kwargs.get(\"provider\", \"\"),\n"
    "    )\n",
)

new_model_class = dedent(
    '''
    class TestProviderModelFallbackAtomicity:
        """Fallback may select only a route compatible with every explicit pin."""

        def test_incompatible_pinned_pair_fails_closed_on_auth_error(self, tmp_path):
            from hermes_cli.auth import AuthError

            job = _base_job(model="glm-4.5-air", provider="zai")
            success, _output, error, model_used, provider_used = _run_with_fallback(
                job,
                primary_provider="zai",
                primary_raises=AuthError("zai token expired"),
                fallback_provider="lmstudio",
                fallback_model="qwen3.8",
                tmp_path=tmp_path,
            )

            assert success is False
            assert "pinned route" in error
            assert model_used == ""
            assert provider_used == ""

        def test_unpinned_job_uses_the_configured_fallback_pair(self, tmp_path):
            from hermes_cli.auth import AuthError

            job = _base_job(
                model=None,
                provider=None,
                provider_snapshot=None,
                model_snapshot=None,
            )
            success, _output, error, model_used, provider_used = _run_with_fallback(
                job,
                primary_provider="zai",
                primary_raises=AuthError("zai token expired"),
                fallback_provider="lmstudio",
                fallback_model="qwen3.8",
                tmp_path=tmp_path,
            )

            assert success is True, error
            assert model_used == "qwen3.8"
            assert provider_used == "lmstudio"

        def test_model_only_pin_allows_a_matching_configured_route(self, tmp_path):
            from hermes_cli.auth import AuthError

            job = _base_job(model="shared-model", provider=None)
            success, _output, error, model_used, provider_used = _run_with_fallback(
                job,
                primary_provider="zai",
                primary_raises=AuthError("zai token expired"),
                fallback_provider="openrouter",
                fallback_model="shared-model",
                tmp_path=tmp_path,
            )

            assert success is True, error
            assert model_used == "shared-model"
            assert provider_used == "openrouter"

        def test_provider_only_pin_accepts_a_canonical_alias_match(self, tmp_path):
            from hermes_cli.auth import AuthError

            job = _base_job(model=None, provider="z-ai")
            success, _output, error, model_used, provider_used = _run_with_fallback(
                job,
                primary_provider="z-ai",
                primary_raises=AuthError("zai token expired"),
                fallback_provider="zai",
                fallback_model="glm-4.5-air",
                tmp_path=tmp_path,
            )

            assert success is True, error
            assert model_used == "glm-4.5-air"
            assert provider_used == "zai"

        def test_incompatible_pinned_pair_fails_closed_on_network_error(
            self, tmp_path
        ):
            import httpx

            job = _base_job(model="glm-4.5-air", provider="zai")
            success, _output, error, model_used, provider_used = _run_with_fallback(
                job,
                primary_provider="zai",
                primary_raises=httpx.ConnectError("DNS resolution failed"),
                fallback_provider="lmstudio",
                fallback_model="qwen3.8",
                tmp_path=tmp_path,
            )

            assert success is False
            assert "pinned route" in error
            assert model_used == ""
            assert provider_used == ""
    '''
)
model_test = read(model_test_path)
model_class_marker = "class TestPinnedModelNotOverwrittenByFallback:"
model_class_start = model_test.find(model_class_marker)
if model_class_start < 0:
    raise RuntimeError("model fallback test class anchor not found")
if model_test.find(model_class_marker, model_class_start + 1) >= 0:
    raise RuntimeError("model fallback test class anchor is not unique")
write(model_test_path, model_test[:model_class_start] + new_model_class)


# 4) Make the safety-net test assert uncertainty and immutable CAS.
silent_test_path = "tests/cron/test_silent_worker_death_90089.py"
new_silent_class = dedent(
    '''
    class TestRunOneJobSafetyNet:
        """Missing terminal evidence must settle as ``unknown``, not ``failed``."""

        def test_safety_net_marks_still_running_as_unknown(
            self, monkeypatch, tmp_path
        ):
            import cron.scheduler as scheduler
            import cron.executions as executions

            monkeypatch.setattr(
                executions, "EXECUTIONS_FILE", tmp_path / "cron" / "executions.db"
            )

            record = executions.create_execution("safety-net-job", source="direct")
            executions.mark_execution_running(record["id"])
            execution_id = record["id"]

            monkeypatch.setattr(
                scheduler,
                "run_job",
                lambda job, **kw: (True, "output", "response", None),
            )
            monkeypatch.setattr(scheduler, "claim_dispatch", lambda _jid: True)
            monkeypatch.setattr(
                scheduler, "save_job_output", lambda *_a, **_k: "/tmp/fake"
            )
            monkeypatch.setattr(
                scheduler, "_deliver_result", lambda *_a, **_k: None
            )
            monkeypatch.setattr(
                scheduler, "mark_job_run", lambda *_a, **_k: True
            )
            # Suppress the ordinary terminal writer. The finally-block CAS must
            # be the only path that can close this execution.
            monkeypatch.setattr(
                scheduler, "finish_execution", lambda *_a, **_k: None
            )

            job = {
                "id": "safety-net-job",
                "execution_id": execution_id,
                "prompt": "test",
            }
            scheduler.run_one_job(job)

            final = executions.latest_execution("safety-net-job")
            assert final["status"] == "unknown"
            assert "side effects ran is unknown" in final["error"]

        def test_unknown_settlement_is_terminal_and_immutable(
            self, monkeypatch, tmp_path
        ):
            executions = _point_ledger(monkeypatch, tmp_path)
            record = executions.create_execution("unknown-cas-job", source="direct")
            executions.mark_execution_running(record["id"])

            settled = executions.mark_execution_unknown(
                record["id"], error="outcome cannot be proved"
            )

            assert settled["status"] == "unknown"
            assert executions.finish_execution(record["id"], success=True) is None
            assert executions.mark_execution_unknown(record["id"]) is None
            final = executions.latest_execution("unknown-cas-job")
            assert final["status"] == "unknown"
            assert final["error"] == "outcome cannot be proved"
    '''
)
silent_test = read(silent_test_path)
silent_class_marker = "class TestRunOneJobSafetyNet:"
silent_class_start = silent_test.find(silent_class_marker)
if silent_class_start < 0:
    raise RuntimeError("silent-worker safety-net class anchor not found")
if silent_test.find(silent_class_marker, silent_class_start + 1) >= 0:
    raise RuntimeError("silent-worker safety-net class anchor is not unique")
write(silent_test_path, silent_test[:silent_class_start] + new_silent_class)

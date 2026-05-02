#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
brain_cli.py — WorldQuant Brain Toolbox Command-Line Interface.

Provides eleven command groups for AI-agent usage:
  auth       Login status, login, persona completion
  datasets   List, refresh, show, search, export-fields
  operators  List, refresh, show, search WQ Brain operators
  template   List, show, save, delete, placeholders
  generate   Preview strategies, generate file
  simulate   Enqueue, run, status, stop, results, list
  alpha      List, show, history, promote, reject registry entries
  backtest   List, show, filter, score, diversity, export
  evolution  Run, from-backtest, auto-run, status, stop, results, list
  telegram   Run Telegram bot polling and send status notifications
  worker     Run the persistent worker that watches Telegram and pending jobs

All commands support --json for machine-readable output.

Usage:
  python brain_cli.py <group> <command> [options]
  python brain_cli.py --help
"""

from __future__ import annotations

import argparse
import ast
import json
import logging
import os
import sys
from typing import Any, Optional

# Make sure the script can be run from anywhere
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

import cli_services as svc
import brain_worker as worker
import telegram_integration as tg

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

_JSON_MODE = False


def _envelope(data: Any, ok: bool = True, status: str = "ok",
              warnings: Optional[list] = None, errors: Optional[list] = None) -> dict:
    return {
        "ok": ok,
        "status": status,
        "data": data,
        "warnings": warnings or [],
        "errors": errors or [],
    }


def _service_error(data: Any) -> Optional[str]:
    if isinstance(data, dict) and str(data.get("status", "")).lower() == "error":
        return str(data.get("message") or data.get("error") or "Command failed.")
    return None


def _out(data: Any, as_json: bool, indent: int = 2):
    service_error = _service_error(data)
    if service_error:
        if as_json:
            print(json.dumps(
                _envelope(None, ok=False, status="error", errors=[{"message": service_error}]),
                ensure_ascii=False,
                indent=indent,
                default=str,
            ))
            sys.exit(1)
        _err(service_error)
    if as_json:
        print(json.dumps(_envelope(data), ensure_ascii=False, indent=indent, default=str))
    elif isinstance(data, list):
        for item in data:
            _print_item(item)
    elif isinstance(data, dict):
        _print_dict(data)
    else:
        print(str(data))


def _print_dict(d: dict, prefix: str = ""):
    for k, v in d.items():
        if isinstance(v, dict):
            print(f"{prefix}{k}:")
            _print_dict(v, prefix + "  ")
        elif isinstance(v, list) and v and isinstance(v[0], dict):
            print(f"{prefix}{k}: [{len(v)} items]")
        else:
            print(f"{prefix}{k}: {v}")


def _print_item(item: Any):
    if isinstance(item, dict):
        parts = []
        for k, v in item.items():
            if isinstance(v, str) and len(v) > 60:
                v = v[:60] + "…"
            parts.append(f"{k}={v}")
        print("  " + "  ".join(parts))
    else:
        print(f"  {item}")


def _table(rows: list, columns: Optional[list] = None, max_col_width: int = 40):
    """Print a simple fixed-width table."""
    if not rows:
        print("  (no rows)")
        return
    if columns is None:
        columns = list(rows[0].keys()) if isinstance(rows[0], dict) else None
    if columns is None:
        for r in rows:
            print(f"  {r}")
        return
    widths = {c: len(str(c)) for c in columns}
    for row in rows:
        for c in columns:
            widths[c] = min(max_col_width, max(widths[c], len(str(row.get(c, "")))))
    fmt  = "  " + "  ".join("{:<" + str(widths[c]) + "}" for c in columns)
    sep  = "  " + "  ".join("-" * widths[c] for c in columns)
    print(fmt.format(*[str(c)[:widths[c]] for c in columns]))
    print(sep)
    for row in rows:
        print(fmt.format(*[str(row.get(c, ""))[:widths[c]] for c in columns]))


def _err(msg: str):
    if _JSON_MODE:
        print(json.dumps(
            _envelope(None, ok=False, status="error", errors=[{"message": msg}]),
            ensure_ascii=False,
            indent=2,
            default=str,
        ))
        sys.exit(1)
    print(f"Error: {msg}", file=sys.stderr)
    sys.exit(1)


def _configure_foreground_logging(level_name: str):
    level = getattr(logging, str(level_name).upper(), None)
    if not isinstance(level, int):
        _err(f"Invalid log level: {level_name}")
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )


def _progress(msg: str):
    print(f"  … {msg}", file=sys.stderr)


class BrainArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        if _JSON_MODE:
            print(json.dumps(
                _envelope(None, ok=False, status="error", errors=[{"message": message}]),
                ensure_ascii=False,
                indent=2,
                default=str,
            ))
            self.exit(2)
        super().error(message)


# ---------------------------------------------------------------------------
# auth group
# ---------------------------------------------------------------------------

def cmd_auth(args):
    sub = args.auth_cmd

    if sub == "login-status":
        result = svc.auth_login_status(args.credentials)
        _out(result, args.json)

    elif sub == "login":
        print("Attempting login…", file=sys.stderr)
        result = svc.auth_complete_from_credentials(
            credentials_path=args.credentials,
            poll_interval=getattr(args, "poll_interval", 5),
            progress_cb=_progress,
        )
        _out({k: v for k, v in result.items() if k != "session"}, args.json)

    elif sub == "persona-complete":
        print("Attempting authentication and persona polling…", file=sys.stderr)
        result = svc.auth_complete_from_credentials(
            credentials_path=args.credentials,
            poll_interval=getattr(args, "poll_interval", 5),
            progress_cb=_progress,
        )
        _out({k: v for k, v in result.items() if k != "session"}, args.json)

    else:
        _err(f"Unknown auth sub-command: {sub}")


# ---------------------------------------------------------------------------
# datasets group
# ---------------------------------------------------------------------------

def cmd_datasets(args):
    sub = args.datasets_cmd

    if sub == "list":
        items = svc.datasets_list(args.datasets_dir)
        if args.json:
            _out(items, True)
        else:
            _table(items, ["dataset_id", "rows", "size_bytes"])

    elif sub == "refresh":
        print("Refreshing datasets from WQ Brain API…", file=sys.stderr)
        result = svc.datasets_refresh(
            datasets_dir=args.datasets_dir,
            credentials_path=args.credentials,
            progress_cb=_progress,
        )
        _out(result, args.json)

    elif sub == "show":
        df = svc.datasets_show(args.dataset_id, args.datasets_dir)
        if df is None:
            _err(f"Dataset '{args.dataset_id}' not found locally. Try: datasets refresh")
        limit = getattr(args, "limit", 50)
        rows  = df.head(limit).to_dict(orient="records")
        if args.json:
            _out({"dataset_id": args.dataset_id, "total": len(df), "rows": rows}, True)
        else:
            print(f"Dataset: {args.dataset_id}  ({len(df)} fields)")
            _table(rows, ["Field", "Description", "Type", "Coverage"])

    elif sub == "search":
        results = svc.datasets_search(
            args.query,
            datasets_dir=args.datasets_dir,
            dataset_id=getattr(args, "dataset_id", None),
        )
        if args.json:
            _out(results, True)
        else:
            print(f"Found {len(results)} fields matching '{args.query}':")
            _table(results, ["dataset_id", "field", "description", "coverage"])

    elif sub == "export-fields":
        result = svc.datasets_export_fields(
            args.dataset_id, args.output, args.datasets_dir
        )
        _out(result, args.json)

    else:
        _err(f"Unknown datasets sub-command: {sub}")


# ---------------------------------------------------------------------------
# operators group
# ---------------------------------------------------------------------------

def cmd_operators(args):
    sub = args.operators_cmd

    if sub == "list":
        items = svc.operators_list(args.operators_dir)
        if args.json:
            _out(items, True)
        else:
            _table(items, ["name", "category", "scope", "definition", "level"])

    elif sub == "refresh":
        print("Refreshing operators from WQ Brain API…", file=sys.stderr)
        result = svc.operators_refresh(
            operators_dir=args.operators_dir,
            credentials_path=args.credentials,
            include_docs=not getattr(args, "metadata_only", False),
            progress_cb=_progress,
        )
        _out(result, args.json)

    elif sub == "show":
        operator = svc.operators_show(
            args.name,
            operators_dir=args.operators_dir,
            include_doc=not getattr(args, "metadata_only", False),
        )
        if operator is None:
            _err(f"Operator '{args.name}' not found locally. Try: operators refresh")
        _out(operator, args.json)

    elif sub == "search":
        results = svc.operators_search(
            args.query,
            operators_dir=args.operators_dir,
            category=getattr(args, "category", None),
        )
        if args.json:
            _out(results, True)
        else:
            print(f"Found {len(results)} operators matching '{args.query}':")
            _table(results, ["name", "category", "scope", "definition", "level"])

    else:
        _err(f"Unknown operators sub-command: {sub}")


# ---------------------------------------------------------------------------
# template group
# ---------------------------------------------------------------------------

def cmd_template(args):
    sub = args.template_cmd

    if sub == "list":
        items = svc.templates_list(args.templates_dir)
        if args.json:
            _out(items, True)
        else:
            _table(items, ["name", "source", "description"])

    elif sub == "show":
        t = svc.templates_show(args.name, args.templates_dir)
        if t is None:
            _err(f"Template '{args.name}' not found.")
        if args.json:
            _out(t, True)
        else:
            print(f"Name:        {t['name']}")
            print(f"Description: {t.get('description', '')}")
            print(f"Source:      {t.get('source', 'custom')}")
            print(f"\nCode:\n{t['code']}")

    elif sub == "save":
        code_text = args.code
        if not code_text and args.code_file:
            with open(args.code_file, "r", encoding="utf-8") as fh:
                code_text = fh.read().strip()
        if not code_text:
            _err("Provide template code via --code or --code-file.")
        result = svc.templates_save(
            args.name, code_text,
            description=getattr(args, "description", ""),
            templates_dir=args.templates_dir,
        )
        _out(result, args.json)

    elif sub == "delete":
        result = svc.templates_delete(args.name, args.templates_dir)
        _out(result, args.json)

    elif sub == "placeholders":
        result = svc.templates_placeholders(args.name, args.templates_dir)
        _out(result, args.json)

    else:
        _err(f"Unknown template sub-command: {sub}")


# ---------------------------------------------------------------------------
# generate group
# ---------------------------------------------------------------------------

def _parse_pools(pool_strs: list) -> dict:
    """
    Parse pool arguments of the form  name=val1,val2,val3
    or  name=val1  name=val2  (multiple --pool flags for same name are merged).
    """
    pools: dict = {}
    for s in pool_strs:
        if "=" not in s:
            _err(f"Pool must be  name=val1,val2,…  got: {s!r}")
        name, vals_str = s.split("=", 1)
        vals = [v.strip() for v in vals_str.split(",") if v.strip()]
        if name in pools:
            pools[name].extend(vals)
        else:
            pools[name] = vals
    return pools


def _resolve_template(args) -> str:
    """Return template code from --template-name or --template."""
    if getattr(args, "template_name", None):
        t = svc.templates_show(args.template_name, svc.TEMPLATES_DIR)
        if t is None:
            _err(f"Template '{args.template_name}' not found.")
        return t["code"]
    if getattr(args, "template", None):
        return args.template
    _err("Provide --template '<code>' or --template-name '<name>'.")


def cmd_generate(args):
    sub = args.generate_cmd

    if sub == "preview":
        template = _resolve_template(args)
        pools    = _parse_pools(getattr(args, "pool", []) or [])
        results  = svc.generate_preview(
            template, pools,
            limit=getattr(args, "limit", 20),
            eliminate_dead=not getattr(args, "no_dead_code_elim", False),
        )
        if args.json:
            _out(results, True)
        else:
            print(f"Generated {len(results)} strategy previews:")
            for r in results:
                print(f"\n  [{r['index']}] candidate={r['candidate']}")
                print(f"       code: {r['code'][:120]}")

    elif sub == "file":
        template = _resolve_template(args)
        pools    = _parse_pools(getattr(args, "pool", []) or [])
        previews = svc.generate_preview(
            template, pools,
            limit=getattr(args, "limit", 1000),
            eliminate_dead=not getattr(args, "no_dead_code_elim", False),
        )
        # Apply simulation param overrides
        sim_params = {}
        for param in ("decay", "delay", "neutralization", "region", "truncation", "universe"):
            val = getattr(args, param, None)
            if val is not None:
                sim_params[param] = val

        output = getattr(args, "output", None) or os.path.join(svc.ALPHAS_DIR,
            f"generated_{__import__('datetime').datetime.now().strftime('%Y%m%d_%H%M%S')}.py")
        result = svc.generate_file(previews, output, sim_params or None)
        _out(result, args.json)

    else:
        _err(f"Unknown generate sub-command: {sub}")


# ---------------------------------------------------------------------------
# simulate group
# ---------------------------------------------------------------------------

def _load_params_from_arg(args) -> list:
    """Load simulation parameters from CSV or JSON file, or inline JSON."""
    if getattr(args, "params_file", None):
        fp = args.params_file
        if fp.endswith(".json"):
            with open(fp, "r", encoding="utf-8") as fh:
                return json.load(fh)
        elif fp.endswith(".py"):
            with open(fp, "r", encoding="utf-8") as fh:
                module_ast = ast.parse(fh.read(), filename=fp)
            for node in module_ast.body:
                if isinstance(node, ast.Assign):
                    for target in node.targets:
                        if isinstance(target, ast.Name) and target.id == "DATA":
                            value = ast.literal_eval(node.value)
                            if not isinstance(value, list):
                                _err("Python strategy file DATA must be a list of strategy dicts.")
                            return value
            _err("Python strategy file is missing a top-level DATA = [...] assignment.")
        else:  # CSV
            import pandas as pd
            df = pd.read_csv(fp)
            return df.to_dict(orient="records")
    if getattr(args, "params_json", None):
        return json.loads(args.params_json)
    if getattr(args, "code", None):
        # Quick inline single-alpha shorthand
        defaults = {
            "code":          args.code,
            "decay":         getattr(args, "decay", 4),
            "delay":         getattr(args, "delay", 1),
            "neutralization":getattr(args, "neutralization", "SUBINDUSTRY"),
            "region":        getattr(args, "region", "USA"),
            "truncation":    getattr(args, "truncation", 0.08),
            "universe":      getattr(args, "universe", "TOP3000"),
        }
        return [defaults]
    _err("Provide --params-file, --params-json, or --code.")


def cmd_simulate(args):
    sub = args.simulate_cmd

    if sub == "enqueue":
        params = _load_params_from_arg(args)
        job_id = svc.simulate_enqueue(params, credentials_path=args.credentials)
        result = {"job_id": job_id, "queued": len(params), "status": "pending"}
        _out(result, args.json)

    elif sub == "run":
        # Support running immediately (no pre-enqueue required)
        if getattr(args, "job_id", None):
            job_id = args.job_id
        else:
            params = _load_params_from_arg(args)
            job_id = svc.simulate_enqueue(params, credentials_path=args.credentials)
            print(f"Created job: {job_id}", file=sys.stderr)

        print(f"Running simulation job {job_id}…", file=sys.stderr)
        result = svc.simulate_run(job_id, progress_cb=_progress)
        _out(result, args.json)

    elif sub == "status":
        job = svc.simulate_status(args.job_id)
        if job is None:
            _err(f"Job '{args.job_id}' not found.")
        _out(job, args.json)

    elif sub == "stop":
        result = svc.simulate_stop(args.job_id)
        _out(result, args.json)

    elif sub == "results":
        limit = getattr(args, "limit", 100)
        data  = svc.simulate_results(args.job_id, limit=limit)
        if data is None:
            _err(f"Job '{args.job_id}' not found.")
        if args.json:
            _out(data, True)
        else:
            job = data.get("job", {})
            print(f"Job {job.get('id')}  status={job.get('status')}  "
                  f"result_file={job.get('result_file')}")
            rows = data.get("rows", [])
            print(f"\n{len(rows)} rows (of {data.get('total', '?')} total):")
            if rows:
                _table(rows[:25], ["passed", "sharpe", "fitness", "turnover", "universe", "code"])

    elif sub == "list":
        jobs = svc.simulate_list()
        if args.json:
            _out(jobs, True)
        else:
            _table(jobs, ["id", "status", "created_at", "updated_at", "result_file"])

    else:
        _err(f"Unknown simulate sub-command: {sub}")


# ---------------------------------------------------------------------------
# alpha group
# ---------------------------------------------------------------------------

def cmd_alpha(args):
    sub = args.alpha_cmd

    if sub == "list":
        items = svc.alpha_list(
            status=getattr(args, "status", None),
            source=getattr(args, "source", None),
            min_sharpe=getattr(args, "min_sharpe", None),
            min_fitness=getattr(args, "min_fitness", None),
            limit=getattr(args, "limit", 50),
        )
        if args.json:
            _out(items, True)
        else:
            _table(items, ["alpha_hash", "alpha_id", "status", "source", "latest_result_link", "code"])

    elif sub == "show":
        alpha = svc.alpha_show(args.identifier)
        if alpha is None:
            _err(f"Alpha '{args.identifier}' not found.")
        _out(alpha, args.json)

    elif sub == "history":
        data = svc.alpha_history(args.identifier)
        if data is None:
            _err(f"Alpha '{args.identifier}' not found.")
        _out(data, args.json)

    elif sub == "promote":
        alpha = svc.alpha_promote(args.identifier, reason=getattr(args, "reason", None))
        if alpha is None:
            _err(f"Alpha '{args.identifier}' not found.")
        _out(alpha, args.json)

    elif sub == "reject":
        alpha = svc.alpha_reject(args.identifier, reason=args.reason)
        if alpha is None:
            _err(f"Alpha '{args.identifier}' not found.")
        _out(alpha, args.json)

    else:
        _err(f"Unknown alpha sub-command: {sub}")


# ---------------------------------------------------------------------------
# backtest group
# ---------------------------------------------------------------------------

def cmd_backtest(args):
    sub = args.backtest_cmd

    if sub == "list":
        items = svc.backtest_list(args.data_dir)
        if args.json:
            _out(items, True)
        else:
            _table(items, ["file", "rows", "size_bytes"])

    elif sub == "show":
        data = svc.backtest_show(args.file, limit=getattr(args, "limit", 20),
                                  data_dir=args.data_dir)
        if data is None:
            _err(f"File '{args.file}' not found in {args.data_dir}.")
        if args.json:
            _out(data, True)
        else:
            print(f"File:    {data['file']}")
            print(f"Rows:    {data['rows']}")
            print(f"Columns: {data['columns']}")
            print("\nSummary:")
            _print_dict(data["summary"], prefix="  ")
            print(f"\nFirst {len(data['head'])} rows:")
            if data["head"]:
                _table(data["head"][:10],
                       ["passed", "sharpe", "fitness", "turnover", "universe", "code"])

    elif sub == "filter":
        df = svc.backtest_filter(
            args.file,
            sharpe_min    = getattr(args, "sharpe_min", None),
            fitness_min   = getattr(args, "fitness_min", None),
            passed_only   = getattr(args, "passed_only", False),
            universe      = getattr(args, "universe", None),
            neutralization= getattr(args, "neutralization", None),
            data_dir      = args.data_dir,
        )
        if df is None:
            _err(f"File '{args.file}' not found.")
        rows = df.to_dict(orient="records")
        if args.json:
            _out({"rows": rows, "total": len(df)}, True)
        else:
            print(f"Filtered: {len(df)} rows")
            _table(rows[:25], ["passed", "sharpe", "fitness", "turnover", "universe", "code"])

    elif sub == "score":
        top_n = getattr(args, "top", 0)
        df    = svc.backtest_score(args.file, top_n=top_n, data_dir=args.data_dir)
        if df is None:
            _err(f"File '{args.file}' not found.")
        rows = df.to_dict(orient="records")
        if args.json:
            _out({"rows": rows, "total": len(df)}, True)
        else:
            print(f"Scored {len(df)} rows (composite_score added):")
            _table(rows[:25], ["composite_score", "passed", "sharpe", "fitness", "universe", "code"])

    elif sub == "diversity":
        top_n      = getattr(args, "top", 20)
        min_hamming= getattr(args, "min_hamming", 0.5)
        df         = svc.backtest_diversity_filter(
            args.file, top_n=top_n, min_hamming=min_hamming, data_dir=args.data_dir
        )
        if df is None:
            _err(f"File '{args.file}' not found.")
        rows = df.to_dict(orient="records")
        if args.json:
            _out({"rows": rows, "total": len(df)}, True)
        else:
            print(f"Diversity-filtered: {len(df)} rows (top {top_n}, min_hamming={min_hamming}):")
            _table(rows[:25], ["composite_score", "passed", "sharpe", "fitness", "code"])

    elif sub == "export":
        output = getattr(args, "output", args.file.replace(".csv", "_export.csv"))
        fmt    = getattr(args, "format", "csv")
        result = svc.backtest_export(args.file, output, format=fmt, data_dir=args.data_dir)
        _out(result, args.json)

    else:
        _err(f"Unknown backtest sub-command: {sub}")


# ---------------------------------------------------------------------------
# evolution group
# ---------------------------------------------------------------------------

def cmd_evolution(args):
    sub = args.evolution_cmd

    if sub == "run":
        template = _resolve_template(args)
        pools    = _parse_pools(getattr(args, "pool", []) or [])
        if not pools:
            _err("Provide at least one --pool name=val1,val2,… for evolution.")

        job_id = svc.evolution_enqueue(
            template         = template,
            pools            = pools,
            pop_size         = getattr(args, "pop_size", 40),
            generations      = getattr(args, "generations", 10),
            mutation_rate    = getattr(args, "mutation_rate", 0.4),
            diversity_weight = getattr(args, "diversity_weight", 0.7),
            top_k            = getattr(args, "top_k", 20),
        )
        print(f"Created evolution job: {job_id}", file=sys.stderr)

        if not getattr(args, "detach", False):
            print(f"Running evolution…", file=sys.stderr)
            result = svc.evolution_run_job(job_id, progress_cb=_progress)
            _out(result, args.json)
        else:
            _out({"job_id": job_id, "status": "pending"}, args.json)

    elif sub == "from-backtest":
        template = _resolve_template(args)
        pools    = _parse_pools(getattr(args, "pool", []) or [])
        result   = svc.evolution_from_backtest(
            template     = template,
            pools        = pools,
            backtest_csv = args.backtest_file,
            top_seed     = getattr(args, "top_seed", 10),
            pop_size     = getattr(args, "pop_size", 40),
            generations  = getattr(args, "generations", 10),
            top_k        = getattr(args, "top_k", 20),
            data_dir     = args.data_dir,
        )
        if args.json:
            _out(result, True)
        else:
            print(f"Matched {result.get('matched')} backtest rows to {result.get('seeds')} seeds.")
            print(f"Results ({len(result.get('results', []))} candidates):")
            for r in result.get("results", [])[:10]:
                print(f"  score={r['score']:.4f}  candidate={r['candidate']}")
                print(f"    {r['code'][:100]}")

    elif sub == "auto-run":
        template   = _resolve_template(args)
        pools      = _parse_pools(getattr(args, "pool", []) or [])
        summary = svc.evolution_auto_run(
            template          = template,
            pools             = pools,
            rounds            = getattr(args, "rounds", 3),
            pop_size          = getattr(args, "pop_size", 40),
            generations       = getattr(args, "generations", 10),
            mutation_rate     = getattr(args, "mutation_rate", 0.4),
            diversity_weight  = getattr(args, "diversity_weight", 0.7),
            top_k             = getattr(args, "top_k", 20),
            credentials_path  = args.credentials,
            sim_params        = {
                "decay": getattr(args, "decay", 4),
                "delay": getattr(args, "delay", 1),
                "neutralization": getattr(args, "neutralization", "SUBINDUSTRY"),
                "region": getattr(args, "region", "USA"),
                "truncation": getattr(args, "truncation", 0.08),
                "universe": getattr(args, "universe", "TOP3000"),
            },
            progress_cb       = _progress,
        )
        _out(summary, args.json)

    elif sub == "status":
        job = svc.evolution_status(args.job_id)
        if job is None:
            _err(f"Job '{args.job_id}' not found.")
        _out(job, args.json)

    elif sub == "stop":
        result = svc.evolution_stop(args.job_id)
        _out(result, args.json)

    elif sub == "results":
        data = svc.evolution_results(args.job_id)
        if data is None:
            _err(f"Job '{args.job_id}' not found.")
        if args.json:
            _out(data, True)
        else:
            job     = data.get("job", {})
            results = data.get("results", [])
            print(f"Job {job.get('id')}  status={job.get('status')}")
            print(f"\n{len(results)} evolved candidates:")
            for r in results[:20]:
                print(f"  score={r.get('score', 0):.4f}  candidate={r.get('candidate')}")
                print(f"    {r.get('code', '')[:100]}")

    elif sub == "list":
        jobs = svc.evolution_list()
        if args.json:
            _out(jobs, True)
        else:
            _table(jobs, ["id", "status", "created_at", "updated_at", "result_file"])

    else:
        _err(f"Unknown evolution sub-command: {sub}")


# ---------------------------------------------------------------------------
# telegram group
# ---------------------------------------------------------------------------

def cmd_telegram(args):
    sub = args.telegram_cmd

    if sub == "run":
        _configure_foreground_logging(getattr(args, "log_level", "INFO"))
        try:
            runner = tg.TelegramBotRunner(
                credentials_path=args.credentials,
                poll_timeout=getattr(args, "poll_timeout", tg.DEFAULT_POLL_TIMEOUT),
            )
        except tg.TelegramConfigError as exc:
            _err(str(exc))
        print("Starting Telegram bot polling…", file=sys.stderr)
        runner.run(once=getattr(args, "once", False))

    elif sub == "status":
        try:
            result = tg.send_status_message(credentials_path=args.credentials)
        except tg.TelegramConfigError as exc:
            _err(str(exc))
        _out(result, args.json)

    elif sub == "chat-id":
        try:
            result = tg.discover_chat_id(
                limit=getattr(args, "limit", 20),
                write_env=getattr(args, "write_env", False),
            )
        except tg.TelegramConfigError as exc:
            _err(str(exc))
        _out(result, args.json)

    else:
        _err(f"Unknown telegram sub-command: {sub}")


# ---------------------------------------------------------------------------
# worker group
# ---------------------------------------------------------------------------

def cmd_worker(args):
    sub = args.worker_cmd

    if sub == "run":
        _configure_foreground_logging(getattr(args, "log_level", "INFO"))
        print("Starting persistent brain worker…", file=sys.stderr)
        runner = worker.BrainWorker(
            credentials_path=args.credentials,
            poll_interval=getattr(args, "poll_interval", worker.DEFAULT_POLL_INTERVAL),
        )
        runner.run_forever()

    elif sub == "status":
        _out(worker.worker_status(), args.json)

    else:
        _err(f"Unknown worker sub-command: {sub}")


# ---------------------------------------------------------------------------
# Argument parser construction
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    root = BrainArgumentParser(
        prog="brain_cli",
        description="WorldQuant Brain Toolbox CLI — AI-agent friendly interface.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Global flags (usable anywhere): --json  --credentials FILE",
    )

    sub_root = root.add_subparsers(dest="group", metavar="<group>")
    sub_root.required = True

    # ── auth ────────────────────────────────────────────────────────────────
    p_auth = sub_root.add_parser("auth", help="Authentication commands.")
    auth_sub = p_auth.add_subparsers(dest="auth_cmd", metavar="<cmd>")
    auth_sub.required = True

    auth_sub.add_parser("login-status",
        help="Check whether current credentials authenticate successfully.")

    p_login = auth_sub.add_parser("login",
        help="Login and complete Persona verification (polls until done).")
    p_login.add_argument("--poll-interval", type=int, default=5, dest="poll_interval",
                         help="Seconds between Persona poll attempts (default: 5).")

    p_persona = auth_sub.add_parser("persona-complete",
        help="Complete a pending Persona verification challenge.")
    p_persona.add_argument("--poll-interval", type=int, default=5, dest="poll_interval")

    # ── datasets ─────────────────────────────────────────────────────────────
    p_ds = sub_root.add_parser("datasets", help="Dataset management commands.")
    p_ds.add_argument("--datasets-dir", default=svc.DATASETS_DIR, dest="datasets_dir")
    ds_sub = p_ds.add_subparsers(dest="datasets_cmd", metavar="<cmd>")
    ds_sub.required = True

    ds_sub.add_parser("list", help="List locally cached datasets.")

    ds_sub.add_parser("refresh",
        help="Fetch all dataset field metadata from WQ Brain API and cache locally.")

    p_ds_show = ds_sub.add_parser("show", help="Show fields for a dataset.")
    p_ds_show.add_argument("dataset_id", help="Dataset ID (e.g., fundamental6).")
    p_ds_show.add_argument("--limit", type=int, default=50)

    p_ds_search = ds_sub.add_parser("search",
        help="Search field names and descriptions across all/one dataset.")
    p_ds_search.add_argument("query", help="Search term.")
    p_ds_search.add_argument("--dataset-id", default=None, dest="dataset_id",
                              help="Restrict search to one dataset.")

    p_ds_export = ds_sub.add_parser("export-fields",
        help="Export a dataset's fields to a CSV file.")
    p_ds_export.add_argument("dataset_id")
    p_ds_export.add_argument("output", help="Output CSV path.")

    # ── operators ────────────────────────────────────────────────────────────
    p_ops = sub_root.add_parser("operators", help="WQ Brain operator metadata commands.")
    p_ops.add_argument("--operators-dir", default=svc.OPERATORS_DIR, dest="operators_dir")
    ops_sub = p_ops.add_subparsers(dest="operators_cmd", metavar="<cmd>")
    ops_sub.required = True

    ops_sub.add_parser("list", help="List locally cached operators.")

    p_ops_refresh = ops_sub.add_parser("refresh",
        help="Fetch operator metadata and detailed docs from WQ Brain API.")
    p_ops_refresh.add_argument("--metadata-only", action="store_true",
                               help="Only refresh operators.json; skip per-operator docs.")

    p_ops_show = ops_sub.add_parser("show", help="Show one operator by name.")
    p_ops_show.add_argument("name", help="Operator name (e.g., ts_rank).")
    p_ops_show.add_argument("--metadata-only", action="store_true",
                            help="Do not include cached detailed doc JSON.")

    p_ops_search = ops_sub.add_parser("search",
        help="Search operator names, definitions, descriptions, and metadata.")
    p_ops_search.add_argument("query", help="Search term.")
    p_ops_search.add_argument("--category", default=None,
                              help="Restrict search to one category (e.g., Group).")

    # ── template ─────────────────────────────────────────────────────────────
    p_tpl = sub_root.add_parser("template", help="Code template management.")
    p_tpl.add_argument("--templates-dir", default=svc.TEMPLATES_DIR, dest="templates_dir")
    tpl_sub = p_tpl.add_subparsers(dest="template_cmd", metavar="<cmd>")
    tpl_sub.required = True

    tpl_sub.add_parser("list", help="List all templates (default + custom).")

    p_tpl_show = tpl_sub.add_parser("show", help="Show template code and metadata.")
    p_tpl_show.add_argument("name")

    p_tpl_save = tpl_sub.add_parser("save",
        help="Create or update a custom template.")
    p_tpl_save.add_argument("name", help="Template name.")
    p_tpl_save.add_argument("--code", default=None,
                             help="Inline template code string.")
    p_tpl_save.add_argument("--code-file", default=None, dest="code_file",
                             help="Path to a file containing the template code.")
    p_tpl_save.add_argument("--description", default="")

    p_tpl_del = tpl_sub.add_parser("delete", help="Delete a custom template.")
    p_tpl_del.add_argument("name")

    p_tpl_ph = tpl_sub.add_parser("placeholders",
        help="List {placeholder} names in a template.")
    p_tpl_ph.add_argument("name")

    # ── generate ─────────────────────────────────────────────────────────────
    p_gen = sub_root.add_parser("generate", help="Strategy generation commands.")
    gen_sub = p_gen.add_subparsers(dest="generate_cmd", metavar="<cmd>")
    gen_sub.required = True

    def _add_template_args(p):
        grp = p.add_mutually_exclusive_group(required=True)
        grp.add_argument("--template",      help="Inline template code.")
        grp.add_argument("--template-name", dest="template_name",
                          help="Load template by name.")
        p.add_argument("--pool", action="append", metavar="name=val1,val2",
                        help="Placeholder pool (repeat for multiple placeholders).")
        p.add_argument("--no-dead-code-elim", action="store_true", dest="no_dead_code_elim",
                        help="Disable dead code elimination.")

    def _add_sim_param_args(p):
        p.add_argument("--decay",          type=int,   default=None)
        p.add_argument("--delay",          type=int,   default=None)
        p.add_argument("--neutralization", default=None)
        p.add_argument("--region",         default=None)
        p.add_argument("--truncation",     type=float, default=None)
        p.add_argument("--universe",       default=None)

    p_gen_prev = gen_sub.add_parser("preview",
        help="Preview strategy codes generated from a template + pools.")
    _add_template_args(p_gen_prev)
    p_gen_prev.add_argument("--limit", type=int, default=20,
                             help="Max strategies to preview (default: 20).")

    p_gen_file = gen_sub.add_parser("file",
        help="Generate a Python strategy file from template + pools.")
    _add_template_args(p_gen_file)
    _add_sim_param_args(p_gen_file)
    p_gen_file.add_argument("--limit", type=int, default=1000)
    p_gen_file.add_argument("--output", default=None,
                             help="Output .py file path (auto-named if omitted).")

    # ── simulate ─────────────────────────────────────────────────────────────
    p_sim = sub_root.add_parser("simulate", help="Simulation job management.")
    p_sim.add_argument("--data-dir", default=svc.DATA_DIR, dest="data_dir")
    sim_sub = p_sim.add_subparsers(dest="simulate_cmd", metavar="<cmd>")
    sim_sub.required = True

    def _add_param_args(p):
        pg = p.add_mutually_exclusive_group()
        pg.add_argument("--params-file", dest="params_file", metavar="FILE",
                         help="CSV or JSON file with simulation parameters.")
        pg.add_argument("--params-json", dest="params_json", metavar="JSON",
                         help="Inline JSON array of parameter dicts.")
        pg.add_argument("--code", help="Single alpha expression (shorthand).")
        p.add_argument("--decay",          type=int,   default=4)
        p.add_argument("--delay",          type=int,   default=1)
        p.add_argument("--neutralization", default="SUBINDUSTRY")
        p.add_argument("--region",         default="USA")
        p.add_argument("--truncation",     type=float, default=0.08)
        p.add_argument("--universe",       default="TOP3000")

    p_enq = sim_sub.add_parser("enqueue",
        help="Enqueue a simulation job without running it.")
    _add_param_args(p_enq)

    p_run = sim_sub.add_parser("run",
        help="Run a simulation job (creates + runs in one step if no --job-id).")
    p_run.add_argument("--job-id", dest="job_id", default=None,
                        help="Run an existing queued job by ID.")
    _add_param_args(p_run)

    p_status = sim_sub.add_parser("status", help="Show job status.")
    p_status.add_argument("job_id")

    p_stop = sim_sub.add_parser("stop", help="Request job stop.")
    p_stop.add_argument("job_id")

    p_res = sim_sub.add_parser("results", help="Show simulation results.")
    p_res.add_argument("job_id")
    p_res.add_argument("--limit", type=int, default=100)

    sim_sub.add_parser("list", help="List all simulation jobs.")

    # ── alpha ─────────────────────────────────────────────────────────────────
    p_alpha = sub_root.add_parser("alpha", help="Alpha registry commands.")
    alpha_sub = p_alpha.add_subparsers(dest="alpha_cmd", metavar="<cmd>")
    alpha_sub.required = True

    p_alpha_list = alpha_sub.add_parser("list", help="List alpha registry entries.")
    p_alpha_list.add_argument("--status", default=None,
                              choices=["candidate", "simulated", "promoted", "rejected", "failed"],
                              help="Filter by alpha status (candidate, simulated, promoted, rejected, failed).")
    p_alpha_list.add_argument("--source", default=None,
                              choices=[
                                  "queued", "simulation", "ga", "template", "manual",
                                  "evolution", "backtest", "import", "unknown",
                              ],
                              help="Filter by alpha source.")
    p_alpha_list.add_argument("--min-sharpe", type=float, default=None, dest="min_sharpe")
    p_alpha_list.add_argument("--min-fitness", type=float, default=None, dest="min_fitness")
    p_alpha_list.add_argument("--limit", type=int, default=50)

    p_alpha_show = alpha_sub.add_parser("show", help="Show one alpha by hash or WQ alpha ID.")
    p_alpha_show.add_argument("identifier", help="alpha_hash or alpha_id")

    p_alpha_history = alpha_sub.add_parser("history", help="Show alpha simulation and event history.")
    p_alpha_history.add_argument("identifier", help="alpha_hash or alpha_id")

    p_alpha_promote = alpha_sub.add_parser("promote", help="Mark an alpha as promoted.")
    p_alpha_promote.add_argument("identifier", help="alpha_hash or alpha_id")
    p_alpha_promote.add_argument("--reason", default=None)

    p_alpha_reject = alpha_sub.add_parser("reject", help="Mark an alpha as rejected.")
    p_alpha_reject.add_argument("identifier", help="alpha_hash or alpha_id")
    p_alpha_reject.add_argument("--reason", required=True)

    # ── backtest ─────────────────────────────────────────────────────────────
    p_bt = sub_root.add_parser("backtest", help="Backtest data commands.")
    p_bt.add_argument("--data-dir", default=svc.DATA_DIR, dest="data_dir")
    bt_sub = p_bt.add_subparsers(dest="backtest_cmd", metavar="<cmd>")
    bt_sub.required = True

    bt_sub.add_parser("list", help="List available backtest CSV files.")

    p_bt_show = bt_sub.add_parser("show", help="Show summary of a backtest file.")
    p_bt_show.add_argument("file", help="CSV filename (basename or full path).")
    p_bt_show.add_argument("--limit", type=int, default=20)

    p_bt_filt = bt_sub.add_parser("filter",
        help="Filter backtest rows by numeric criteria.")
    p_bt_filt.add_argument("file")
    p_bt_filt.add_argument("--sharpe-min",     type=float, default=None, dest="sharpe_min")
    p_bt_filt.add_argument("--fitness-min",    type=float, default=None, dest="fitness_min")
    p_bt_filt.add_argument("--passed-only",    action="store_true", dest="passed_only")
    p_bt_filt.add_argument("--universe",       default=None)
    p_bt_filt.add_argument("--neutralization", default=None)

    p_bt_score = bt_sub.add_parser("score",
        help="Add composite_score column and sort descending.")
    p_bt_score.add_argument("file")
    p_bt_score.add_argument("--top", type=int, default=0,
                             help="Return only top N rows (0 = all).")

    p_bt_div = bt_sub.add_parser("diversity",
        help="Return diverse top-N rows by code token similarity.")
    p_bt_div.add_argument("file")
    p_bt_div.add_argument("--top",         type=int,   default=20)
    p_bt_div.add_argument("--min-hamming", type=float, default=0.5, dest="min_hamming",
                           help="Minimum fraction of tokens that must differ (default: 0.5).")

    p_bt_exp = bt_sub.add_parser("export", help="Export backtest data to file.")
    p_bt_exp.add_argument("file")
    p_bt_exp.add_argument("--output", default=None)
    p_bt_exp.add_argument("--format", choices=["csv", "json"], default="csv")

    # ── evolution ────────────────────────────────────────────────────────────
    p_evo = sub_root.add_parser("evolution", help="Evolution engine commands.")
    p_evo.add_argument("--data-dir", default=svc.DATA_DIR, dest="data_dir")
    evo_sub = p_evo.add_subparsers(dest="evolution_cmd", metavar="<cmd>")
    evo_sub.required = True

    def _add_evo_args(p, require_template: bool = True):
        if require_template:
            grp = p.add_mutually_exclusive_group(required=True)
            grp.add_argument("--template",      help="Inline template code.")
            grp.add_argument("--template-name", dest="template_name")
        else:
            p.add_argument("--template",      default=None)
            p.add_argument("--template-name", dest="template_name", default=None)
        p.add_argument("--pool", action="append", metavar="name=val1,val2")
        p.add_argument("--pop-size",         type=int,   default=40, dest="pop_size")
        p.add_argument("--generations",      type=int,   default=10)
        p.add_argument("--mutation-rate",    type=float, default=0.4, dest="mutation_rate")
        p.add_argument("--diversity-weight", type=float, default=0.7, dest="diversity_weight")
        p.add_argument("--top-k",            type=int,   default=20, dest="top_k")

    p_evo_run = evo_sub.add_parser("run",
        help="Run evolution engine and return top-k diverse candidates.")
    _add_evo_args(p_evo_run)
    p_evo_run.add_argument("--detach", action="store_true",
                            help="Create job but don't execute (enqueue only).")

    p_evo_bt = evo_sub.add_parser("from-backtest",
        help="Seed evolution from a backtest CSV and run.")
    _add_evo_args(p_evo_bt)
    p_evo_bt.add_argument("backtest_file",
                           help="Backtest CSV filename (in --data-dir or full path).")
    p_evo_bt.add_argument("--top-seed", type=int, default=10, dest="top_seed",
                           help="Number of top backtest matches to use as seeds.")

    p_evo_auto = evo_sub.add_parser("auto-run",
        help="Run multiple evolution rounds and accumulate candidates.")
    _add_evo_args(p_evo_auto)
    _add_sim_param_args(p_evo_auto)
    p_evo_auto.add_argument("--rounds", type=int, default=3)

    p_evo_status = evo_sub.add_parser("status", help="Check evolution job status.")
    p_evo_status.add_argument("job_id")

    p_evo_stop = evo_sub.add_parser("stop", help="Request stop for an evolution job.")
    p_evo_stop.add_argument("job_id")

    p_evo_res = evo_sub.add_parser("results", help="Show results of an evolution job.")
    p_evo_res.add_argument("job_id")

    evo_sub.add_parser("list", help="List all evolution jobs.")

    # ── telegram ──────────────────────────────────────────────────────────────
    p_tg = sub_root.add_parser("telegram", help="Telegram bot and notification commands.")
    tg_sub = p_tg.add_subparsers(dest="telegram_cmd", metavar="<cmd>")
    tg_sub.required = True

    p_tg_run = tg_sub.add_parser("run", help="Run the Telegram bot polling loop.")
    p_tg_run.add_argument("--poll-timeout", type=int, default=tg.DEFAULT_POLL_TIMEOUT, dest="poll_timeout",
                          help="Long-poll timeout in seconds (default: 60).")
    p_tg_run.add_argument("--once", action="store_true",
                          help="Process at most one polling cycle and exit.")
    p_tg_run.add_argument("--log-level", default="INFO",
                          choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                          help="Console log level for the polling loop (default: INFO).")

    tg_sub.add_parser("status", help="Send the current system status to the configured Telegram chat.")

    p_tg_chat = tg_sub.add_parser("chat-id", help="Discover recent Telegram chat IDs from getUpdates.")
    p_tg_chat.add_argument("--limit", type=int, default=20,
                           help="Maximum number of recent updates to inspect (default: 20).")
    p_tg_chat.add_argument("--write-env", action="store_true", dest="write_env",
                           help="Write the most recent discovered chat ID into .env as TELEGRAM_CHAT_ID.")

    # ── worker ────────────────────────────────────────────────────────────────
    p_worker = sub_root.add_parser("worker", help="Persistent worker commands.")
    worker_sub = p_worker.add_subparsers(dest="worker_cmd", metavar="<cmd>")
    worker_sub.required = True

    p_worker_run = worker_sub.add_parser("run", help="Run the persistent worker loop.")
    p_worker_run.add_argument("--poll-interval", type=int, default=worker.DEFAULT_POLL_INTERVAL, dest="poll_interval",
                              help="Seconds between pending-job scans (default: 3).")
    p_worker_run.add_argument("--log-level", default="INFO",
                              choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                              help="Console log level for the worker loop (default: INFO).")

    worker_sub.add_parser("status", help="Show whether the persistent worker is running.")

    return root


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

DISPATCH = {
    "auth":      cmd_auth,
    "datasets":  cmd_datasets,
    "operators": cmd_operators,
    "template":  cmd_template,
    "generate":  cmd_generate,
    "simulate":  cmd_simulate,
    "alpha":     cmd_alpha,
    "backtest":  cmd_backtest,
    "evolution": cmd_evolution,
    "telegram":  cmd_telegram,
    "worker":    cmd_worker,
}


def _preprocess_global_flags(argv: list) -> tuple:
    """
    Extract --json and --credentials FILE from anywhere in the argument list
    so they work whether placed before or after the subcommand group.
    Returns (cleaned_argv, json_flag, credentials_value).
    """
    json_flag    = False
    credentials  = svc.CREDS_PATH
    cleaned      = []
    i = 0
    while i < len(argv):
        tok = argv[i]
        if tok == "--json":
            json_flag = True
        elif tok in ("--credentials", "--credentials=") and i + 1 < len(argv):
            credentials = argv[i + 1]
            i += 1
        elif tok.startswith("--credentials="):
            credentials = tok.split("=", 1)[1]
        else:
            cleaned.append(tok)
        i += 1
    return cleaned, json_flag, credentials


def main():
    global _JSON_MODE
    raw_argv     = sys.argv[1:]
    cleaned_argv, json_flag, credentials = _preprocess_global_flags(raw_argv)
    _JSON_MODE = json_flag

    parser = build_parser()
    args   = parser.parse_args(cleaned_argv)

    # Inject the pre-processed global flags into the namespace
    args.json        = json_flag
    args.credentials = credentials

    # Propagate shared flags down
    if not hasattr(args, "datasets_dir"):
        args.datasets_dir = svc.DATASETS_DIR
    if not hasattr(args, "operators_dir"):
        args.operators_dir = svc.OPERATORS_DIR
    if not hasattr(args, "templates_dir"):
        args.templates_dir = svc.TEMPLATES_DIR
    if not hasattr(args, "data_dir"):
        args.data_dir = svc.DATA_DIR

    handler = DISPATCH.get(args.group)
    if handler is None:
        _err(f"Unknown group: {args.group}")
    handler(args)


if __name__ == "__main__":
    main()

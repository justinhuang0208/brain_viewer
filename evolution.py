#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Auto Evolution Engine for WorldQuant Brain Strategy Generator.

Implements a template-based genetic algorithm that evolves combinations of
placeholder values (e.g. {field}, {window}) for maximum coverage/diversity,
then ranks candidates and feeds them into the existing simulation workflow.

Supports closed-loop feedback: load a backtest CSV, match result rows back
to generated candidates, compute real fitness from Sharpe/fitness/subsharpe,
and seed the next generation from the top performers.
"""

import re
import csv
import os
import random
import itertools
from typing import Dict, List, Tuple, Any, Optional

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QTableWidget, QTableWidgetItem, QGroupBox, QFormLayout,
    QLineEdit, QSpinBox, QDoubleSpinBox, QMessageBox,
    QHeaderView, QAbstractItemView, QScrollArea, QCheckBox,
    QFrame, QSizePolicy, QFileDialog,
)
from PySide6.QtCore import Qt, Signal, Slot, QThread, QObject
from PySide6.QtGui import QFont, QColor

# ---------------------------------------------------------------------------
# Pure evolution engine  (zero Qt dependencies)
# ---------------------------------------------------------------------------

def parse_placeholders(template: str) -> List[str]:
    """Return ordered, deduplicated list of {name} placeholder names."""
    return list(dict.fromkeys(re.findall(r'\{(\w+)\}', template)))


def render_candidate(template: str, candidate: Dict[str, str]) -> str:
    """Substitute all {name} placeholders in template with candidate values."""
    result = template
    for key, value in candidate.items():
        result = result.replace(f'{{{key}}}', str(value))
    return result


def _candidate_key(candidate: Dict[str, str]) -> tuple:
    return tuple(sorted(candidate.items()))


def _hamming_diversity(candidate: Dict[str, str],
                       population: List[Dict[str, str]]) -> float:
    """
    Average fraction of placeholders that differ between this candidate and
    every other member of the population.  Range [0, 1]; higher = more diverse.
    """
    if len(population) <= 1:
        return 1.0
    keys = list(candidate.keys())
    if not keys:
        return 0.0
    total = 0.0
    for other in population:
        if other is candidate:
            continue
        diffs = sum(1 for k in keys if candidate.get(k) != other.get(k))
        total += diffs / len(keys)
    return total / (len(population) - 1)


def _uniqueness(candidate: Dict[str, str],
                population: List[Dict[str, str]]) -> float:
    """Score 1 for a unique candidate, penalised for each duplicate in pop."""
    key = _candidate_key(candidate)
    dups = sum(1 for c in population if _candidate_key(c) == key) - 1
    return max(0.0, 1.0 - dups * 0.5)


# ---------------------------------------------------------------------------
# Backtest feedback helpers  (zero Qt dependencies)
# ---------------------------------------------------------------------------

def _normalize_code(code: str) -> str:
    """Collapse all whitespace for code-string comparison."""
    return ' '.join(code.split())


def compute_real_fitness(row: Dict[str, Any],
                         sharpe_weight: float = 0.40,
                         fitness_weight: float = 0.30,
                         subsharpe_weight: float = 0.20,
                         turnover_weight: float = 0.10) -> float:
    """Compute a scalar fitness from one backtest result row.

    Uses sharpe, fitness, subsharpe, turnover, and a pass-bonus.
    Returns a non-negative float; higher is better.
    """
    def _f(key: str, default: float = 0.0) -> float:
        try:
            return float(row.get(key, default) or default)
        except (ValueError, TypeError):
            return float(default)

    sharpe     = _f('sharpe')
    fitness    = _f('fitness')
    subsharpe  = _f('subsharpe')
    turnover   = _f('turnover', 50.0)
    passed     = str(row.get('passed', '')).strip().upper() == 'PASS'

    # Normalise each metric toward [0, 1]
    sharpe_score    = max(0.0, sharpe)    / 2.0   # 2.0 → 1.0
    fitness_score   = max(0.0, fitness)            # already ~[0,1]
    subsharpe_score = max(0.0, subsharpe) / 1.5   # 1.5 → 1.0
    # Prefer turnover in the 3–20 % range; penalise extremes
    turnover_score  = max(0.0, 1.0 - abs(turnover - 10.0) / 30.0)

    score = (sharpe_weight    * sharpe_score
           + fitness_weight   * fitness_score
           + subsharpe_weight * subsharpe_score
           + turnover_weight  * turnover_score)

    if passed:
        score *= 1.10   # 10 % bonus for a passing alpha

    return round(score, 6)


def load_backtest_csv(path: str) -> List[Dict[str, Any]]:
    """Load a backtest CSV and return list of row dicts.

    Each row gets an extra ``_norm_code`` key (normalised code string).
    Rows without a non-empty ``code`` column are skipped.
    """
    rows: List[Dict[str, Any]] = []
    with open(path, newline='', encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            code = row.get('code', '').strip()
            if code:
                row['_norm_code'] = _normalize_code(code)
                rows.append(row)
    return rows


def match_results_to_csv(
    results: List[Tuple[float, Dict[str, str], str]],
    csv_rows: List[Dict[str, Any]],
) -> List[Tuple[float, Dict[str, str], str]]:
    """Match evolved candidates to CSV backtest rows by normalised code.

    Returns ``(real_fitness, candidate_dict, code)`` tuples for matched
    candidates, sorted by real_fitness descending.
    """
    index = {row['_norm_code']: row for row in csv_rows}
    matched: List[Tuple[float, Dict[str, str], str]] = []
    for _score, candidate, code in results:
        row = index.get(_normalize_code(code))
        if row is not None:
            matched.append((compute_real_fitness(row), candidate, code))
    matched.sort(key=lambda x: -x[0])
    return matched


def reverse_match_csv_to_template(
    template: str,
    pools: Dict[str, List[str]],
    csv_rows: List[Dict[str, Any]],
    max_combos: int = 20_000,
) -> List[Tuple[float, Dict[str, str], str]]:
    """Enumerate pool combinations and check each against CSV backtest codes.

    Useful when no prior ``_results`` exist yet — it finds any CSV row whose
    code is reproducible from the current template + pools.

    Returns ``(real_fitness, candidate_dict, code)`` sorted by real_fitness desc.
    """
    placeholders = parse_placeholders(template)
    active = [p for p in placeholders if p in pools]
    if not active:
        return []

    csv_index = {row['_norm_code']: row for row in csv_rows}
    if not csv_index:
        return []

    all_combos = list(itertools.product(*[pools[p] for p in active]))
    if len(all_combos) > max_combos:
        all_combos = random.sample(all_combos, max_combos)

    matched: List[Tuple[float, Dict[str, str], str]] = []
    seen: set = set()
    for combo in all_combos:
        candidate = dict(zip(active, combo))
        code = render_candidate(template, candidate)
        norm = _normalize_code(code)
        if norm in csv_index and norm not in seen:
            seen.add(norm)
            matched.append((compute_real_fitness(csv_index[norm]), candidate, code))

    matched.sort(key=lambda x: -x[0])
    return matched


class EvolutionEngine:
    """
    Genetic algorithm that evolves placeholder value combinations for
    maximum diversity / coverage across the defined value pools.

    Parameters
    ----------
    template : str
        Alpha expression containing ``{name}`` placeholders.
    pools : dict
        ``{placeholder_name: [value1, value2, ...]}``.
    pop_size : int
        Number of individuals in the population.
    generations : int
        Number of GA iterations.
    mutation_rate : float
        Probability of mutating each placeholder per offspring.
    parent_ratio : float
        Fraction of population kept as parents each generation.
    diversity_weight : float
        Weight on the diversity term vs uniqueness term in fitness.
    seed_population : list, optional
        Pre-selected candidate dicts (e.g. best from backtest) to use as
        the starting population.  Padded with random individuals to
        ``pop_size`` if shorter.
    known_real_fitness : dict, optional
        Mapping of candidate keys to real backtest fitness. When present, the
        GA blends known real fitness into selection so matched backtest winners
        are genuinely favoured during evolution rather than only used as seeds.
    """

    def __init__(self,
                 template: str,
                 pools: Dict[str, List[str]],
                 pop_size: int = 40,
                 generations: int = 10,
                 mutation_rate: float = 0.4,
                 parent_ratio: float = 0.3,
                 diversity_weight: float = 0.7,
                 seed_population: Optional[List[Dict[str, str]]] = None,
                 known_real_fitness: Optional[Dict[tuple, float]] = None):
        self.template = template
        self.pools = pools
        self.pop_size = max(pop_size, 4)
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.parent_num = max(2, int(self.pop_size * parent_ratio))
        self.diversity_weight = diversity_weight
        self.known_real_fitness = dict(known_real_fitness or {})

        template_phs = parse_placeholders(template)
        self.active_placeholders = [p for p in template_phs if p in pools]

        # Validate and store seed candidates (filter to only active placeholders)
        self.seed_population: List[Dict[str, str]] = []
        for cand in (seed_population or []):
            if all(p in cand for p in self.active_placeholders):
                self.seed_population.append(
                    {p: cand[p] for p in self.active_placeholders}
                )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _random_candidate(self) -> Dict[str, str]:
        return {p: random.choice(self.pools[p]) for p in self.active_placeholders}

    def _initial_population(self) -> List[Dict[str, str]]:
        all_combos = list(
            itertools.product(*[self.pools[p] for p in self.active_placeholders])
        )
        if len(all_combos) <= self.pop_size:
            pop = [dict(zip(self.active_placeholders, c)) for c in all_combos]
            while len(pop) < self.pop_size:
                pop.append(self._random_candidate())
        else:
            sampled = random.sample(all_combos, self.pop_size)
            pop = [dict(zip(self.active_placeholders, c)) for c in sampled]
        return pop

    def _fitness(self, candidate: Dict[str, str],
                 population: List[Dict[str, str]]) -> float:
        div = _hamming_diversity(candidate, population)
        uniq = _uniqueness(candidate, population)
        structural = self.diversity_weight * div + (1.0 - self.diversity_weight) * uniq
        real = self.known_real_fitness.get(_candidate_key(candidate))
        if real is None:
            return structural
        return 0.65 * real + 0.35 * structural

    def _score_population(self,
                          population: List[Dict[str, str]]
                          ) -> List[Tuple[float, Dict[str, str]]]:
        scored = [(self._fitness(c, population), c) for c in population]
        scored.sort(key=lambda x: -x[0])
        return scored

    def _crossover(self,
                   p1: Dict[str, str],
                   p2: Dict[str, str]) -> Dict[str, str]:
        n = len(self.active_placeholders)
        if n <= 1:
            return p1.copy()
        split = random.randint(1, n - 1)
        child = {}
        for i, ph in enumerate(self.active_placeholders):
            child[ph] = p1[ph] if i < split else p2[ph]
        return child

    def _mutate(self, candidate: Dict[str, str]) -> Dict[str, str]:
        mutant = candidate.copy()
        for ph in self.active_placeholders:
            if random.random() < self.mutation_rate:
                mutant[ph] = random.choice(self.pools[ph])
        return mutant

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, progress_callback=None) -> List[Tuple[float, Dict[str, str], str]]:
        """
        Evolve the population and return deduplicated results sorted by
        descending diversity score.

        Returns
        -------
        list of (score, candidate_dict, rendered_code)
        """
        if not self.active_placeholders:
            return []

        if self.seed_population:
            population = list(self.seed_population)
            while len(population) < self.pop_size:
                population.append(self._random_candidate())
            population = population[: self.pop_size]
        else:
            population = self._initial_population()

        for gen in range(self.generations):
            scored = self._score_population(population)
            parents = [c for _, c in scored[: self.parent_num]]

            kids: List[Dict[str, str]] = []
            attempts = 0
            target = self.pop_size - len(parents)
            while len(kids) < target and attempts < self.pop_size * 20:
                if len(parents) < 2:
                    child = self._mutate(parents[0])
                else:
                    p1, p2 = random.sample(parents, 2)
                    child = self._mutate(self._crossover(p1, p2))
                kids.append(child)
                attempts += 1

            population = (parents + kids)[: self.pop_size]

            if progress_callback:
                progress_callback(gen + 1, self.generations)

        # Final scoring + deduplication
        scored = self._score_population(population)
        seen: set = set()
        results: List[Tuple[float, Dict[str, str], str]] = []
        for score, candidate in scored:
            key = _candidate_key(candidate)
            if key not in seen:
                seen.add(key)
                code = render_candidate(self.template, candidate)
                results.append((score, candidate, code))
        return results


# ---------------------------------------------------------------------------
# Qt worker for background execution
# ---------------------------------------------------------------------------

class _EvolutionWorker(QObject):
    progress = Signal(int, int)   # current_gen, total_gen
    finished = Signal(list)       # list of (score, candidate, code)
    error = Signal(str)

    def __init__(self, engine: EvolutionEngine):
        super().__init__()
        self.engine = engine

    @Slot()
    def run(self):
        try:
            results = self.engine.run(progress_callback=self.progress.emit)
            self.finished.emit(results)
        except Exception as exc:
            self.error.emit(str(exc))


# ---------------------------------------------------------------------------
# EvolutionWidget  — plugs into GeneratorMainWindow as a tab
# ---------------------------------------------------------------------------

class EvolutionWidget(QWidget):
    """
    Auto Evolution panel.

    Reads the current template from *template_editor*, evolves combinations
    of ``{placeholder}`` values drawn from user-defined pools, ranks them by
    diversity, and emits a list of strategy dicts (same schema as the normal
    generator) via ``candidates_ready``.

    Parameters
    ----------
    template_editor : CodeTemplateEditor
        Live reference to the template editor so we can read its content.
    fields_widget : SelectedFieldsWidget
        Used to pre-populate the ``{field}`` pool with the current field list.
    settings_widget : StrategySettingsWidget
        Used to read base simulation settings when sending candidates.
    """

    # Emits list of strategy dicts — same format expected by SimulationWidget
    candidates_ready = Signal(list)
    # Auto-loop control signals (connected by app.py)
    request_simulation_start = Signal()    # → SimulationWidget.start_simulation_thread
    request_clear_simulation = Signal()    # → SimulationWidget.clear_for_auto_loop
    auto_loop_mode_changed = Signal(bool)  # → sets SimulationWidget.auto_loop_active

    def __init__(self, template_editor, fields_widget, settings_widget,
                 parent=None):
        super().__init__(parent)
        self.template_editor = template_editor
        self.fields_widget = fields_widget
        self.settings_widget = settings_widget

        self._results: List[Tuple[float, Dict[str, str], str]] = []
        self._pool_editors: Dict[str, QLineEdit] = {}
        self._thread: Optional[QThread] = None
        self._worker: Optional[_EvolutionWorker] = None
        # Backtest feedback state
        self._bt_fitness_map: Dict[str, float] = {}   # norm_code → real fitness
        self._seed_candidates: List[Dict[str, str]] = []  # seeds for next run
        self._bt_source_label_text: str = ""

        # Auto-loop state
        self._auto_active: bool = False
        self._auto_stop_requested: bool = False
        self._auto_remaining: int = 0
        self._auto_total: int = 0

        self._init_ui()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(8)

        # Description
        desc = QLabel(
            "<b>Auto Evolution</b> evolves combinations of "
            "<code>{placeholder}</code> values from the current template "
            "for maximum diversity.  "
            "Click <i>Refresh Placeholders</i> whenever you edit the template, "
            "define value pools, configure the GA, then run.  "
            "Use <i>Evolve From Backtest…</i> to load a backtest CSV, match "
            "result rows back to candidates and seed the next generation from "
            "the best performers."
        )
        desc.setWordWrap(True)
        layout.addWidget(desc)

        sep = QFrame()
        sep.setFrameShape(QFrame.HLine)
        sep.setFrameShadow(QFrame.Sunken)
        layout.addWidget(sep)

        # ── Pool editor ────────────────────────────────────────────────
        pool_outer = QGroupBox("Placeholder Value Pools")
        pool_outer_layout = QVBoxLayout(pool_outer)

        refresh_btn = QPushButton("⟳  Refresh Placeholders from Template")
        refresh_btn.setToolTip(
            "Re-scan the template for {placeholders} and rebuild this section."
        )
        refresh_btn.clicked.connect(self.refresh_placeholders)
        pool_outer_layout.addWidget(refresh_btn)

        self._pool_form_widget = QWidget()
        self._pool_form = QFormLayout(self._pool_form_widget)
        self._pool_form.setContentsMargins(4, 4, 4, 4)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(self._pool_form_widget)
        scroll.setMaximumHeight(180)
        scroll.setFrameShape(QFrame.NoFrame)
        pool_outer_layout.addWidget(scroll)

        layout.addWidget(pool_outer)

        # ── GA config ──────────────────────────────────────────────────
        cfg_group = QGroupBox("Evolution Config")
        cfg_form = QFormLayout(cfg_group)

        self._pop_size_spin = QSpinBox()
        self._pop_size_spin.setRange(10, 500)
        self._pop_size_spin.setValue(40)
        self._pop_size_spin.setToolTip("Total individuals per generation.")
        cfg_form.addRow("Population Size:", self._pop_size_spin)

        self._gen_spin = QSpinBox()
        self._gen_spin.setRange(1, 200)
        self._gen_spin.setValue(10)
        self._gen_spin.setToolTip("Number of GA generations to run.")
        cfg_form.addRow("Generations:", self._gen_spin)

        self._mutation_spin = QDoubleSpinBox()
        self._mutation_spin.setRange(0.0, 1.0)
        self._mutation_spin.setSingleStep(0.05)
        self._mutation_spin.setValue(0.4)
        self._mutation_spin.setToolTip(
            "Probability of mutating each placeholder slot per offspring."
        )
        cfg_form.addRow("Mutation Rate:", self._mutation_spin)

        self._top_k_spin = QSpinBox()
        self._top_k_spin.setRange(1, 500)
        self._top_k_spin.setValue(20)
        self._top_k_spin.setToolTip("Show this many top-ranked unique candidates.")
        cfg_form.addRow("Top-K Results:", self._top_k_spin)

        layout.addWidget(cfg_group)

        # ── Action bar ─────────────────────────────────────────────────
        action_bar = QHBoxLayout()

        self._run_btn = QPushButton("▶  Run Evolution")
        self._run_btn.setStyleSheet(
            "background-color: #1565c0; color: white; "
            "font-weight: bold; padding: 7px 18px;"
        )
        self._run_btn.setToolTip(
            "Evolve candidates using the GA and rank by diversity.  "
            "Clears any backtest seeds — use for a fresh diversity run."
        )
        self._run_btn.clicked.connect(self.run_evolution)
        action_bar.addWidget(self._run_btn)

        self._bt_btn = QPushButton("📊  Evolve From Backtest…")
        self._bt_btn.setStyleSheet(
            "background-color: #6a1b9a; color: white; "
            "font-weight: bold; padding: 7px 18px;"
        )
        self._bt_btn.setToolTip(
            "Load a backtest CSV, match result rows to candidates via code, "
            "compute real fitness (Sharpe/fitness/subsharpe/turnover), and "
            "seed the next evolution from the top performers."
        )
        self._bt_btn.clicked.connect(self._evolve_from_backtest)
        action_bar.addWidget(self._bt_btn)

        self._progress_label = QLabel("Ready.")
        self._progress_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        action_bar.addWidget(self._progress_label)

        self._send_btn = QPushButton("Send Selected → Simulation")
        self._send_btn.setStyleSheet(
            "background-color: #2e7d32; color: white; "
            "font-weight: bold; padding: 7px 18px;"
        )
        self._send_btn.setEnabled(False)
        self._send_btn.setToolTip(
            "Append checked candidates to the Simulation tab as strategy rows."
        )
        self._send_btn.clicked.connect(self.send_to_simulation)
        action_bar.addWidget(self._send_btn)

        layout.addLayout(action_bar)

        # Seed-status banner (shown only when backtest seeds are loaded)
        self._seed_label = QLabel("")
        self._seed_label.setStyleSheet(
            "color: #6a1b9a; font-style: italic; padding: 2px 4px;"
        )
        self._seed_label.setWordWrap(True)
        self._seed_label.hide()
        layout.addWidget(self._seed_label)

        # ── Results table ──────────────────────────────────────────────
        results_group = QGroupBox(
            "Evolved Candidates  (diversity score ↓ · BT Fitness = real backtest score)"
        )
        results_layout = QVBoxLayout(results_group)

        self._results_table = QTableWidget(0, 5)
        self._results_table.setHorizontalHeaderLabels(
            ["✔", "Diversity", "BT Fitness", "Parameters", "Rendered Code"]
        )
        self._results_table.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeToContents
        )
        self._results_table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeToContents
        )
        self._results_table.horizontalHeader().setSectionResizeMode(
            2, QHeaderView.ResizeToContents
        )
        self._results_table.horizontalHeader().setSectionResizeMode(
            3, QHeaderView.ResizeToContents
        )
        self._results_table.horizontalHeader().setSectionResizeMode(
            4, QHeaderView.Stretch
        )
        self._results_table.setFont(QFont("Consolas", 11))
        self._results_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._results_table.setAlternatingRowColors(True)
        results_layout.addWidget(self._results_table)

        layout.addWidget(results_group, 1)

        # ── Auto Loop section ──────────────────────────────────────────
        auto_sep = QFrame()
        auto_sep.setFrameShape(QFrame.HLine)
        auto_sep.setFrameShadow(QFrame.Sunken)
        layout.addWidget(auto_sep)

        auto_group = QGroupBox("🔁  Unattended Auto Loop")
        auto_outer = QVBoxLayout(auto_group)

        auto_cfg_row = QHBoxLayout()
        auto_cfg_row.addWidget(QLabel("Loop count:"))
        self._auto_loops_spin = QSpinBox()
        self._auto_loops_spin.setRange(1, 50)
        self._auto_loops_spin.setValue(3)
        self._auto_loops_spin.setToolTip(
            "Number of evolve → simulate → seed cycles to run unattended."
        )
        auto_cfg_row.addWidget(self._auto_loops_spin)
        auto_cfg_row.addStretch()

        self._auto_btn = QPushButton("▶▶  Start Auto Loop")
        self._auto_btn.setStyleSheet(
            "background-color: #e65100; color: white; font-weight: bold; padding: 7px 18px;"
        )
        self._auto_btn.setToolTip(
            "Run the full evolve → simulate → seed cycle automatically.\n"
            "Requires the Simulation tab to be logged in first (Check Login).\n\n"
            "Each cycle:\n"
            "  1. Evolve candidates with GA\n"
            "  2. Send top-K to Simulation tab and run\n"
            "  3. Wait for all simulations to finish\n"
            "  4. Compute real fitness (Sharpe/fitness/subsharpe/turnover)\n"
            "  5. Seed next generation from top performers\n\n"
            "Click again to stop safely after the current simulation batch."
        )
        self._auto_btn.clicked.connect(self._toggle_auto_loop)
        auto_cfg_row.addWidget(self._auto_btn)

        auto_outer.addLayout(auto_cfg_row)

        self._auto_status_label = QLabel("Auto loop: idle")
        self._auto_status_label.setStyleSheet(
            "color: #444; font-style: italic; padding: 3px 4px; "
            "background: #fff8e1; border: 1px solid #ffe082; border-radius: 3px;"
        )
        self._auto_status_label.setWordWrap(True)
        auto_outer.addWidget(self._auto_status_label)

        layout.addWidget(auto_group)

        # Populate pools on first show
        self.refresh_placeholders()

    # ------------------------------------------------------------------
    # Pool management
    # ------------------------------------------------------------------

    def refresh_placeholders(self):
        """Re-scan template and rebuild the pool editor rows."""
        template = self.template_editor.get_current_template()
        placeholders = parse_placeholders(template)

        # Preserve existing user input before clearing
        old_values: Dict[str, str] = {
            ph: editor.text() for ph, editor in self._pool_editors.items()
        }

        # Clear form
        while self._pool_form.rowCount() > 0:
            self._pool_form.removeRow(0)
        self._pool_editors.clear()

        if not placeholders:
            self._pool_form.addRow(
                QLabel(
                    "No <code>{placeholders}</code> found in the template."
                    "  Add e.g. <code>{field}</code> or <code>{window}</code>."
                ),
                QLabel("")
            )
            return

        fields = self.fields_widget.get_selected_fields()
        for ph in placeholders:
            editor = QLineEdit()
            editor.setFont(QFont("Consolas", 11))

            # Restore previously entered values
            if ph in old_values and old_values[ph].strip():
                editor.setText(old_values[ph])
            elif ph == "field":
                default = ", ".join(fields) if fields else "close, open, volume"
                editor.setPlaceholderText(
                    "Comma-separated fields (defaults to Selected Fields list)"
                )
                editor.setText(default)
            else:
                editor.setPlaceholderText(
                    "Comma-separated values, e.g.  5, 10, 21, 63, 126"
                )
                editor.setText("5, 10, 21, 63, 126")

            self._pool_form.addRow(f"  {{{ph}}}  pool:", editor)
            self._pool_editors[ph] = editor

    def _parse_pools(self) -> Optional[Dict[str, List[str]]]:
        pools: Dict[str, List[str]] = {}
        for ph, editor in self._pool_editors.items():
            raw = editor.text().strip()
            if not raw:
                QMessageBox.warning(
                    self, "Empty Pool",
                    f"Pool for {{{ph}}} is empty. Please enter comma-separated values."
                )
                return None
            values = [v.strip() for v in raw.split(",") if v.strip()]
            if not values:
                QMessageBox.warning(
                    self, "Empty Pool",
                    f"Pool for {{{ph}}} has no parseable values."
                )
                return None
            pools[ph] = values
        return pools

    # ------------------------------------------------------------------
    # Evolution execution
    # ------------------------------------------------------------------

    def run_evolution(self):
        """Run a fresh diversity-based evolution.  Clears any backtest seeds."""
        self._seed_candidates = []
        self._bt_fitness_map = {}
        self._seed_label.hide()
        self._start_evolution()

    def _start_evolution(self,
                         seeds: Optional[List[Dict[str, str]]] = None,
                         known_real_fitness: Optional[Dict[tuple, float]] = None):
        """Internal: build engine and launch background thread."""
        template = self.template_editor.get_current_template()
        if not template:
            QMessageBox.warning(self, "No Template", "The template editor is empty.")
            return

        pools = self._parse_pools()
        if pools is None:
            return

        engine = EvolutionEngine(
            template=template,
            pools=pools,
            pop_size=self._pop_size_spin.value(),
            generations=self._gen_spin.value(),
            mutation_rate=self._mutation_spin.value(),
            seed_population=seeds or [],
            known_real_fitness=known_real_fitness or {},
        )

        if not engine.active_placeholders:
            QMessageBox.warning(
                self, "No Active Placeholders",
                "None of the template's {placeholders} have a corresponding pool entry. "
                "Use Refresh Placeholders and fill in the pool fields."
            )
            return

        self._run_btn.setEnabled(False)
        self._bt_btn.setEnabled(False)
        self._send_btn.setEnabled(False)
        self._progress_label.setText("Starting evolution…")
        self._results_table.setRowCount(0)
        self._results = []

        self._thread = QThread()
        self._worker = _EvolutionWorker(engine)
        self._worker.moveToThread(self._thread)

        self._thread.started.connect(self._worker.run)
        self._worker.progress.connect(self._on_progress)
        self._worker.finished.connect(self._on_finished)
        self._worker.error.connect(self._on_error)
        self._worker.finished.connect(self._thread.quit)
        self._worker.error.connect(self._thread.quit)
        self._thread.finished.connect(self._cleanup_thread)

        self._thread.start()

    @Slot()
    def _cleanup_thread(self):
        self._thread = None
        self._worker = None

    @Slot(int, int)
    def _on_progress(self, current: int, total: int):
        self._progress_label.setText(f"Generation {current} / {total}…")

    @Slot(list)
    def _on_finished(self, results):
        self._results = results
        top_k = self._top_k_spin.value()
        self._populate_table(results[:top_k])
        shown = min(top_k, len(results))
        seed_note = ""
        if self._seed_candidates:
            seed_note = f"  (seeded from {len(self._seed_candidates)} backtest match(es))"
        self._progress_label.setText(
            f"Done — {len(results)} unique candidates, showing top {shown}.{seed_note}"
        )
        self._run_btn.setEnabled(True)
        self._bt_btn.setEnabled(True)
        self._send_btn.setEnabled(bool(results))

        if self._auto_active:
            self._auto_on_evolution_done(results)

    @Slot(str)
    def _on_error(self, msg: str):
        self._progress_label.setText(f"Error: {msg}")
        if self._auto_active:
            self._auto_abort(f"Evolution error: {msg}")
        else:
            QMessageBox.critical(self, "Evolution Error", msg)
        self._run_btn.setEnabled(True)
        self._bt_btn.setEnabled(True)

    # ------------------------------------------------------------------
    # Results table
    # ------------------------------------------------------------------

    def _populate_table(self, results):
        self._results_table.setRowCount(0)
        for score, candidate, code in results:
            row = self._results_table.rowCount()
            self._results_table.insertRow(row)

            # col 0 — Checkbox
            cb = QCheckBox()
            cb.setChecked(True)
            cb_container = QWidget()
            cb_layout = QHBoxLayout(cb_container)
            cb_layout.addWidget(cb)
            cb_layout.setAlignment(Qt.AlignCenter)
            cb_layout.setContentsMargins(0, 0, 0, 0)
            self._results_table.setCellWidget(row, 0, cb_container)

            # col 1 — Diversity score
            score_item = QTableWidgetItem(f"{score:.3f}")
            score_item.setTextAlignment(Qt.AlignCenter)
            self._results_table.setItem(row, 1, score_item)

            # col 2 — Real BT fitness (if this code was matched to backtest data)
            norm = _normalize_code(code)
            bt_score = self._bt_fitness_map.get(norm)
            if bt_score is not None:
                bt_item = QTableWidgetItem(f"{bt_score:.3f}")
                bt_item.setTextAlignment(Qt.AlignCenter)
                bt_item.setForeground(QColor("#6a1b9a"))
                bt_item.setToolTip(
                    "Real fitness from backtest: "
                    "0.4×Sharpe + 0.3×fitness + 0.2×subsharpe + 0.1×turnover_score"
                )
            else:
                bt_item = QTableWidgetItem("—")
                bt_item.setTextAlignment(Qt.AlignCenter)
                bt_item.setForeground(QColor("#aaaaaa"))
            self._results_table.setItem(row, 2, bt_item)

            # col 3 — Compact parameter summary
            params_str = "  ".join(f"{k}={v}" for k, v in sorted(candidate.items()))
            params_item = QTableWidgetItem(params_str)
            params_item.setForeground(QColor("#555555"))
            self._results_table.setItem(row, 3, params_item)

            # col 4 — Rendered code; store candidate dict in UserRole
            code_item = QTableWidgetItem(code.strip())
            code_item.setData(Qt.UserRole, candidate)
            self._results_table.setItem(row, 4, code_item)

    # ------------------------------------------------------------------
    # Send to simulation
    # ------------------------------------------------------------------

    def send_to_simulation(self):
        """Collect checked candidates and emit as strategy dicts."""
        settings = self.settings_widget.get_settings()

        strategies = []
        for row in range(self._results_table.rowCount()):
            cb_container = self._results_table.cellWidget(row, 0)
            if cb_container:
                cb = cb_container.findChild(QCheckBox)
                if cb and cb.isChecked():
                    code_item = self._results_table.item(row, 4)
                    if code_item:
                        strategy = settings.copy()
                        strategy["code"] = code_item.text().strip()
                        strategies.append(strategy)

        if not strategies:
            QMessageBox.warning(
                self, "Nothing Selected",
                "No candidates are checked. Tick the checkboxes in the ✔ column."
            )
            return

        self.candidates_ready.emit(strategies)
        QMessageBox.information(
            self, "Sent to Simulation",
            f"{len(strategies)} evolved candidate(s) appended to the Simulation tab."
        )

    # ------------------------------------------------------------------
    # Backtest-driven evolution
    # ------------------------------------------------------------------

    def _evolve_from_backtest(self):
        """Load a backtest CSV, match rows to candidates, seed next evolution."""
        template = self.template_editor.get_current_template()
        if not template:
            QMessageBox.warning(self, "No Template", "The template editor is empty.")
            return

        pools = self._parse_pools()
        if pools is None:
            return

        script_dir = os.path.dirname(os.path.abspath(__file__))
        default_dir = os.path.join(script_dir, "data")
        path, _ = QFileDialog.getOpenFileName(
            self, "Load Backtest CSV", default_dir, "CSV files (*.csv)"
        )
        if not path:
            return

        # Load CSV
        try:
            csv_rows = load_backtest_csv(path)
        except Exception as exc:
            QMessageBox.critical(
                self, "Load Error", f"Failed to read CSV:\n{exc}"
            )
            return

        if not csv_rows:
            QMessageBox.warning(
                self, "Empty File",
                "The selected CSV has no rows with a non-empty 'code' column."
            )
            return

        # ── Step 1: direct match against already-evolved results ────────
        matched = match_results_to_csv(self._results, csv_rows) if self._results else []

        # ── Step 2: reverse-match all pool combinations against CSV ─────
        reverse = reverse_match_csv_to_template(template, pools, csv_rows)
        # Merge without duplicates (prefer direct-match scores if both hit the same code)
        matched_norms = {_normalize_code(code) for _, _, code in matched}
        for rf, cand, code in reverse:
            if _normalize_code(code) not in matched_norms:
                matched.append((rf, cand, code))
                matched_norms.add(_normalize_code(code))
        matched.sort(key=lambda x: -x[0])

        if not matched:
            QMessageBox.warning(
                self, "No Matches Found",
                f"Could not match any of the {len(csv_rows)} CSV row(s) to the "
                f"current template and pools.\n\n"
                "Tips:\n"
                "• Make sure the CSV was generated from the same template\n"
                "• Ensure all relevant values are listed in the pool fields\n"
                "• Try clicking 'Refresh Placeholders' and checking the pools"
            )
            return

        # ── Step 3: store seeds and fitness map ─────────────────────────
        top_k = self._top_k_spin.value()
        seeds = [cand for _, cand, _ in matched[:top_k]]
        self._seed_candidates = seeds
        self._bt_fitness_map = {
            _normalize_code(code): rf for rf, _, code in matched
        }
        known_real_fitness = {
            _candidate_key(cand): rf for rf, cand, _ in matched[:top_k]
        }

        n_total   = len(csv_rows)
        n_matched = len(matched)
        top_rf    = matched[0][0]

        self._seed_label.setText(
            f"🌱 Backtest seed: {n_matched} match(es) from {n_total} CSV row(s) · "
            f"top real fitness: {top_rf:.3f} · "
            f"seeding next generation from top {len(seeds)} and blending real fitness into selection"
        )
        self._seed_label.show()

        # ── Step 4: launch seeded evolution ─────────────────────────────
        self._start_evolution(seeds=seeds, known_real_fitness=known_real_fitness)

    # ------------------------------------------------------------------
    # Auto Loop
    # ------------------------------------------------------------------

    def _toggle_auto_loop(self):
        if self._auto_active:
            self._stop_auto_loop()
        else:
            self._start_auto_loop()

    def _start_auto_loop(self):
        """Validate and start the unattended evolve→simulate→seed loop."""
        if self._thread is not None:
            QMessageBox.warning(self, "Busy",
                "An evolution is already running. Wait for it to finish before starting the auto loop.")
            return

        template = self.template_editor.get_current_template()
        if not template:
            QMessageBox.warning(self, "No Template", "The template editor is empty.")
            return
        if self._parse_pools() is None:
            return

        n = self._auto_loops_spin.value()
        self._auto_active = True
        self._auto_stop_requested = False
        self._auto_total = n
        self._auto_remaining = n
        self._seed_candidates = []
        self._bt_fitness_map = {}

        self._auto_btn.setText("⏹  Stop Auto Loop")
        self._auto_btn.setStyleSheet(
            "background-color: #b71c1c; color: white; font-weight: bold; padding: 7px 18px;"
        )
        self._run_btn.setEnabled(False)
        self._bt_btn.setEnabled(False)
        self._auto_loops_spin.setEnabled(False)
        self.auto_loop_mode_changed.emit(True)

        cycle = self._auto_total - self._auto_remaining + 1
        self._auto_status_label.setText(
            f"⏳ Loop {cycle}/{self._auto_total}: running evolution…"
        )
        self._start_evolution(seeds=[], known_real_fitness={})

    def _stop_auto_loop(self):
        """Request a graceful stop; the current evolution/sim is allowed to finish."""
        self._auto_stop_requested = True
        self._auto_status_label.setText(
            "Auto loop: stop requested. Will stop after the current evolution/simulation cycle."
        )
        self._auto_btn.setEnabled(False)

    def _auto_abort(self, reason: str):
        """Abort the auto loop with an error message."""
        self._auto_active = False
        self._auto_stop_requested = False
        self._auto_remaining = 0
        self.auto_loop_mode_changed.emit(False)
        self._auto_btn.setText("▶▶  Start Auto Loop")
        self._auto_btn.setStyleSheet(
            "background-color: #e65100; color: white; font-weight: bold; padding: 7px 18px;"
        )
        self._run_btn.setEnabled(True)
        self._bt_btn.setEnabled(True)
        self._auto_loops_spin.setEnabled(True)
        self._auto_btn.setEnabled(True)
        self._auto_status_label.setText(f"❌ Auto loop aborted: {reason}")
        QMessageBox.critical(self, "Auto Loop Aborted", reason)

    def _auto_on_evolution_done(self, results: List[Tuple[float, Dict[str, str], str]]):
        """Called after GA finishes during auto loop. Sends candidates to simulation."""
        if not self._auto_active:
            return
        if self._auto_stop_requested:
            self._auto_finish(stopped=True)
            return

        top_k = self._top_k_spin.value()
        candidates = results[:top_k]
        if not candidates:
            self._auto_abort("Evolution produced no candidates. Check template and pool settings.")
            return

        settings = self.settings_widget.get_settings()
        strategies = []
        for _score, _candidate, code in candidates:
            strategy = settings.copy()
            strategy["code"] = code.strip()
            strategies.append(strategy)

        cycle = self._auto_total - self._auto_remaining + 1
        self._auto_status_label.setText(
            f"⏳ Loop {cycle}/{self._auto_total}: sending {len(strategies)} candidates to Simulation…"
        )

        # Clear any leftover rows, load the new batch, then start simulation
        self.request_clear_simulation.emit()
        self.candidates_ready.emit(strategies)
        self.request_simulation_start.emit()

    @Slot(list)
    def on_simulation_completed(self, row_results: list):
        """
        Slot called by SimulationWidget.simulation_completed_with_results.

        ``row_results`` is a list of row-data lists with columns:
          [passed, delay, region, neutralization, decay, truncation,
           sharpe, fitness, turnover, weight, subsharpe, correlation,
           universe, link, code]
        """
        if not self._auto_active:
            return

        cycle = self._auto_total - self._auto_remaining + 1

        if not row_results:
            # Simulation was stopped or had no results — abort the loop
            self._auto_abort(
                f"Loop {cycle}/{self._auto_total}: simulation returned no results "
                "(was it stopped manually, or is the Simulation tab not logged in?)."
            )
            return

        # Build result dicts and match back to evolved candidates
        ROW_COL = {'passed': 0, 'sharpe': 6, 'fitness': 7,
                   'turnover': 8, 'subsharpe': 10, 'code': 14}
        matched: List[Tuple[float, Dict[str, str], str]] = []
        for row_data in row_results:
            try:
                if len(row_data) <= ROW_COL['code']:
                    continue
                code = str(row_data[ROW_COL['code']])
                if not code.strip():
                    continue
                result_dict = {
                    'passed':    'PASS' if int(row_data[ROW_COL['passed']]) >= 1 else 'FAIL',
                    'sharpe':    row_data[ROW_COL['sharpe']],
                    'fitness':   row_data[ROW_COL['fitness']],
                    'turnover':  row_data[ROW_COL['turnover']],
                    'subsharpe': row_data[ROW_COL['subsharpe']],
                    'code':      code,
                }
                rf = compute_real_fitness(result_dict)
                norm = _normalize_code(code)
                candidate = None
                for _sc, cand, ccode in self._results:
                    if _normalize_code(ccode) == norm:
                        candidate = cand
                        break
                if candidate is not None:
                    matched.append((rf, candidate, code))
            except Exception:
                continue

        matched.sort(key=lambda x: -x[0])

        # Update fitness map so table shows real BT scores
        self._bt_fitness_map = {_normalize_code(code): rf for rf, _, code in matched}
        top_k = self._top_k_spin.value()
        self._populate_table(self._results[:top_k])

        n_matched = len(matched)
        top_rf = matched[0][0] if matched else 0.0
        self._auto_status_label.setText(
            f"✅ Loop {cycle}/{self._auto_total} done — "
            f"{n_matched} matched, top fitness {top_rf:.3f}"
        )

        # Decrement and decide whether to continue
        self._auto_remaining -= 1
        if self._auto_stop_requested:
            self._auto_finish(stopped=True)
            return

        if self._auto_remaining > 0:
            self._auto_seed_and_continue(matched)
        else:
            self._auto_finish()

    def _auto_seed_and_continue(self, matched: List[Tuple[float, Dict[str, str], str]]):
        """Seed the next generation from the best simulation results and evolve."""
        top_k = self._top_k_spin.value()
        seeds = [cand for _, cand, _ in matched[:top_k]]
        known_real_fitness = {
            _candidate_key(cand): rf for rf, cand, _ in matched[:top_k]
        }
        self._seed_candidates = seeds
        if seeds:
            top_rf = matched[0][0]
            self._seed_label.setText(
                f"🌱 Auto loop seed: {len(seeds)} candidate(s) · top fitness {top_rf:.3f}"
            )
            self._seed_label.show()

        cycle = self._auto_total - self._auto_remaining + 1
        self._auto_status_label.setText(
            f"⏳ Loop {cycle}/{self._auto_total}: running evolution (seeded from real fitness)…"
        )
        self._start_evolution(seeds=seeds, known_real_fitness=known_real_fitness)

    def _auto_finish(self, stopped: bool = False):
        """Called when the auto loop finishes or stops gracefully."""
        self._auto_active = False
        self._auto_stop_requested = False
        self.auto_loop_mode_changed.emit(False)
        self._auto_btn.setText("▶▶  Start Auto Loop")
        self._auto_btn.setStyleSheet(
            "background-color: #e65100; color: white; font-weight: bold; padding: 7px 18px;"
        )
        self._run_btn.setEnabled(True)
        self._bt_btn.setEnabled(True)
        self._send_btn.setEnabled(bool(self._results))
        self._auto_loops_spin.setEnabled(True)
        self._auto_btn.setEnabled(True)
        if stopped:
            self._auto_status_label.setText(
                "Auto loop stopped after the current cycle. Results shown above."
            )
            QMessageBox.information(
                self, "Auto Loop Stopped",
                "The auto loop stopped after completing the current cycle.\n\n"
                "The current evolved candidates remain available above."
            )
        else:
            self._auto_status_label.setText(
                f"🏁 Auto loop complete — {self._auto_total} cycle(s) finished. "
                "Results shown above; use 'Send Selected → Simulation' to re-submit top candidates."
            )
            QMessageBox.information(
                self, "Auto Loop Complete",
                f"All {self._auto_total} auto loop cycle(s) finished.\n\n"
                "The evolved candidates table now shows real backtest fitness scores.\n"
                "Use 'Send Selected → Simulation' to re-submit the best candidates."
            )

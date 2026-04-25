#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Auto Evolution Engine for WorldQuant Brain Strategy Generator.

Implements a template-based genetic algorithm that evolves combinations of
placeholder values (e.g. {field}, {window}) for maximum coverage/diversity,
then ranks candidates and feeds them into the existing simulation workflow.

No simulation feedback is required — fitness is a deterministic diversity proxy.
"""

import re
import random
import itertools
from typing import Dict, List, Tuple, Any, Optional

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QTableWidget, QTableWidgetItem, QGroupBox, QFormLayout,
    QLineEdit, QSpinBox, QDoubleSpinBox, QMessageBox,
    QHeaderView, QAbstractItemView, QScrollArea, QCheckBox,
    QFrame, QSizePolicy,
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
    """

    def __init__(self,
                 template: str,
                 pools: Dict[str, List[str]],
                 pop_size: int = 40,
                 generations: int = 10,
                 mutation_rate: float = 0.4,
                 parent_ratio: float = 0.3,
                 diversity_weight: float = 0.7):
        self.template = template
        self.pools = pools
        self.pop_size = max(pop_size, 4)
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.parent_num = max(2, int(self.pop_size * parent_ratio))
        self.diversity_weight = diversity_weight

        template_phs = parse_placeholders(template)
        self.active_placeholders = [p for p in template_phs if p in pools]

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
        return self.diversity_weight * div + (1.0 - self.diversity_weight) * uniq

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
            "for maximum diversity. "
            "Click <i>Refresh Placeholders</i> whenever you edit the "
            "template, define value pools, configure the GA, then run."
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
        self._run_btn.setToolTip("Evolve candidates using the GA and rank by diversity.")
        self._run_btn.clicked.connect(self.run_evolution)
        action_bar.addWidget(self._run_btn)

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

        # ── Results table ──────────────────────────────────────────────
        results_group = QGroupBox("Evolved Candidates  (ranked by diversity score ↓)")
        results_layout = QVBoxLayout(results_group)

        self._results_table = QTableWidget(0, 4)
        self._results_table.setHorizontalHeaderLabels(
            ["✔", "Score", "Parameters", "Rendered Code"]
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
            3, QHeaderView.Stretch
        )
        self._results_table.setFont(QFont("Consolas", 11))
        self._results_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._results_table.setAlternatingRowColors(True)
        results_layout.addWidget(self._results_table)

        layout.addWidget(results_group, 1)

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
        )

        if not engine.active_placeholders:
            QMessageBox.warning(
                self, "No Active Placeholders",
                "None of the template's {placeholders} have a corresponding pool entry. "
                "Use Refresh Placeholders and fill in the pool fields."
            )
            return

        self._run_btn.setEnabled(False)
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
        self._progress_label.setText(
            f"Done — {len(results)} unique candidates, showing top {shown}."
        )
        self._run_btn.setEnabled(True)
        self._send_btn.setEnabled(bool(results))

    @Slot(str)
    def _on_error(self, msg: str):
        self._progress_label.setText(f"Error: {msg}")
        QMessageBox.critical(self, "Evolution Error", msg)
        self._run_btn.setEnabled(True)

    # ------------------------------------------------------------------
    # Results table
    # ------------------------------------------------------------------

    def _populate_table(self, results):
        self._results_table.setRowCount(0)
        for score, candidate, code in results:
            row = self._results_table.rowCount()
            self._results_table.insertRow(row)

            # Checkbox column
            cb = QCheckBox()
            cb.setChecked(True)
            cb_container = QWidget()
            cb_layout = QHBoxLayout(cb_container)
            cb_layout.addWidget(cb)
            cb_layout.setAlignment(Qt.AlignCenter)
            cb_layout.setContentsMargins(0, 0, 0, 0)
            self._results_table.setCellWidget(row, 0, cb_container)

            # Score
            score_item = QTableWidgetItem(f"{score:.3f}")
            score_item.setTextAlignment(Qt.AlignCenter)
            self._results_table.setItem(row, 1, score_item)

            # Compact parameter summary
            params_str = "  ".join(f"{k}={v}" for k, v in sorted(candidate.items()))
            params_item = QTableWidgetItem(params_str)
            params_item.setForeground(QColor("#555555"))
            self._results_table.setItem(row, 2, params_item)

            # Rendered code — store candidate dict for retrieval
            code_item = QTableWidgetItem(code.strip())
            code_item.setData(Qt.UserRole, candidate)
            self._results_table.setItem(row, 3, code_item)

    # ------------------------------------------------------------------
    # Send to simulation
    # ------------------------------------------------------------------

    def send_to_simulation(self):
        """Collect checked candidates and emit as strategy dicts."""
        settings = self.settings_widget.get_settings()
        base = {
            k: v for k, v in settings.items()
            if k in ("neutralization", "decay", "truncation", "delay",
                     "universe", "region")
        }

        strategies = []
        for row in range(self._results_table.rowCount()):
            cb_container = self._results_table.cellWidget(row, 0)
            if cb_container:
                cb = cb_container.findChild(QCheckBox)
                if cb and cb.isChecked():
                    code_item = self._results_table.item(row, 3)
                    if code_item:
                        strategy = base.copy()
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

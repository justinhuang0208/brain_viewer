from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTableWidget, QTableWidgetItem,
    QPushButton, QLabel, QMessageBox, QHeaderView, QComboBox, QDialog,
    QPlainTextEdit, QDialogButtonBox, QCheckBox, QMenu, QLineEdit,
    QStackedWidget
)
from PySide6.QtCore import Qt, QThread, Signal, QObject, Slot, QPoint
from PySide6.QtGui import QAction, QColor # Import QColor
import uuid

class BatchEditDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("批量調整參數")
        self.setMinimumWidth(300)
        layout = QVBoxLayout(self)

        # 選擇要調整的參數
        self.param_combo = QComboBox(self)
        # 排除 code 欄位
        self.param_combo.addItems([p for p in PARAM_COLUMNS if p != "code"])
        layout.addWidget(QLabel("選擇要調整的參數:"))
        layout.addWidget(self.param_combo)

        # 輸入新值區域（使用 QStackedWidget 來切換不同的輸入介面）
        self.value_stack = QStackedWidget(self)
        
        # 普通文字輸入
        self.value_input = QLineEdit(self)
        value_page = QWidget()
        value_layout = QVBoxLayout(value_page)
        value_layout.addWidget(QLabel("輸入新的值:"))
        value_layout.addWidget(self.value_input)
        self.value_stack.addWidget(value_page)
        
        # Delay 下拉選單
        self.delay_combo = QComboBox(self)
        self.delay_combo.addItems(DELAY_OPTIONS)
        delay_page = QWidget()
        delay_layout = QVBoxLayout(delay_page)
        delay_layout.addWidget(QLabel("選擇延遲值:"))
        delay_layout.addWidget(self.delay_combo)
        self.value_stack.addWidget(delay_page)
        
        # Neutralization 下拉選單
        self.neutralization_combo = QComboBox(self)
        self.neutralization_combo.addItems(NEUTRALIZATION_OPTIONS)
        neutralization_page = QWidget()
        neutralization_layout = QVBoxLayout(neutralization_page)
        neutralization_layout.addWidget(QLabel("選擇中性化選項:"))
        neutralization_layout.addWidget(self.neutralization_combo)
        self.value_stack.addWidget(neutralization_page)
        
        # Universe 下拉選單
        self.universe_combo = QComboBox(self)
        self.universe_combo.addItems(UNIVERSE_OPTIONS)
        universe_page = QWidget()
        universe_layout = QVBoxLayout(universe_page)
        universe_layout.addWidget(QLabel("選擇 Universe:"))
        universe_layout.addWidget(self.universe_combo)
        self.value_stack.addWidget(universe_page)
        
        layout.addWidget(self.value_stack)
        
        # 連接參數選擇的信號
        self.param_combo.currentTextChanged.connect(self.on_param_changed)
        
        # 僅套用到已選取的行
        self.selected_only = QCheckBox("僅套用到已選取的行", self)
        self.selected_only.setChecked(True)
        layout.addWidget(self.selected_only)

        # 按鈕
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel, self)
        layout.addWidget(buttons)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)

    def on_param_changed(self, param):
        """當選擇的參數改變時，切換到對應的輸入介面"""
        if param == "delay":
            self.value_stack.setCurrentIndex(1)
        elif param == "neutralization":
            self.value_stack.setCurrentIndex(2)
        elif param == "universe":
            self.value_stack.setCurrentIndex(3)
        else:
            self.value_stack.setCurrentIndex(0)

    def get_values(self):
        param = self.param_combo.currentText()
        if param == "delay":
            value = self.delay_combo.currentText()
        elif param == "neutralization":
            value = self.neutralization_combo.currentText()
        elif param == "universe":
            value = self.universe_combo.currentText()
        else:
            value = self.value_input.text()
        
        return {
            'param': param,
            'value': value,
            'selected_only': self.selected_only.isChecked()
        }

from PySide6.QtCore import Qt, QThread, Signal, QObject, Slot
import ast
import csv
import logging
import requests
import json
import time
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Lock # Import Lock for thread-safe writing

PARAM_COLUMNS = [
    "code", "decay", "delay", "neutralization", "region", "truncation", "universe"
]
# Add a placeholder for the checkbox column default value (None, as it's handled differently)
DEFAULT_VALUES = [None, "", 4, 1, "SUBINDUSTRY", "USA", 0.08, "TOP3000"]

DELAY_OPTIONS = ["1", "0"]
NEUTRALIZATION_OPTIONS = ["NONE", "MARKET", "SECTOR", "INDUSTRY", "SUBINDUSTRY"]
UNIVERSE_OPTIONS = ["TOP3000", "TOP1000", "TOP500", "TOP200", "TOPSP500"]

class CodeEditDialog(QDialog):
    def __init__(self, code_text, parent=None):
        super().__init__(parent)
        self.setWindowTitle("編輯 Code")
        self.resize(600, 300)
        layout = QVBoxLayout(self)
        self.editor = QPlainTextEdit(self)
        self.editor.setPlainText(code_text)
        layout.addWidget(self.editor)
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel, self)
        layout.addWidget(buttons)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)

    def get_code(self):
        return self.editor.toPlainText()

class SimulationWidget(QWidget):
    def update_single_simulation_progress(self, uuid: str, percentage: int):
        """根據 uuid 更新對應行的進度 (%) 欄"""
        progress_col = self.progress_col_index
        found = False
        for row in range(self.table.rowCount()):
            chk_item = self.table.item(row, 0)
            if chk_item and chk_item.data(Qt.UserRole) == uuid:
                progress_item = self.table.item(row, progress_col)
                if not progress_item:
                    progress_item = QTableWidgetItem()
                    self.table.setItem(row, progress_col, progress_item)
                progress_item.setText(f"{percentage}%")
                progress_item.setTextAlignment(Qt.AlignCenter)
                found = True
                break
        if not found:
            print(f"警告: 在表格中未找到與進度更新 uuid '{uuid}' 匹配的行")
    def _reset_table_colors(self):
        """重設所有表格儲存格（包含 QTableWidgetItem 與 cellWidget）的背景顏色為預設白色"""
        for row in range(self.table.rowCount()):
            for col in range(self.table.columnCount()):
                cell_item = self.table.item(row, col)
                widget = self.table.cellWidget(row, col)
                if widget:
                    widget.setStyleSheet("background-color: white;")
                elif cell_item:
                    cell_item.setBackground(Qt.white)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("模擬參數編輯與執行")
        # self.process = None # Removed QProcess attribute

        self.active_wq_session = None  # 儲存已登入的 session

        layout = QVBoxLayout(self)
        ...
        # 進度顯示
        self.progress_label = QLabel("尚未開始模擬")
        layout.addWidget(self.progress_label)

        # 參數表格 - 增加一欄給 checkbox
        # 新增進度欄: "選取" + "進度 (%)" + 其餘 PARAM_COLUMNS
        self.progress_col_index = 1  # 進度欄固定為索引 1
        self.table = QTableWidget(0, len(PARAM_COLUMNS) + 2) # +1 for checkbox, +1 for progress
        header_labels = ["選取", "進度 (%)"] + PARAM_COLUMNS
        self.table.setHorizontalHeaderLabels(header_labels)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        # Make the checkbox column narrower
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        # 設定進度欄寬
        self.table.horizontalHeader().setSectionResizeMode(self.progress_col_index, QHeaderView.ResizeToContents)
        # 設定 decay 欄寬
        try:
            decay_col_index = PARAM_COLUMNS.index("decay") + 2  # +2: checkbox+progress
            self.table.horizontalHeader().setSectionResizeMode(decay_col_index, QHeaderView.ResizeToContents)
        except ValueError:
            pass
        # 設定 delay, region, truncation, universe, neutralization 欄寬
        for col_name in ["delay", "region", "truncation", "universe", "neutralization"]:
            try:
                col_index = PARAM_COLUMNS.index(col_name) + 2
                self.table.horizontalHeader().setSectionResizeMode(col_index, QHeaderView.ResizeToContents)
            except ValueError:
                pass
        layout.addWidget(self.table)
        
        # 添加右鍵選單事件處理
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.show_context_menu)

        # 按鈕區
        btn_layout = QHBoxLayout() # Combine buttons in one layout

        # 資料編輯按鈕與下拉選單
        self.edit_btn = QPushButton("新增參數")
        self.edit_btn.clicked.connect(self.add_row)
        btn_layout.addWidget(self.edit_btn)

        # 將原本 edit_menu 的三個 action 設為實例屬性，供右鍵選單使用
        self.dup_action = QAction("複製選取", self)
        self.del_action = QAction("刪除選取", self)
        self.batch_edit_action = QAction("批量調整", self)

        # 新增：刪除全部資料按鈕
        self.del_all_btn = QPushButton("刪除全部資料")
        btn_layout.addWidget(self.del_all_btn)

        # 模擬按鈕
        self.sim_btn = QPushButton("執行模擬")
        btn_layout.addWidget(self.sim_btn) # Add sim button to the same layout

        # 檢查登入按鈕
        self.check_login_btn = QPushButton("檢查登入")
        btn_layout.addWidget(self.check_login_btn)

        layout.addLayout(btn_layout) # Add the combined button layout

        # 連接信號
        self.dup_action.triggered.connect(self.duplicate_selected_rows)
        self.del_action.triggered.connect(self.delete_selected_rows)
        self.batch_edit_action.triggered.connect(self.show_batch_edit_dialog)
        self.del_all_btn.clicked.connect(self.delete_all_rows) # 連接刪除全部按鈕
        self.sim_btn.clicked.connect(self.toggle_simulation) # 改為連接 toggle_simulation
        self.check_login_btn.clicked.connect(self.check_login_status) # Connect new button
        self.table.cellDoubleClicked.connect(self.handle_cell_double_clicked)
        self.simulation_thread = None
        self.simulation_worker = None
        self.is_simulating = False # 追蹤模擬狀態
        # Store the index of the 'code' column for faster lookup
        try:
            self._code_col_index = PARAM_COLUMNS.index('code') + 2 # +2 for checkbox+progress
        except ValueError:
            self._code_col_index = -1 # Should not happen if PARAM_COLUMNS is correct

    @Slot(str, list)
    def highlight_completed_row(self, uuid: str, row_data: list):
        """根據完成的 uuid 標示對應的表格行"""
        light_green = QColor("#e8f5e9")
        found = False
        for row in range(self.table.rowCount()):
            chk_item = self.table.item(row, 0)
            if chk_item and chk_item.data(Qt.UserRole) == uuid:
                for col in range(self.table.columnCount()):
                    cell_item = self.table.item(row, col)
                    if not cell_item:
                        widget = self.table.cellWidget(row, col)
                        if widget:
                            try:
                                widget.setStyleSheet("background-color: #e8f5e9;")
                            except AttributeError:
                                pass
                        else:
                            cell_item = QTableWidgetItem()
                            self.table.setItem(row, col, cell_item)
                            cell_item.setBackground(light_green)
                    else:
                        cell_item.setBackground(light_green)
                found = True
                break
        if not found:
            print(f"警告: 在表格中未找到與完成的 uuid '{uuid}' 匹配的行")

    @Slot(str, dict)
    def highlight_processing_row(self, uuid: str, simulation_data: dict):
        """根據開始處理的 uuid 標示對應的表格行為淺橘色"""
        light_orange = QColor("#fff3e0")
        found = False
        for row in range(self.table.rowCount()):
            chk_item = self.table.item(row, 0)
            if chk_item and chk_item.data(Qt.UserRole) == uuid:
                for col in range(self.table.columnCount()):
                    cell_item = self.table.item(row, col)
                    if not cell_item:
                        widget = self.table.cellWidget(row, col)
                        if widget:
                            try:
                                widget.setStyleSheet("background-color: #fff3e0;")
                            except AttributeError:
                                pass
                        else:
                            cell_item = QTableWidgetItem()
                            self.table.setItem(row, col, cell_item)
                            cell_item.setBackground(light_orange)
                    else:
                        cell_item.setBackground(light_orange)
                found = True
                break
        if not found:
            print(f"警告: 在表格中未找到與正在處理的 uuid '{uuid}' 匹配的行")

    @Slot(list) # Decorator to mark this as a slot
    def load_strategies_from_generator(self, strategies: list):
        """接收來自 Generator 的策略列表並載入表格 (Slot)"""
        if not strategies:
            QMessageBox.warning(self, "無數據", "從生成器接收到空的策略列表。")
            return

        # 將策略字典列表轉換為 DataFrame
        try:
            df = pd.DataFrame(strategies)
            # 確保列名與 PARAM_COLUMNS 匹配 (或進行必要的映射)
            # 目前假設 generator 產生的字典鍵與 PARAM_COLUMNS 匹配

            # 檢查必要的 'code' 列是否存在
            if 'code' not in df.columns:
                 QMessageBox.warning(self, "缺少欄位", "從生成器匯入的數據缺少關鍵的 'code' 欄位，無法載入。")
                 return

            print(f"接收到 {len(df)} 筆策略，準備追加到模擬器表格...")
            self.load_parameters_from_dataframe(df)
            QMessageBox.information(self, "匯入成功", f"已成功將 {len(df)} 筆策略追加到表格中。")

        except Exception as e:
            QMessageBox.critical(self, "匯入錯誤", f"將策略列表轉換為 DataFrame 或載入時出錯: {e}")
            print(f"將策略列表轉換為 DataFrame 或載入時出錯: {e}")

    def load_parameters_from_dataframe(self, df: pd.DataFrame):
        """從 DataFrame 載入參數到表格中（追加模式）"""
        # 獲取當前表格的行數作為起始位置
        start_row = self.table.rowCount()

        # 確保必要的欄位存在，如果不存在則使用預設值或跳過
        required_cols = set(PARAM_COLUMNS)
        available_cols = set(df.columns)

        missing_cols = required_cols - available_cols
        if missing_cols:
            print(f"警告: 傳入的 DataFrame 缺少以下欄位: {', '.join(missing_cols)}")
            # 可以選擇在這裡返回或繼續處理（使用預設值）
            # 如果缺少 'code'，可能無法繼續
            if 'code' in missing_cols:
                 QMessageBox.warning(self, "缺少欄位", f"匯入的數據缺少關鍵的 'code' 欄位，無法載入。")
                 return

        print(f"正在從 DataFrame 載入 {len(df)} 筆參數...")
        for _, row_series in df.iterrows():
            row_data = []
            for col_name in PARAM_COLUMNS:
                if col_name in row_series:
                    row_data.append(row_series[col_name])
                else:
                    # 如果欄位缺失，嘗試從 DEFAULT_VALUES 獲取預設值
                    try:
                        default_index = PARAM_COLUMNS.index(col_name) + 1 # +1 because DEFAULT_VALUES includes None for checkbox
                        row_data.append(DEFAULT_VALUES[default_index])
                        print(f"警告: 欄位 '{col_name}' 缺失，使用預設值: {DEFAULT_VALUES[default_index]}")
                    except (ValueError, IndexError):
                        row_data.append("") # Fallback to empty string if no default found
                        print(f"警告: 欄位 '{col_name}' 缺失且找不到預設值，使用空字串。")

            self.add_row(data=row_data)
        print(f"已成功載入 {self.table.rowCount()} 筆參數到表格。")

    def add_row(self, data=None): # Allow passing data for duplication
        row = self.table.rowCount()
        self.table.insertRow(row)

        # 產生 UUID 並存入 checkbox 欄
        row_uuid = uuid.uuid4().hex
        chk_item = QTableWidgetItem()
        chk_item.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
        chk_item.setCheckState(Qt.Unchecked)
        chk_item.setData(Qt.UserRole, row_uuid)
        self.table.setItem(row, 0, chk_item)

        # 新增進度欄 (預設為 "-")
        progress_item = QTableWidgetItem("-")
        progress_item.setTextAlignment(Qt.AlignCenter)
        self.table.setItem(row, 1, progress_item)

        # Use provided data or defaults for other columns
        values_to_set = data if data else DEFAULT_VALUES[1:] # Skip the None placeholder for checkbox

        for col_idx, val in enumerate(values_to_set):
            table_col = col_idx + 2 # +2: 跳過 checkbox 與進度欄
            param_key = PARAM_COLUMNS[col_idx]

            if param_key == "delay":
                combo = QComboBox()
                combo.addItems(DELAY_OPTIONS)
                current_val = str(val) if val is not None else str(DEFAULT_VALUES[table_col-1])
                combo.setCurrentText(current_val)
                self.table.setCellWidget(row, table_col, combo)
            elif param_key == "neutralization":
                combo = QComboBox()
                combo.addItems(NEUTRALIZATION_OPTIONS)
                current_val = str(val) if val is not None else str(DEFAULT_VALUES[table_col-1])
                combo.setCurrentText(current_val)
                self.table.setCellWidget(row, table_col, combo)
            elif param_key == "universe":
                combo = QComboBox()
                combo.addItems(UNIVERSE_OPTIONS)
                current_val = str(val) if val is not None else str(DEFAULT_VALUES[table_col-1])
                combo.setCurrentText(current_val)
                self.table.setCellWidget(row, table_col, combo)
            else:
                current_val = str(val).strip() if val is not None else str(DEFAULT_VALUES[table_col-1]).strip()
                item = QTableWidgetItem(current_val)
                self.table.setItem(row, table_col, item)

    def show_batch_edit_dialog(self):
        """顯示批量調整對話框"""
        dlg = BatchEditDialog(self)
        if dlg.exec() == QDialog.Accepted:
            values = dlg.get_values()
            param = values['param']
            new_value = values['value']
            selected_only = values['selected_only']

            # 獲取參數所在的列
            param_col = PARAM_COLUMNS.index(param) + 2  # +2: 跳過「選取」和「進度 (%)」兩欄

            # 驗證輸入值
            if param == "delay":
                if new_value not in DELAY_OPTIONS:
                    QMessageBox.warning(self, "無效的值", f"延遲值必須是以下其中之一: {', '.join(DELAY_OPTIONS)}")
                    return
            elif param == "neutralization":
                if new_value not in NEUTRALIZATION_OPTIONS:
                    QMessageBox.warning(self, "無效的值", f"中性化選項必須是以下其中之一: {', '.join(NEUTRALIZATION_OPTIONS)}")
                    return
            elif param == "universe":
                if new_value not in UNIVERSE_OPTIONS:
                    QMessageBox.warning(self, "無效的值", f"universe 選項必須是以下其中之一: {', '.join(UNIVERSE_OPTIONS)}")
                    return
            elif param == "decay":
                try:
                    int(new_value)
                except ValueError:
                    QMessageBox.warning(self, "無效的值", "decay 必須是整數")
                    return
            elif param == "truncation":
                try:
                    float(new_value)
                except ValueError:
                    QMessageBox.warning(self, "無效的值", "truncation 必須是數字")
                    return

            rows_updated = 0
            for row in range(self.table.rowCount()):
                # 檢查是否只更新選取的行
                if selected_only:
                    chk_item = self.table.item(row, 0)
                    if not chk_item or chk_item.checkState() != Qt.Checked:
                        continue

                # 根據參數類型更新值
                if param in ["delay", "neutralization", "universe"]:
                    combo = self.table.cellWidget(row, param_col)
                    if combo:
                        combo.setCurrentText(new_value)
                        rows_updated += 1
                else:
                    item = self.table.item(row, param_col)
                    if not item:
                        item = QTableWidgetItem()
                        self.table.setItem(row, param_col, item)
                    item.setText(new_value)
                    rows_updated += 1

            QMessageBox.information(self, "更新完成", f"已更新 {rows_updated} 筆資料")

    def handle_cell_double_clicked(self, row, col): # Correct indent
        if col == 0: # Ignore double clicks on checkbox column
             return
        param_col_index = col - 2  # 修正：跳過「選取」和「進度 (%)」兩欄
        if param_col_index >= 0 and PARAM_COLUMNS[param_col_index] == "code":
            item = self.table.item(row, col) # Use original 'col' for table access
            code_text = item.text() if item else ""
            dlg = CodeEditDialog(code_text, self)
            if dlg.exec() == QDialog.Accepted:
                new_code = dlg.get_code()
                if not item:
                    item = QTableWidgetItem()
                    self.table.setItem(row, col, item) # Use original 'col'
                item.setText(new_code)

    def duplicate_selected_rows(self):
        rows_to_duplicate = []
        for row in range(self.table.rowCount()):
            chk_item = self.table.item(row, 0)
            if chk_item and chk_item.checkState() == Qt.Checked:
                row_data = []
                for col in range(2, self.table.columnCount()): # Start from col 2 (skip 進度 %)
                    widget = self.table.cellWidget(row, col)
                    if widget: # Handle widgets (ComboBox)
                         if isinstance(widget, QComboBox):
                             row_data.append(widget.currentText())
                         # Add other widget types here if needed
                    else: # Handle QTableWidgetItem
                        item = self.table.item(row, col)
                        row_data.append(item.text() if item else "")
                rows_to_duplicate.append(row_data)

        for data in rows_to_duplicate:
            self.add_row(data=data) # Pass data to add_row

    def delete_selected_rows(self):
        rows_to_delete = []
        for row in range(self.table.rowCount()):
            chk_item = self.table.item(row, 0)
            if chk_item and chk_item.checkState() == Qt.Checked:
                rows_to_delete.append(row)

        # Delete rows in reverse order to avoid index issues
        for row in sorted(rows_to_delete, reverse=True):
            self.table.removeRow(row)

    def delete_all_rows(self):
        """刪除表格中的所有資料"""
        if self.table.rowCount() == 0:
            QMessageBox.information(self, "提示", "表格已經是空的。")
            return

        reply = QMessageBox.question(self, '確認刪除',
                                       "您確定要刪除表格中的所有資料嗎？此操作無法復原。",
                                       QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

        if reply == QMessageBox.Yes:
            self.table.setRowCount(0)
            QMessageBox.information(self, "成功", "已刪除所有資料。")

    def show_context_menu(self, position: QPoint):
        """顯示右鍵選單"""
        # 獲取選取的項目
        selected_rows = set()
        for item in self.table.selectedItems():
            selected_rows.add(item.row())

        if not selected_rows:
            return

        # 檢查選取的行是否都已勾選
        all_checked = True
        for row in selected_rows:
            item = self.table.item(row, 0)  # 第一欄是 checkbox
            if item and item.checkState() != Qt.Checked:
                all_checked = False
                break

        # 創建選單並根據當前狀態設定文字
        context_menu = QMenu(self)
        check_action = context_menu.addAction("取消勾選" if all_checked else "勾選")

        # 加入複製/刪除/批量調整功能到右鍵選單
        # 只有在有選取列時才啟用
        self.dup_action.setEnabled(bool(selected_rows))
        self.del_action.setEnabled(bool(selected_rows))
        self.batch_edit_action.setEnabled(bool(selected_rows))
        context_menu.addAction(self.dup_action)
        context_menu.addAction(self.del_action)
        context_menu.addAction(self.batch_edit_action)

        # 顯示選單
        action = context_menu.exec_(self.table.mapToGlobal(position))

        # 處理選單動作
        if action == check_action:
            new_state = Qt.Unchecked if all_checked else Qt.Checked
            for row in selected_rows:
                item = self.table.item(row, 0)  # 第一欄是 checkbox
                if item:
                    item.setCheckState(new_state)

    def get_parameters(self):
        params = []
        for row in range(self.table.rowCount()):
            entry = {}
            chk_item = self.table.item(row, 0)
            if chk_item is not None:
                row_uuid = chk_item.data(Qt.UserRole)
            else:
                row_uuid = None
            entry['uuid'] = row_uuid
            for col_idx, key in enumerate(PARAM_COLUMNS):
                table_col = col_idx + 2 # 修正：考慮「選取」和「進度 (%)」兩欄
                if key == "delay":
                    widget = self.table.cellWidget(row, table_col)
                    entry[key] = int(widget.currentText()) if widget else 1
                elif key == "neutralization":
                    widget = self.table.cellWidget(row, table_col)
                    entry[key] = widget.currentText() if widget else "SUBINDUSTRY"
                elif key == "universe":
                    widget = self.table.cellWidget(row, table_col)
                    entry[key] = widget.currentText() if widget else "TOP3000"
                else:
                    val = self.table.item(row, table_col)
                    if val is None:
                        entry[key] = ""
                    else:
                        text = val.text()
                        if key in ["decay"]:
                            try:
                                entry[key] = int(text)
                            except Exception:
                                entry[key] = 0
                        elif key == "truncation":
                            try:
                                entry[key] = float(text)
                            except Exception:
                                entry[key] = 0.0
                        else:
                            entry[key] = text
            params.append(entry)
        return params

    def toggle_simulation(self):
        """根據模擬狀態啟動或停止模擬"""
        if not self.is_simulating:
            self.start_simulation_thread()
        else:
            self.stop_simulation_thread()

    def stop_simulation_thread(self):
        """請求停止正在執行的模擬執行緒"""
        if self.simulation_thread and self.simulation_thread.isRunning() and self.simulation_worker:
            print("請求停止模擬...") # Debug message
            self.progress_label.setText("正在請求停止模擬...")
            self.simulation_worker.request_stop() # 呼叫 Worker 的停止方法
            # 禁用按鈕，防止重複點擊，直到 finished 信號觸發重置
            self.sim_btn.setEnabled(False)
            
            # 重置所有欄位的背景顏色為白色
            self._reset_table_colors()
            # 重設所有進度欄
            for row in range(self.table.rowCount()):
                progress_item = self.table.item(row, self.progress_col_index)
                if progress_item:
                    progress_item.setText("-")
        else:
            print("無法停止：沒有模擬正在執行或 Worker 不存在。")
            # 如果沒有在執行，確保 UI 狀態正確
            if not self.is_simulating:
                 self.sim_btn.setText("執行模擬")
                 self.sim_btn.setEnabled(True)
                 self.edit_btn.setEnabled(True)
                 self.check_login_btn.setEnabled(True)

    def start_simulation_thread(self):
        # --- 修復閃退：檢查舊執行緒是否仍在運行 ---
        if self.simulation_thread and self.simulation_thread.isRunning():
            QMessageBox.warning(self, "操作過快", "上一個模擬執行緒仍在清理中，請稍後再試。")
            return
        # --- 修復結束 ---
    
        self._reset_table_colors()  # 新增：重置所有表格顏色
    
        # 重設所有進度欄
        for row in range(self.table.rowCount()):
            progress_item = self.table.item(row, self.progress_col_index)
            if not progress_item:
                progress_item = QTableWidgetItem("-")
                progress_item.setTextAlignment(Qt.AlignCenter)
                self.table.setItem(row, self.progress_col_index, progress_item)
            else:
                progress_item.setText("-")
    
        params = self.get_parameters()
        if not params:
            QMessageBox.warning(self, "無參數", "請至少新增一組模擬參數")
            return
    
        # 更新狀態和 UI
        self.is_simulating = True
        self.sim_btn.setText("停止模擬")
        self.edit_btn.setEnabled(False) # 禁用編輯按鈕
        self.check_login_btn.setEnabled(False) # 禁用檢查登入按鈕
        # self.sim_btn.setEnabled(False) # 保持按鈕啟用以便停止
        self.progress_label.setText("模擬進行中...")
    
        # 創建執行緒和 Worker
        self.simulation_thread = QThread()
        self.simulation_worker = SimulationWorker(params, session=self.active_wq_session)
        self.simulation_worker.moveToThread(self.simulation_thread)
    
        # 連接信號和槽
        self.simulation_worker.progress_updated.connect(self.update_progress_label)
        self.simulation_worker.error_occurred.connect(self.handle_simulation_error)
        self.simulation_worker.finished.connect(self.handle_simulation_finished)
        self.simulation_thread.started.connect(self.simulation_worker.run)
        self.simulation_thread.finished.connect(self.simulation_thread.deleteLater) # 清理執行緒
        self.simulation_worker.finished.connect(self.simulation_thread.quit)
        self.simulation_worker.finished.connect(self.simulation_worker.deleteLater) # 清理 Worker
        # Connect the new signal for row completion
        self.simulation_worker.simulation_row_completed.connect(self.highlight_completed_row)
        # Connect the new signal for row starting
        self.simulation_worker.simulation_row_started.connect(self.highlight_processing_row) # Add this connection
        # 連接個別進度信號
        self.simulation_worker.single_simulation_progress.connect(self.update_single_simulation_progress)
    
        # 啟動執行緒
        self.simulation_thread.start()

    def update_progress_label(self, message):
        self.progress_label.setText(message)

    def handle_simulation_error(self, error_message):
        self._reset_table_colors()  # 重設所有表格顏色
        # 重設所有進度欄
        for row in range(self.table.rowCount()):
            progress_item = self.table.item(row, self.progress_col_index)
            if progress_item:
                progress_item.setText("-")
        # 若為登入/憑證相關錯誤，清除 session
        if any(x in error_message for x in ["憑證", "401", "Unauthorized", "驗證", "expired", "過期", "登入失敗"]):
            self.active_wq_session = None
        self.progress_label.setText("模擬失敗")
        QMessageBox.critical(self, "模擬失敗", error_message)
        # --- 修復閃退：先重置狀態再啟用按鈕 ---
        self.is_simulating = False
        # 重置 UI 狀態
        self.sim_btn.setText("執行模擬")
        self.sim_btn.setEnabled(True)
        self.edit_btn.setEnabled(True)
        self.check_login_btn.setEnabled(True)
        # --- 修復結束 ---

    def handle_simulation_finished(self):
        # 檢查是否是因為停止請求而結束
        stopped_early = self.simulation_worker and self.simulation_worker.stop_requested
    
        # 重設所有表格顏色為預設（白色）
        self._reset_table_colors()
        # 重設所有進度欄
        for row in range(self.table.rowCount()):
            progress_item = self.table.item(row, self.progress_col_index)
            if progress_item:
                progress_item.setText("-")
    
        if stopped_early:
            self.progress_label.setText("模擬已停止")
            QMessageBox.warning(self, "模擬停止", "模擬已被使用者手動停止。")
        else:
            self.progress_label.setText("模擬完成")
            QMessageBox.information(self, "模擬完成", "模擬已成功完成！請檢查 data 資料夾中的 CSV 和 LOG 檔案。")
    
        # --- 修復閃退：先重置狀態再啟用按鈕 ---
        self.is_simulating = False
        self.simulation_thread = None
        self.simulation_worker = None
        # 重置 UI 狀態
        self.sim_btn.setText("執行模擬")
        self.sim_btn.setEnabled(True)
        self.edit_btn.setEnabled(True)
        self.check_login_btn.setEnabled(True)
        # --- 修復結束 ---
    
        # 只有在模擬正常完成時才詢問是否清空表格
        if not stopped_early:
            reply = QMessageBox.question(self, '清空表格?',
                                           "模擬已完成，您想要清空模擬參數表格嗎？",
                                           QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
    
            if reply == QMessageBox.Yes:
                self.table.setRowCount(0)
                # 更新標籤，即使不清空也要顯示完成
                self.progress_label.setText("模擬完成 (表格已清空)")
            else:
                # 更新標籤，即使不清空也要顯示完成
                self.progress_label.setText("模擬完成 (表格保留)")
        # 如果是手動停止，標籤已設為 "模擬已停止"，無需再改

    def check_login_status(self):
        """在背景執行緒中檢查 WorldQuant Brain 的登入狀態"""
        # 禁用檢查按鈕
        self.check_login_btn.setEnabled(False)
        
        # 創建執行緒和 worker
        self.login_check_thread = QThread()
        self.login_check_worker = LoginCheckWorker()
        self.login_check_worker.moveToThread(self.login_check_thread)

        # 連接信號
        self.login_check_worker.login_success.connect(self.handle_login_success)
        self.login_check_worker.login_failed.connect(self.handle_login_failed)
        self.login_check_worker.validation_required.connect(self.handle_validation_required)
        self.login_check_worker.update_status.connect(self.progress_label.setText)
        self.login_check_worker.finished.connect(self.login_check_thread.quit)
        self.login_check_worker.finished.connect(self.login_check_worker.deleteLater)
        self.login_check_thread.finished.connect(self.login_check_thread.deleteLater)
        self.login_check_thread.finished.connect(lambda: self.check_login_btn.setEnabled(True))

        # 啟動執行緒
        self.login_check_thread.started.connect(self.login_check_worker.run)
        self.login_check_thread.start()

    @Slot(object)
    def handle_login_success(self, session):
        """處理登入成功的槽函數，儲存 session"""
        self.active_wq_session = session
        QMessageBox.information(self, "登入成功", "WorldQuant Brain 登入狀態正常。")
        self.progress_label.setText("登入狀態正常")

    @Slot(str)
    def handle_login_failed(self, error_message):
        """處理登入失敗的槽函數"""
        # 若為登入/憑證相關錯誤，清除 session
        if any(x in error_message for x in ["憑證", "401", "Unauthorized", "驗證", "expired", "過期", "登入失敗"]):
            self.active_wq_session = None
        QMessageBox.critical(self, "登入檢查失敗", error_message)
        self.progress_label.setText(f"登入檢查失敗: {error_message}")

    @Slot(str)
    def handle_validation_required(self, persona_url):
        """處理需要驗證的槽函數"""
        QMessageBox.warning(self, "需要驗證", f"登入需要生物驗證，請前往:\n{persona_url}")
        self.progress_label.setText("登入需要驗證")

# LoginCheckWorker class for handling login checks in background
class LoginCheckWorker(QObject):
    login_success = Signal(object)  # 改為可傳遞 session 物件
    login_failed = Signal(str)  # 傳遞錯誤訊息
    validation_required = Signal(str)  # 傳遞驗證 URL
    update_status = Signal(str)  # 用於更新進度標籤
    finished = Signal()

    def __init__(self, json_fn='credentials.json'):
        super().__init__()
        self.json_fn = json_fn

    def run(self):
        try:
            # 讀取憑證檔案
            with open(self.json_fn, 'r') as f:
                creds = json.load(f)
                email, password = creds['email'], creds['password']
        except FileNotFoundError:
            self.login_failed.emit(f"找不到憑證檔案 {self.json_fn}")
            self.finished.emit()
            return
        except Exception as e:
            self.login_failed.emit(f"讀取憑證檔案時發生錯誤: {e}")
            self.finished.emit()
            return

        # 建立 session 並執行登入請求
        session = requests.Session()
        session.auth = (email, password)
        
        try:
            self.update_status.emit("正在檢查登入狀態...")
            r = session.post('https://api.worldquantbrain.com/authentication', timeout=10)
            r.raise_for_status()

            response_json = r.json()
            if 'user' in response_json:
                self.login_success.emit(session)  # 傳遞 session
            elif 'inquiry' in response_json:
                # 需要生物驗證
                persona_url = f"{r.url}/persona?inquiry={response_json['inquiry']}"
                self.validation_required.emit(persona_url)
            else:
                # 其他登入失敗情況
                error_detail = response_json.get('detail', '未知錯誤')
                self.login_failed.emit(f"登入失敗: {error_detail}")

        except requests.exceptions.Timeout:
            self.login_failed.emit("檢查登入狀態時連線超時")
        except requests.exceptions.RequestException as e:
            self.login_failed.emit(f"檢查登入狀態時發生網路錯誤: {e}")
        except Exception as e:
            self.login_failed.emit(f"檢查登入狀態時發生未知錯誤: {e}")
        finally:
            self.finished.emit()

# Worker 類別，用於在背景執行緒中執行模擬
class SimulationWorker(QObject):
    finished = Signal()
    progress_updated = Signal(str)
    error_occurred = Signal(str)
    simulation_row_completed = Signal(str, list) # uuid, csv_row_data
    simulation_row_started = Signal(str, dict) # uuid, simulation_data
    single_simulation_progress = Signal(str, int) # uuid, percentage

    def __init__(self, params, session=None):
        super().__init__()
        self.params = params
        self.session = session  # 可選的 session
        self.stop_requested = False # 加入停止標誌

    def request_stop(self):
        """設置停止標誌"""
        self.stop_requested = True
        print("Worker 收到停止請求。") # Debug message

    def run(self):
        try:
            # Pass worker_ref and session during initialization
            wq = WQSession(worker_ref=self, existing_session=self.session)
            wq.simulate(self.params) # WQSession.simulate will check the flag

            # Emit finished signal only if not stopped prematurely
            # (or always emit, and let the handler check the flag)
            # Let's always emit finished, and handle the stop state in the widget
            self.finished.emit()
        except Exception as e:
            logging.exception("模擬執行緒錯誤") # 記錄詳細錯誤到日誌
            self.error_occurred.emit(f'模擬過程中發生錯誤: {type(e).__name__}: {e}')
            self.finished.emit() # 確保 finished 信號被發送

# 將 WQSession 移到類別外部，使其成為獨立的類別
class WQSession(requests.Session):
    def __init__(self, json_fn='credentials.json', worker_ref=None, existing_session=None):
        if existing_session is not None:
            # 直接複製 existing_session 的屬性
            super().__init__()
            self.__dict__.update(existing_session.__dict__)
            # 複製 cookies
            self.cookies = requests.cookies.cookiejar_from_dict(requests.utils.dict_from_cookiejar(existing_session.cookies))
            # 複製 headers
            self.headers = existing_session.headers.copy()
            # 複製 auth
            self.auth = getattr(existing_session, 'auth', None)
            # 其他必要屬性可依需求補充
            self.worker_ref = worker_ref
            self.json_fn = json_fn
            from datetime import datetime
            self.first_run = True
            self.csv_file = f"data/{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            self.log_file = self.csv_file.replace('.csv','.log')
            self.login_expired = False
            self.rows_processed = []
            self.latest_csv_file = None
            self._csv_lock = Lock()
            # 跳過 self.login()
        else:
            super().__init__()
            self.worker_ref = worker_ref # Assign worker_ref immediately
            for handler in logging.root.handlers:
                logging.root.removeHandler(handler)
            logging.basicConfig(encoding='utf-8', level=logging.INFO, format='%(asctime)s: %(message)s')
            self.json_fn = json_fn
            from datetime import datetime
            self.first_run = True
            self.csv_file = f"data/{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            self.log_file = self.csv_file.replace('.csv','.log')
            self.login()
            old_get, old_post = self.get, self.post
            def new_get(*args, **kwargs):
                try:    return old_get(*args, **kwargs)
                except: return new_get(*args, **kwargs)
            def new_post(*args, **kwargs):
                try:    return old_post(*args, **kwargs)
                except: return new_post(*args, **kwargs)
            self.get, self.post = new_get, new_post
            self.login_expired = False
            self.rows_processed = []
            self.latest_csv_file = None
            self._csv_lock = Lock() # Lock for thread-safe CSV writing

    def login(self):
        try:
            with open(self.json_fn, 'r') as f:
                creds = json.loads(f.read())
                email, password = creds['email'], creds['password']
                self.auth = (email, password)
                r = self.post('https://api.worldquantbrain.com/authentication')
            if 'user' not in r.json():
                if 'inquiry' in r.json():
                    # 在背景執行緒中無法直接顯示 QMessageBox，需要透過信號通知主執行緒
                    if self.worker_ref:
                        self.worker_ref.error_occurred.emit(f"需要驗證: 請先至 {r.url}/persona?inquiry={r.json()['inquiry']} 完成生物驗證")
                    self.login_expired = True # 標記登入失敗
                    return
                else:
                    if self.worker_ref:
                        self.worker_ref.error_occurred.emit(f'登入失敗: WARNING! {r.json()}')
                    self.login_expired = True # 標記登入失敗
                    return
            logging.info('Logged in to WQBrain!')
        except FileNotFoundError:
            if self.worker_ref:
                self.worker_ref.error_occurred.emit(f"登入失敗: 找不到憑證檔案 {self.json_fn}")
            self.login_expired = True
        except Exception as e:
            if self.worker_ref:
                self.worker_ref.error_occurred.emit(f"登入失敗: {type(e).__name__}: {e}")
            self.login_expired = True

    def simulate(self, data):
        if self.login_expired: # 如果登入失敗，直接返回
             return []

        self.rows_processed = []
        self.completed_count = 0
        self.total_count = len(data)
        self.total_rows = len(data)

    # Method to process a single simulation, designed to be run in a thread
    def _process_single_simulation(self, simulation):
        if self.login_expired: return None # Return None if login failed

        # Get thread name for logging (optional but helpful for debugging)
        try:
            from threading import current_thread
            thread = current_thread().name
        except ImportError:
            thread = "worker"

        try: # Wrap the entire simulation logic in a try block
            # 將 simulation_row_started.emit 的時機延後到實際開始模擬（API 請求前）
            alpha = simulation['code'].strip()
            delay = simulation.get('delay', 1)
            universe = simulation.get('universe', 'TOP3000')
            truncation = simulation.get('truncation', 0.1)
            region = simulation.get('region', 'USA')
            decay = simulation.get('decay', 6)
            neutralization = simulation.get('neutralization', 'SUBINDUSTRY').upper()
            pasteurization = simulation.get('pasteurization', 'ON')
            nan = simulation.get('nanHandling', 'OFF')
            row_uuid = simulation.get('uuid', None)
            logging.info(f"{thread} -- Simulating alpha: {alpha}")
            # --- 修復 429：加入重試邏輯 (POST) ---
            max_retries = 3
            retry_delay = 15 # seconds
            for attempt in range(max_retries):
                # 檢查停止請求
                if self.worker_ref and self.worker_ref.stop_requested:
                    logging.info(f"{thread} -- 偵測到停止請求，取消模擬請求。")
                    return {'uuid': row_uuid, 'error': '模擬被手動停止', 'alpha': alpha}

                try:
                    # 在真正開始發送 API 請求前才 emit started
                    if self.worker_ref:
                        self.worker_ref.simulation_row_started.emit(row_uuid, simulation)
                    r = self.post('https://api.worldquantbrain.com/simulations', json={
                        'regular': alpha,
                        'type': 'REGULAR',
                        'settings': {
                            "nanHandling":nan,
                            "instrumentType":"EQUITY",
                            "delay":delay,
                            "universe":universe,
                            "truncation":truncation,
                            "unitHandling":"VERIFY",
                            "pasteurization":pasteurization,
                            "region":region,
                            "language":"FASTEXPR",
                            "decay":decay,
                            "neutralization":neutralization,
                            "visualization":False
                        }
                    })
                    r.raise_for_status() # Check for HTTP errors (including 429 initially)
                    nxt = r.headers['Location']
                    break # Success, exit retry loop
                except requests.exceptions.HTTPError as http_err:
                    if http_err.response.status_code == 429:
                        retry_msg = f"請求過多 (429)，等待 {retry_delay} 秒後重試 ({attempt + 1}/{max_retries})..."
                        logging.warning(f"{thread} -- {retry_msg}")
                        # --- 更新進度標籤 ---
                        if self.worker_ref:
                            self.worker_ref.progress_updated.emit(retry_msg)
                        # --- 更新結束 ---
                        if attempt < max_retries - 1:
                            # 等待時檢查停止請求
                            for _ in range(retry_delay * 5): # Check every 0.2 seconds
                                if self.worker_ref and self.worker_ref.stop_requested:
                                    logging.info(f"{thread} -- 偵測到停止請求，中斷重試等待。")
                                    return {'error': '模擬被手動停止', 'alpha': alpha}
                                time.sleep(0.2)
                            continue # Continue to the next retry attempt
                        else:
                            logging.error(f"{thread} -- 達到最大重試次數 ({max_retries})，放棄模擬請求。")
                            error_msg = f"API 請求過多 (429)，達到最大重試次數 ({max_retries})。"
                            if self.worker_ref:
                                self.worker_ref.error_occurred.emit(error_msg) # Emit error signal
                            return {'error': error_msg, 'alpha': alpha}
                    else:
                        # Handle other HTTP errors
                        logging.error(f"{thread} -- Simulation request failed with HTTP error: {http_err}")
                        if self.worker_ref:
                            self.worker_ref.error_occurred.emit(f"模擬請求失敗: {http_err}")
                        return {'error': f"模擬請求失敗: {http_err}", 'alpha': alpha}
                except requests.exceptions.RequestException as req_err: # Handle non-HTTP request errors (e.g., connection error)
                    logging.error(f"{thread} -- Simulation request failed: {req_err}")
                    if self.worker_ref:
                        self.worker_ref.error_occurred.emit(f"模擬請求失敗: {req_err}")
                    return {'error': f"模擬請求失敗: {req_err}", 'alpha': alpha}
                except Exception as e: # Catch other potential errors like missing headers or JSON parsing
                    logging.error(f"{thread} -- Error during simulation request: {e}")
                    error_msg = f"模擬請求時發生未知錯誤: {e}"
                    if 'credentials' in str(e) or (hasattr(r, 'json') and 'credentials' in r.json().get('detail', '')): # Check if it's a credential error
                        self.login_expired = True
                        error_msg = "登入憑證可能已過期，請重新登入或檢查憑證。"
                        if self.worker_ref:
                            self.worker_ref.error_occurred.emit(error_msg)
                        return {'error': error_msg, 'alpha': alpha} # Return error
                    else:
                         if self.worker_ref:
                            self.worker_ref.error_occurred.emit(error_msg)
                    return {'error': error_msg, 'alpha': alpha} # Return error

            logging.info(f'{thread} -- Obtained simulation link: {nxt}')
            ok = True
            # --- 修復 429：加入重試邏輯 (GET 狀態) ---
            max_retries = 3
            retry_delay = 15 # seconds
            while True: # Loop for checking simulation status
                # 檢查停止請求 (在每次嘗試獲取狀態之前)
                if self.worker_ref and self.worker_ref.stop_requested:
                    logging.info(f"{thread} -- 偵測到停止請求，中斷狀態檢查。")
                    return {'error': '模擬被手動停止', 'alpha': alpha}

                for attempt in range(max_retries):
                    # 再次檢查停止請求 (在每次重試之前)
                    if self.worker_ref and self.worker_ref.stop_requested:
                        logging.info(f"{thread} -- 偵測到停止請求，中斷狀態檢查重試。")
                        return {'error': '模擬被手動停止', 'alpha': alpha}

                    try:
                        r = self.get(nxt)
                        r.raise_for_status() # Check for HTTP errors (including 429)
                        r_json = r.json()
                        status_check_success = True # Flag success for this attempt
                        break # Success, exit retry loop for this status check
                    except requests.exceptions.HTTPError as http_err:
                        if http_err.response.status_code == 429:
                            retry_msg = f"檢查狀態請求過多 (429)，等待 {retry_delay} 秒後重試 ({attempt + 1}/{max_retries})..."
                            logging.warning(f"{thread} -- {retry_msg}")
                            # --- 更新進度標籤 ---
                            if self.worker_ref:
                                self.worker_ref.progress_updated.emit(retry_msg)
                            # --- 更新結束 ---
                            if attempt < max_retries - 1:
                                # 等待時檢查停止請求
                                for _ in range(retry_delay * 5): # Check every 0.2 seconds
                                    if self.worker_ref and self.worker_ref.stop_requested:
                                        logging.info(f"{thread} -- 偵測到停止請求，中斷重試等待。")
                                        return {'error': '模擬被手動停止', 'alpha': alpha}
                                    time.sleep(0.2)
                                status_check_success = False # Mark attempt as failed due to 429
                                continue # Continue to the next retry attempt
                            else:
                                logging.error(f"{thread} -- 達到最大重試次數 ({max_retries})，放棄檢查狀態。")
                                ok = (False, f"API 請求過多 (429)，達到最大重試次數 ({max_retries})。")
                                status_check_success = False # Mark as failed
                                break # Exit retry loop, ok is set to error
                        else:
                            # Handle other HTTP errors
                            logging.error(f"{thread} -- Failed to get simulation status with HTTP error: {http_err}")
                            ok = (False, f"無法取得模擬狀態: {http_err}")
                            status_check_success = False # Mark as failed
                            break # Exit retry loop, ok is set to error
                    except requests.exceptions.RequestException as req_err: # Handle non-HTTP request errors
                        logging.error(f"{thread} -- Failed to get simulation status: {req_err}")
                        ok = (False, f"無法取得模擬狀態: {req_err}")
                        status_check_success = False # Mark as failed
                        break # Exit retry loop, ok is set to error
                    except Exception as e: # Handle other errors like JSON parsing
                        logging.error(f"{thread} -- Error checking simulation status: {e}")
                        message = r_json.get('message', str(e)) if 'r_json' in locals() and isinstance(r_json, dict) else str(e)
                        ok = (False, message)
                        status_check_success = False # Mark as failed
                        break # Exit retry loop, ok is set to error

                # --- 重試邏輯結束 ---

                if not status_check_success: # If all retries failed for status check
                    break # Exit the outer while loop for status checking

                # --- 原有狀態處理邏輯 ---
                if 'alpha' in r_json:
                    alpha_link = r_json['alpha']
                    break
                else: # Added else for clarity and correct indentation
                    # Correct indentation for these lines
                    progress = r_json.get('progress', 0)
                    logging.info(f"{thread} -- Waiting for simulation to end ({int(100*progress)}%)")
                    # 個別進度更新
                    if self.worker_ref:
                        self.worker_ref.single_simulation_progress.emit(row_uuid, int(100*progress))
                    # Comment out the individual alpha progress update to avoid flickering with the overall count
                    # if self.worker_ref:
                    #     self.worker_ref.progress_updated.emit(f"模擬進行中 ({alpha[:20]}...): {int(100*progress)}%")
                # --- 原有狀態處理邏輯結束 ---

                # 如果模擬尚未完成，則等待並繼續檢查狀態
                # 將 10 秒 sleep 拆成 50 次 0.2 秒 sleep，每次檢查是否收到停止請求
                wait_interval = 0.2
                total_wait_time = 10 # seconds
                num_intervals = int(total_wait_time / wait_interval)

                for _ in range(num_intervals):
                    if self.worker_ref and self.worker_ref.stop_requested:
                        logging.info(f"{thread} -- 偵測到停止請求，中斷等待。")
                        return {'error': '模擬被手動停止', 'alpha': alpha}
                    time.sleep(wait_interval)
                # Loop back to check status again

            # --- 狀態檢查循環結束 ---

            if ok != True: # Check if status checking failed after retries
                error_msg = f"模擬失敗 ({alpha[:20]}...): {ok[1]}"
                logging.info(f'{thread} -- Issue when sending simulation request: {ok[1]}')
                if self.worker_ref:
                    self.worker_ref.error_occurred.emit(error_msg)
                row = [
                    0, delay, region,
                    neutralization, decay, truncation,
                    0, 0, 0, 'FAIL', 0, -1, universe, nxt, alpha
                ]
            else:
                # --- 修復 429：加入重試邏輯 (GET Alpha 詳細資訊) ---
                max_retries = 3
                retry_delay = 15 # seconds
                alpha_details_fetched = False
                for attempt in range(max_retries):
                     # 檢查停止請求
                    if self.worker_ref and self.worker_ref.stop_requested:
                        logging.info(f"{thread} -- 偵測到停止請求，取消獲取 Alpha 詳細資訊。")
                        return {'error': '模擬被手動停止', 'alpha': alpha}

                    try:
                        r = self.get(f'https://api.worldquantbrain.com/alphas/{alpha_link}')
                        r.raise_for_status() # Check for HTTP errors (including 429)
                        r_json = r.json()
                        alpha_details_fetched = True
                        break # Success, exit retry loop
                    except requests.exceptions.HTTPError as http_err:
                        if http_err.response.status_code == 429:
                            retry_msg = f"獲取 Alpha 詳細請求過多 (429)，等待 {retry_delay} 秒後重試 ({attempt + 1}/{max_retries})..."
                            logging.warning(f"{thread} -- {retry_msg}")
                            # --- 更新進度標籤 ---
                            if self.worker_ref:
                                self.worker_ref.progress_updated.emit(retry_msg)
                            # --- 更新結束 ---
                            if attempt < max_retries - 1:
                                # 等待時檢查停止請求
                                for _ in range(retry_delay * 5): # Check every 0.2 seconds
                                    if self.worker_ref and self.worker_ref.stop_requested:
                                        logging.info(f"{thread} -- 偵測到停止請求，中斷重試等待。")
                                        return {'error': '模擬被手動停止', 'alpha': alpha}
                                    time.sleep(0.2)
                                continue # Continue to the next retry attempt
                            else:
                                logging.error(f"{thread} -- 達到最大重試次數 ({max_retries})，放棄獲取 Alpha 詳細資訊。")
                                error_msg = f"API 請求過多 (429)，達到最大重試次數 ({max_retries})。"
                                # Fall through to handle as error below
                        else:
                            # Handle other HTTP errors
                            logging.error(f"{thread} -- Failed to get alpha details with HTTP error: {http_err}")
                            error_msg = f"無法取得 Alpha 詳細資訊: {http_err}"
                            # Fall through to handle as error below
                        break # Exit retry loop after error or max retries
                    except requests.exceptions.RequestException as req_err: # Handle non-HTTP request errors
                        logging.error(f"{thread} -- Failed to get alpha details: {req_err}")
                        error_msg = f"無法取得 Alpha 詳細資訊: {req_err}"
                        break # Exit retry loop
                    except Exception as e: # Handle other errors like JSON parsing
                        logging.error(f"{thread} -- Error getting alpha details: {e}")
                        error_msg = f"處理 Alpha 詳細資訊時出錯: {e}"
                        break # Exit retry loop

                # --- 重試邏輯結束 ---

                if alpha_details_fetched:
                    logging.info(f'{thread} -- Obtained alpha link: https://platform.worldquantbrain.com/alpha/{alpha_link}')
                    passed = 0
                    weight_check = 'N/A'
                    subsharpe = -1
                    for check in r_json.get('is', {}).get('checks', []):
                        passed += check.get('result') == 'PASS'
                        if check.get('name') == 'CONCENTRATED_WEIGHT':
                            weight_check = check.get('result', 'N/A')
                        if check.get('name') == 'LOW_SUB_UNIVERSE_SHARPE':
                            subsharpe = check.get('value', -1)

                    row = [
                        passed, delay, region,
                        neutralization, decay, truncation,
                        r_json.get('is', {}).get('sharpe', 0),
                        r_json.get('is', {}).get('fitness', 0),
                        round(100 * r_json.get('is', {}).get('turnover', 0), 2),
                        weight_check, subsharpe, -1,
                        universe, f'https://platform.worldquantbrain.com/alpha/{alpha_link}', alpha
                    ]
                    # 新增：成功取得 Alpha 詳細資訊後，進度設為 100%
                    if self.worker_ref:
                        self.worker_ref.single_simulation_progress.emit(row_uuid, 100)
                else: # Handle failure to fetch alpha details after retries
                    # error_msg is set in the except blocks above
                    row = [0, delay, region, neutralization, decay, truncation, 0, 0, 0, 'FAIL', 0, -1, universe, f'alpha/{alpha_link}', alpha]
                    if self.worker_ref:
                        self.worker_ref.error_occurred.emit(error_msg) # Emit error signal

            # Return the processed row data (or error if applicable)
            # Ensure 'row' key exists even on error for consistent handling later
            if 'row' not in locals():
                 row = [0, delay, region, neutralization, decay, truncation, 0, 0, 0, 'FAIL', 0, -1, universe, 'N/A', alpha] # Default error row
            return {'uuid': row_uuid, 'row': row, 'simulation': simulation}

        except Exception as e:
            # Catch any unexpected error during the process
            error_msg = f"處理模擬 '{simulation.get('code', 'N/A')[:20]}...' 時發生未預期錯誤: {e}"
            logging.exception(f"{thread} -- Unexpected error in _process_single_simulation")
            if self.worker_ref:
                self.worker_ref.error_occurred.emit(error_msg)
            # Return an error indicator
            return {'uuid': row_uuid, 'error': error_msg, 'alpha': simulation.get('code', 'N/A')}

    def simulate(self, data):
        if self.login_expired: # 如果登入失敗，直接返回
             return []

        self.rows_processed = []
        self.completed_count = 0
        self.total_rows = len(data) # Use the length of the current batch

        try:
            for handler in logging.root.handlers:
                logging.root.removeHandler(handler)
            self.latest_csv_file = self.csv_file
            mode = 'w' if self.first_run else 'a'
            logging.basicConfig(
                encoding='utf-8', level=logging.INFO,
                format='%(asctime)s: %(message)s',
                filename=self.log_file
            )
            if self.first_run:
                logging.info(f'Creating CSV file: {self.csv_file}')
            with open(self.csv_file, mode, newline='') as f:
                writer = csv.writer(f)
                if self.first_run:
                    header = [
                        'passed', 'delay', 'region', 'neutralization', 'decay', 'truncation',
                        'sharpe', 'fitness', 'turnover', 'weight',
                        'subsharpe', 'correlation', 'universe', 'link', 'code'
                    ]
                    writer.writerow(header)
                    self.first_run = False

                # Use ThreadPoolExecutor to run simulations concurrently
                from concurrent.futures import as_completed
                with ThreadPoolExecutor(max_workers=3) as executor:
                    futures = []
                    for simulation in data:
                        future = executor.submit(self._process_single_simulation, simulation)
                        futures.append(future)

                    def done_callback(fut):
                        # 處理完成時的 UI 更新與進度
                        result = fut.result()
                        if result and 'row' in result:
                            with self._csv_lock:
                                writer.writerow(result['row'])
                                f.flush()
                                self.rows_processed.append(result['simulation'])
                                self.completed_count += 1
                                logging.info(f'Result added to CSV for alpha: {result["simulation"]["code"][:20]}...')
                                if self.worker_ref:
                                    self.worker_ref.simulation_row_completed.emit(result['uuid'], result['row'])
                            if self.worker_ref:
                                self.worker_ref.progress_updated.emit(f"{self.completed_count}/{self.total_rows}")
                        elif result and 'error' in result:
                            logging.warning(f"Skipping result for alpha {result['alpha'][:20]} due to error: {result['error']}")
                        elif result is None and not self.login_expired:
                            logging.warning("Received None result from simulation processing.")

                    for fut in futures:
                        fut.add_done_callback(done_callback)

                    # 等待所有 future 完成或遇到停止請求
                    for fut in as_completed(futures):
                        if self.worker_ref and self.worker_ref.stop_requested:
                            logging.info("停止請求已收到，停止處理剩餘結果。")
                            break
                        if self.login_expired:
                            break
                    # 注意：done_callback 會自動處理每個完成的結果
        except Exception as e:
            logging.exception("模擬執行或檔案寫入錯誤") # Log the full traceback
            if self.worker_ref:
                self.worker_ref.error_occurred.emit(f'檔案寫入或處理時發生錯誤: {type(e).__name__}: {e}')
        # Ensure finished signal is emitted even if file writing fails
        # The actual return value might not be critical if errors are handled via signals
        return [sim for sim in data if sim not in self.rows_processed]

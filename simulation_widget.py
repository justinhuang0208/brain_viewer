from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTableWidget, QTableWidgetItem,
    QPushButton, QLabel, QMessageBox, QHeaderView, QComboBox, QDialog,
    QPlainTextEdit, QDialogButtonBox, QCheckBox, QMenu
)
from PySide6.QtGui import QAction, QColor # Import QColor
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
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("模擬參數編輯與執行")
        # self.process = None # Removed QProcess attribute

        layout = QVBoxLayout(self)

        # 進度顯示
        self.progress_label = QLabel("尚未開始模擬")
        layout.addWidget(self.progress_label)

        # 參數表格 - 增加一欄給 checkbox
        self.table = QTableWidget(0, len(PARAM_COLUMNS) + 1) # +1 for checkbox column
        header_labels = ["選取"] + PARAM_COLUMNS # Add header for checkbox
        self.table.setHorizontalHeaderLabels(header_labels)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        # Make the checkbox column narrower
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        layout.addWidget(self.table)

        # 按鈕區
        btn_layout = QHBoxLayout() # Combine buttons in one layout

        # 資料編輯按鈕與下拉選單
        self.edit_btn = QPushButton("編輯資料")
        edit_menu = QMenu(self)
        add_action = QAction("新增參數", self)
        dup_action = QAction("複製選取", self)
        del_action = QAction("刪除選取", self)
        edit_menu.addAction(add_action)
        edit_menu.addAction(dup_action)
        edit_menu.addAction(del_action)
        self.edit_btn.setMenu(edit_menu)
        btn_layout.addWidget(self.edit_btn)

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
        add_action.triggered.connect(self.add_row)
        dup_action.triggered.connect(self.duplicate_selected_rows)
        del_action.triggered.connect(self.delete_selected_rows)
        self.del_all_btn.clicked.connect(self.delete_all_rows) # 連接刪除全部按鈕
        self.sim_btn.clicked.connect(self.toggle_simulation) # 改為連接 toggle_simulation
        self.check_login_btn.clicked.connect(self.check_login_status) # Connect new button
        self.table.cellDoubleClicked.connect(self.handle_cell_double_clicked)
        self.simulation_thread = None
        self.simulation_worker = None
        self.is_simulating = False # 追蹤模擬狀態
        # Store the index of the 'code' column for faster lookup
        try:
            self._code_col_index = PARAM_COLUMNS.index('code') + 1 # +1 for checkbox column
        except ValueError:
            self._code_col_index = -1 # Should not happen if PARAM_COLUMNS is correct

    @Slot(list)
    def highlight_completed_row(self, row_data: list):
        """根據完成的 row_data 標示對應的表格行"""
        # Check if the table's code column index is valid
        if self._code_col_index == -1:
            print("錯誤：無法確定表格中的 'code' 欄位索引")
            return

        # The row_data comes from the CSV structure, where 'code' is the last element.
        if not row_data:
             print("錯誤：收到的 row_data 為空")
             return
        # Ensure row_data has enough elements before accessing the last one
        if len(row_data) == 0:
             print("錯誤: 收到的 row_data 列表為空")
             return
        completed_code = str(row_data[-1]) # Get the code from the LAST element of the CSV row data

        light_green = QColor("#e8f5e9") # Define light green color
        # Define light orange color - Moved definition here for clarity
        # light_orange = QColor("#fff3e0") # Already defined in highlight_processing_row

        # Find the row in the table with the matching code in the correct table column
        found = False
        for row in range(self.table.rowCount()):
            # Access the 'code' column item in the TABLE (index self._code_col_index) # Correct indent
            item = self.table.item(row, self._code_col_index)
            # Compare the table item's text with the completed code, using strip()
            if item and item.text().strip() == completed_code.strip(): # Add strip() here
                # Highlight the entire row
                for col in range(self.table.columnCount()):
                    cell_item = self.table.item(row, col)
                    if not cell_item: # Create item if it doesn't exist (e.g., for checkbox or empty cells) # Correct indent
                        # For widgets like ComboBox, we cannot set background directly on item
                        widget = self.table.cellWidget(row, col) # Correct indent
                        if widget: # Correct indent
                            # Attempt to set background for widgets (might not work for all styles)
                            try:
                                widget.setStyleSheet("background-color: #e8f5e9;")
                            except AttributeError: # Correct indent
                                pass # Ignore if widget doesn't support setStyleSheet
                        else:
                            # Create item for non-widget cells
                             cell_item = QTableWidgetItem()
                             self.table.setItem(row, col, cell_item)
                             cell_item.setBackground(light_green)
                    else:
                         cell_item.setBackground(light_green)
                found = True
                break # Stop searching once the row is found and highlighted

        if not found:
             print(f"警告: 在表格中未找到與完成的 code '{completed_code[:30]}...' 匹配的行")

    @Slot(dict)
    def highlight_processing_row(self, simulation_data: dict):
        """根據開始處理的 simulation_data 標示對應的表格行為淺橘色"""
        if self._code_col_index == -1:
            print("錯誤：無法確定表格中的 'code' 欄位索引")
            return

        processing_code = simulation_data.get('code', '').strip()
        if not processing_code:
            print("錯誤: 收到的 simulation_data 中缺少 'code'")
            return

        light_orange = QColor("#fff3e0") # Define light orange color

        # Find the row in the table with the matching code
        found = False
        for row in range(self.table.rowCount()):
            item = self.table.item(row, self._code_col_index)
            if item and item.text().strip() == processing_code:
                # Highlight the entire row with light orange
                for col in range(self.table.columnCount()):
                    cell_item = self.table.item(row, col)
                    if not cell_item:
                        widget = self.table.cellWidget(row, col)
                        if widget:
                            try:
                                # Set background for widgets (might need adjustments based on widget type/style)
                                widget.setStyleSheet("background-color: #fff3e0;")
                            except AttributeError:
                                pass # Ignore if widget doesn't support setStyleSheet
                        else:
                            # Create item for non-widget cells
                            cell_item = QTableWidgetItem()
                            self.table.setItem(row, col, cell_item)
                            cell_item.setBackground(light_orange)
                    else:
                        # Set background for existing items
                        cell_item.setBackground(light_orange)
                found = True
                break # Stop searching once the row is found and highlighted

        if not found:
            print(f"警告: 在表格中未找到與正在處理的 code '{processing_code[:30]}...' 匹配的行")

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

            print(f"接收到 {len(df)} 筆策略，準備載入模擬器表格...")
            self.load_parameters_from_dataframe(df)
            QMessageBox.information(self, "匯入成功", f"已成功從生成器匯入 {len(df)} 筆策略。")

        except Exception as e:
            QMessageBox.critical(self, "匯入錯誤", f"將策略列表轉換為 DataFrame 或載入時出錯: {e}")
            print(f"將策略列表轉換為 DataFrame 或載入時出錯: {e}")

    def load_parameters_from_dataframe(self, df: pd.DataFrame):
        """從 DataFrame 載入參數到表格中"""
        # 考慮是否在每次載入時清空表格，或者追加
        # 目前行為是清空
        self.table.setRowCount(0) # 清空表格

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

        # Add checkbox item to the first column
        chk_item = QTableWidgetItem()
        chk_item.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
        chk_item.setCheckState(Qt.Unchecked)
        self.table.setItem(row, 0, chk_item)

        # Use provided data or defaults for other columns
        values_to_set = data if data else DEFAULT_VALUES[1:] # Skip the None placeholder for checkbox

        for col_idx, val in enumerate(values_to_set):
            table_col = col_idx + 1 # Actual table column index (starts from 1)
            param_key = PARAM_COLUMNS[col_idx]

            if param_key == "delay":
                combo = QComboBox()
                combo.addItems(DELAY_OPTIONS)
                # Set current text based on provided data or default
                current_val = str(val) if val is not None else str(DEFAULT_VALUES[table_col])
                combo.setCurrentText(current_val)
                self.table.setCellWidget(row, table_col, combo)
            elif param_key == "neutralization":
                combo = QComboBox()
                combo.addItems(NEUTRALIZATION_OPTIONS)
                current_val = str(val) if val is not None else str(DEFAULT_VALUES[table_col])
                combo.setCurrentText(current_val)
                self.table.setCellWidget(row, table_col, combo)
            elif param_key == "universe":
                combo = QComboBox()
                combo.addItems(UNIVERSE_OPTIONS)
                current_val = str(val) if val is not None else str(DEFAULT_VALUES[table_col])
                combo.setCurrentText(current_val)
                self.table.setCellWidget(row, table_col, combo) # Correct indent
            else:
                # Add strip() when setting item text, especially for 'code'
                current_val = str(val).strip() if val is not None else str(DEFAULT_VALUES[table_col]).strip()
                item = QTableWidgetItem(current_val)
                self.table.setItem(row, table_col, item)

    def handle_cell_double_clicked(self, row, col): # Correct indent
        if col == 0: # Ignore double clicks on checkbox column
             return
        param_col_index = col - 1 # Adjust column index for PARAM_COLUMNS
        if PARAM_COLUMNS[param_col_index] == "code":
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
                for col in range(1, self.table.columnCount()): # Start from col 1
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

    def get_parameters(self):
        params = []
        for row in range(self.table.rowCount()):
            entry = {}
            for col_idx, key in enumerate(PARAM_COLUMNS):
                table_col = col_idx + 1 # Actual table column index
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
        else:
            print("無法停止：沒有模擬正在執行或 Worker 不存在。")
            # 如果沒有在執行，確保 UI 狀態正確
            if not self.is_simulating:
                 self.sim_btn.setText("執行模擬")
                 self.sim_btn.setEnabled(True)
                 self.edit_btn.setEnabled(True)
                 self.check_login_btn.setEnabled(True)

    def start_simulation_thread(self):
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
        self.progress_label.setText("模擬準備中...")

        # 創建執行緒和 Worker
        self.simulation_thread = QThread()
        self.simulation_worker = SimulationWorker(params)
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

        # 啟動執行緒
        self.simulation_thread.start()

    def update_progress_label(self, message):
        self.progress_label.setText(message)

    def handle_simulation_error(self, error_message):
        self.progress_label.setText("模擬失敗")
        QMessageBox.critical(self, "模擬失敗", error_message)
        # 重置 UI 狀態
        self.is_simulating = False
        self.sim_btn.setText("執行模擬")
        self.sim_btn.setEnabled(True)
        self.edit_btn.setEnabled(True)
        self.check_login_btn.setEnabled(True)

    def handle_simulation_finished(self):
        # 檢查是否是因為停止請求而結束
        stopped_early = self.simulation_worker and self.simulation_worker.stop_requested

        if stopped_early:
            self.progress_label.setText("模擬已停止")
            QMessageBox.warning(self, "模擬停止", "模擬已被使用者手動停止。")
        else:
            self.progress_label.setText("模擬完成")
            QMessageBox.information(self, "模擬完成", "模擬已成功完成！請檢查 data 資料夾中的 CSV 和 LOG 檔案。")

        # 重置 UI 狀態
        self.is_simulating = False
        self.sim_btn.setText("執行模擬")
        self.sim_btn.setEnabled(True)
        self.edit_btn.setEnabled(True)
        self.check_login_btn.setEnabled(True)

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
        """檢查 WorldQuant Brain 的登入狀態"""
        json_fn = 'credentials.json'
        try:
            with open(json_fn, 'r') as f:
                creds = json.load(f) # Corrected: Pass file object directly
                email, password = creds['email'], creds['password']
        except FileNotFoundError:
            QMessageBox.critical(self, "登入檢查失敗", f"找不到憑證檔案 {json_fn}")
            self.progress_label.setText("登入檢查失敗 (找不到憑證)")
            return
        except Exception as e:
            QMessageBox.critical(self, "登入檢查失敗", f"讀取憑證檔案時發生錯誤: {e}")
            self.progress_label.setText("登入檢查失敗 (讀取憑證錯誤)")
            return

        session = requests.Session()
        session.auth = (email, password)
        try:
            self.progress_label.setText("正在檢查登入狀態...")
            self.check_login_btn.setEnabled(False) # Disable button during check
            # Use a short timeout to avoid blocking the UI for too long
            r = session.post('https://api.worldquantbrain.com/authentication', timeout=10)
            r.raise_for_status() # Check for HTTP errors like 4xx/5xx

            response_json = r.json()
            if 'user' in response_json:
                QMessageBox.information(self, "登入成功", "WorldQuant Brain 登入狀態正常。")
                self.progress_label.setText("登入狀態正常")
            elif 'inquiry' in response_json:
                # Handle persona verification requirement
                persona_url = f"{r.url}/persona?inquiry={response_json['inquiry']}"
                QMessageBox.warning(self, "需要驗證", f"登入需要生物驗證，請前往:\n{persona_url}")
                self.progress_label.setText("登入需要驗證")
            else:
                # Handle other unexpected login failures
                error_detail = response_json.get('detail', '未知錯誤')
                QMessageBox.critical(self, "登入失敗", f"登入失敗: {error_detail}")
                self.progress_label.setText(f"登入失敗: {error_detail}")

        except requests.exceptions.Timeout:
            QMessageBox.critical(self, "登入檢查超時", "檢查登入狀態時連線超時。")
            self.progress_label.setText("登入檢查超時")
        except requests.exceptions.RequestException as e:
            QMessageBox.critical(self, "登入檢查錯誤", f"檢查登入狀態時發生網路錯誤: {e}")
            self.progress_label.setText(f"登入檢查網路錯誤")
        except Exception as e:
            QMessageBox.critical(self, "登入檢查錯誤", f"檢查登入狀態時發生未知錯誤: {e}")
            self.progress_label.setText("登入檢查未知錯誤")
        finally:
            self.check_login_btn.setEnabled(True) # Re-enable button

# Worker 類別，用於在背景執行緒中執行模擬
class SimulationWorker(QObject):
    finished = Signal()
    progress_updated = Signal(str)
    error_occurred = Signal(str)
    simulation_row_completed = Signal(list) # New signal for completed row
    simulation_row_started = Signal(dict) # New signal for starting row (emit simulation dict)

    def __init__(self, params):
        super().__init__()
        self.params = params
        self.stop_requested = False # 加入停止標誌

    def request_stop(self):
        """設置停止標誌"""
        self.stop_requested = True
        print("Worker 收到停止請求。") # Debug message

    def run(self):
        try:
            # Pass worker_ref during initialization
            # WQSession can access self.stop_requested via worker_ref
            wq = WQSession(worker_ref=self)
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
    def __init__(self, json_fn='credentials.json', worker_ref=None): # Add worker_ref parameter
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
        # self.worker_ref = None # Reference to the worker for emitting signals - Removed, set in __init__
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
            # Emit the started signal before processing
            if self.worker_ref:
                self.worker_ref.simulation_row_started.emit(simulation) # Emit the signal here

            alpha = simulation['code'].strip()
            delay = simulation.get('delay', 1)
            universe = simulation.get('universe', 'TOP3000')
            truncation = simulation.get('truncation', 0.1)
            region = simulation.get('region', 'USA')
            decay = simulation.get('decay', 6)
            neutralization = simulation.get('neutralization', 'SUBINDUSTRY').upper()
            pasteurization = simulation.get('pasteurization', 'ON')
            nan = simulation.get('nanHandling', 'OFF')
            logging.info(f"{thread} -- Simulating alpha: {alpha}")
            while True:
                try:
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
                    r.raise_for_status() # Check for HTTP errors
                    nxt = r.headers['Location']
                    break
                except requests.exceptions.RequestException as req_err:
                    logging.error(f"{thread} -- Simulation request failed: {req_err}")
                    if self.worker_ref:
                        self.worker_ref.error_occurred.emit(f"模擬請求失敗: {req_err}")
                    # Return an error indicator instead of just returning
                    return {'error': f"模擬請求失敗: {req_err}", 'alpha': alpha}
                except Exception as e: # Catch other potential errors like missing headers
                    logging.error(f"{thread} -- Error during simulation request: {e}")
                    error_msg = f"模擬請求時發生未知錯誤: {e}"
                    if 'credentials' in str(e) or (hasattr(r, 'json') and 'credentials' in r.json().get('detail', '')):
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
            while True:
                try:
                    r = self.get(nxt)
                    r.raise_for_status()
                    r_json = r.json()
                    if 'alpha' in r_json:
                        alpha_link = r_json['alpha']
                        break
                    # Correct indentation for these lines
                    progress = r_json.get('progress', 0)
                    logging.info(f"{thread} -- Waiting for simulation to end ({int(100*progress)}%)")
                    # Comment out the individual alpha progress update to avoid flickering with the overall count
                    # if self.worker_ref:
                    #     self.worker_ref.progress_updated.emit(f"模擬進行中 ({alpha[:20]}...): {int(100*progress)}%")
                # Correct indentation for the except block
                except requests.exceptions.RequestException as req_err:
                    logging.error(f"{thread} -- Failed to get simulation status: {req_err}")
                    ok = (False, f"無法取得模擬狀態: {req_err}")
                    break
                except Exception as e:
                    logging.error(f"{thread} -- Error checking simulation status: {e}")
                    # Use r_json if available, otherwise just the exception
                    message = r_json.get('message', str(e)) if 'r_json' in locals() and isinstance(r_json, dict) else str(e)
                    ok = (False, message)
                    break
                time.sleep(10)

            if ok != True:
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
                try:
                    r = self.get(f'https://api.worldquantbrain.com/alphas/{alpha_link}')
                    r.raise_for_status()
                    r_json = r.json()
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
                except requests.exceptions.RequestException as req_err:
                    logging.error(f"{thread} -- Failed to get alpha details: {req_err}")
                    error_msg = f"無法取得 Alpha 詳細資訊: {req_err}"
                    row = [0, delay, region, neutralization, decay, truncation, 0, 0, 0, 'FAIL', 0, -1, universe, f'alpha/{alpha_link}', alpha]
                    if self.worker_ref:
                        self.worker_ref.error_occurred.emit(error_msg)
                except Exception as e:
                    error_msg = f"處理 Alpha 詳細資訊時出錯: {e}"
                    logging.error(f"{thread} -- Error getting alpha details: {e}")
                    row = [0, delay, region, neutralization, decay, truncation, 0, 0, 0, 'FAIL', 0, -1, universe, f'alpha/{alpha_link}', alpha]
                    if self.worker_ref:
                        self.worker_ref.error_occurred.emit(error_msg)

            # Return the processed row data
            return {'row': row, 'simulation': simulation}

        except Exception as e:
            # Catch any unexpected error during the process
            error_msg = f"處理模擬 '{simulation.get('code', 'N/A')[:20]}...' 時發生未預期錯誤: {e}"
            logging.exception(f"{thread} -- Unexpected error in _process_single_simulation")
            if self.worker_ref:
                self.worker_ref.error_occurred.emit(error_msg)
            # Return an error indicator
            return {'error': error_msg, 'alpha': simulation.get('code', 'N/A')}

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
                with ThreadPoolExecutor(max_workers=3) as executor:
                    # Map simulations to the processing method
                    results = executor.map(self._process_single_simulation, data)

                    # Process results as they complete
                    for result in results:
                        # Check for stop request before processing each result
                        if self.worker_ref and self.worker_ref.stop_requested:
                            logging.info("停止請求已收到，停止處理剩餘結果。")
                            break # Exit the loop processing results

                        if self.login_expired: break # Stop processing if login expired during execution

                        if result and 'row' in result:
                            # Check for stop request again before writing (optional, but safer)
                            if self.worker_ref and self.worker_ref.stop_requested:
                                logging.info("停止請求已收到，跳過寫入 CSV。")
                                break
                            # Write row to CSV using lock for thread safety
                            with self._csv_lock:
                                writer.writerow(result['row'])
                                f.flush() # Ensure data is written to disk
                                self.rows_processed.append(result['simulation'])
                                self.completed_count += 1
                                logging.info(f'Result added to CSV for alpha: {result["simulation"]["code"][:20]}...')
                                # Emit signal for the completed row via worker_ref
                                if self.worker_ref: # Check if worker_ref exists
                                    self.worker_ref.simulation_row_completed.emit(result['row'])

                            # Update overall progress using the desired format
                            if self.worker_ref:
                                self.worker_ref.progress_updated.emit(f"{self.completed_count}/{self.total_rows}")
                        elif result and 'error' in result:
                            # Error already logged and emitted by _process_single_simulation
                            logging.warning(f"Skipping result for alpha {result['alpha'][:20]} due to error: {result['error']}")
                        # Handle case where result is None (e.g., initial login failure)
                        elif result is None and not self.login_expired:
                             logging.warning("Received None result from simulation processing.")

        except Exception as e:
            logging.exception("模擬執行或檔案寫入錯誤") # Log the full traceback
            if self.worker_ref:
                self.worker_ref.error_occurred.emit(f'檔案寫入或處理時發生錯誤: {type(e).__name__}: {e}')
        # Ensure finished signal is emitted even if file writing fails
        # The actual return value might not be critical if errors are handled via signals
        return [sim for sim in data if sim not in self.rows_processed]

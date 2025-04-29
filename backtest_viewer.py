import sys
import os
import sqlite3
import pandas as pd
from PySide6.QtWidgets import (QApplication, QMainWindow, QTableView, QTreeView,
                              QSplitter, QVBoxLayout, QHBoxLayout, QWidget,
                              QLineEdit, QLabel, QComboBox, QFileSystemModel,
                              QHeaderView, QPushButton, QStatusBar, QTabWidget,
                              QMessageBox, QDialog, QTextEdit, QFrame,
                              QFileDialog, QCheckBox, QScrollArea, QMenu, # Added QMenu
                              QInputDialog) # Added QInputDialog for save dialog
from PySide6.QtCore import Qt, QDir, QModelIndex, QSortFilterProxyModel, Signal, Slot, QAbstractTableModel, QRegularExpression
from PySide6.QtGui import QColor, QFont, QPalette, QIcon, QAction # Added QAction
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import matplotlib
import webbrowser

# 設置matplotlib中文支持
def set_chinese_font():
    # 嘗試多種中文字體，按優先順序
    chinese_fonts = ['Microsoft JhengHei', 'DFKai-SB', 'SimHei', 'STHeiti', 'STSong', 'Arial Unicode MS', 'SimSun']
    
    # 在Windows上查找可用字體
    if os.name == 'nt':
        from matplotlib.font_manager import FontManager
        fm = FontManager()
        available_fonts = set([f.name for f in fm.ttflist])
        
        for font in chinese_fonts:
            if (font in available_fonts):
                print(f"使用中文字體: {font}")
                matplotlib.rcParams['font.family'] = font
                return True
                
    # 如果找不到特定中文字體，嘗試使用系統預設配置
    try:
        plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Arial Unicode MS', 'sans-serif']
        plt.rcParams['axes.unicode_minus'] = False  # 解決負號顯示問題
        return True
    except:
        return False

# 嘗試設置中文字體
set_chinese_font()

# 自定義過濾代理模型，支持數值比較過濾
class NumericFilterProxyModel(QSortFilterProxyModel):
    def __init__(self, parent=None):
        super(NumericFilterProxyModel, self).__init__(parent)
        self.filter_column = -1  # -1表示所有列
        self.filter_comparator = '>'  # 默認比較符
        self.filter_value = None  # 過濾數值
        self.use_numeric_filter = False  # 是否使用數值過濾
        
    def setNumericFilter(self, column, comparator, value):
        self.filter_column = column
        self.filter_comparator = comparator
        self.filter_value = value
        self.use_numeric_filter = True
        self.invalidateFilter()
        
    def clearNumericFilter(self):
        self.use_numeric_filter = False
        self.invalidateFilter()
        
    def filterAcceptsRow(self, source_row, source_parent):
        # 首先檢查基本文本過濾
        if not super(NumericFilterProxyModel, self).filterAcceptsRow(source_row, source_parent):
            return False
            
        # 如果未啟用數值過濾，直接返回True
        if not self.use_numeric_filter or self.filter_value is None:
            return True
            
        # 獲取數據並進行比較
        model = self.sourceModel()
        index = model.index(source_row, self.filter_column, source_parent)
        
        if not index.isValid():
            return True
            
        data = model.data(index, Qt.DisplayRole)
        if data is None:
            return True
            
        try:
            # 嘗試轉換為浮點數進行比較
            value = float(data)
            
            if self.filter_comparator == '>':
                return value > self.filter_value
            elif self.filter_comparator == '<':
                return value < self.filter_value
            elif self.filter_comparator == '=':
                return value == self.filter_value
            elif self.filter_comparator == '>=':
                return value >= self.filter_value
            elif self.filter_comparator == '<=':
                return value <= self.filter_value
            else:
                return True
        except (ValueError, TypeError):
            # 如果無法轉換為數值，保留該行
            return True
            
        return True

# 新增: 過濾檔案的 ProxyModel
class FileFilterProxyModel(QSortFilterProxyModel):
    def filterAcceptsRow(self, source_row, source_parent):
        model = self.sourceModel()
        index = model.index(source_row, 0, source_parent)
        file_name = model.fileName(index)
        # 過濾掉 _header1.csv, _header2.csv, 以及 .log 檔案
        if file_name in ["_header1.csv", "_header2.csv"]:
            return False
        if file_name.lower().endswith(".log"):
            return False
        return super().filterAcceptsRow(source_row, source_parent)

# SQLite表格模型
class SqliteTableModel(QAbstractTableModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.db_conn = None
        self.table_name = None
        self.columns = []
        self.row_count = 0
        self.filter_clause = ""
        self.sort_clause = ""
        self._check_states = {}  # 使用字典來儲存勾選狀態，鍵為 rowid

    def setup_model(self, db_conn, table_name):
        """設置模型的資料庫連接和表格名稱"""
        self.db_conn = db_conn
        self.table_name = table_name
        self.refresh_metadata()

    def refresh_metadata(self):
        """重新載入表格的元數據（欄位和行數）"""
        if not self.db_conn or not self.table_name:
            return

        # 獲取欄位信息
        cursor = self.db_conn.cursor()
        cursor.execute(f"PRAGMA table_info({self.table_name})")
        self.columns = [row[1] for row in cursor.fetchall()]

        # 獲取總行數
        cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
        self.row_count = cursor.fetchone()[0]

        # 初始化勾選狀態
        cursor.execute(f"SELECT rowid FROM {self.table_name}")
        self._check_states = {row[0]: Qt.Unchecked for row in cursor.fetchall()}

    def rowCount(self, parent=None):
        if not self.db_conn or not self.table_name:
            return 0
        
        cursor = self.db_conn.cursor()
        query = f"SELECT COUNT(*) FROM {self.table_name}"
        if self.filter_clause:
            query += f" WHERE {self.filter_clause}"
        cursor.execute(query)
        return cursor.fetchone()[0]

    def columnCount(self, parent=None):
        # 加1是為了勾選框列
        return len(self.columns) + 1 if self.columns else 1

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None

        row = index.row()
        col = index.column()

        # 處理勾選框列（第一列）
        if col == 0:
            if role == Qt.CheckStateRole:
                cursor = self.db_conn.cursor()
                cursor.execute(f"SELECT rowid FROM {self.table_name} {self._get_filter_and_sort_clause()} LIMIT 1 OFFSET {row}")
                result = cursor.fetchone()
                if result:
                    rowid = result[0]
                    return self._check_states.get(rowid, Qt.Unchecked)
            return None

        # 獲取實際數據
        try:
            cursor = self.db_conn.cursor()
            # 調整列索引（因為第一列是勾選框）
            actual_col = self.columns[col - 1]
            query = f"SELECT {actual_col} FROM {self.table_name} {self._get_filter_and_sort_clause()} LIMIT 1 OFFSET {row}"
            cursor.execute(query)
            value = cursor.fetchone()
            
            if value is None:
                return None
            
            if role == Qt.DisplayRole:
                return str(value[0])
                
            elif role == Qt.ToolTipRole:
                if len(str(value[0])) > 30:
                    return str(value[0])
                    
            elif role == Qt.BackgroundRole:
                # 為特定欄位設置背景色
                column_name = self.columns[col - 1].lower()
                if column_name == 'passed':
                    if str(value[0]).upper() == 'PASS':
                        return QColor("#c8e6c9")
                    elif str(value[0]).upper() == 'FAIL':
                        return QColor("#ffccbc")
                elif column_name == 'sharpe':
                    try:
                        sharpe = float(value[0])
                        if sharpe > 1.5:
                            return QColor("#c8e6c9")
                        elif sharpe > 1.0:
                            return QColor("#fff9c4")
                        else:
                            return QColor("#ffccbc")
                    except:
                        pass

        except Exception as e:
            print(f"Error fetching data: {e}")
            return None

        return None

    def setData(self, index, value, role=Qt.EditRole):
        if not index.isValid():
            return False

        # 處理勾選框狀態變更
        if index.column() == 0 and role == Qt.CheckStateRole:
            try:
                cursor = self.db_conn.cursor()
                cursor.execute(f"SELECT rowid FROM {self.table_name} {self._get_filter_and_sort_clause()} LIMIT 1 OFFSET {index.row()}")
                result = cursor.fetchone()
                if result:
                    rowid = result[0]
                    self._check_states[rowid] = Qt.CheckState(value)
                    self.dataChanged.emit(index, index, [role])
                    return True
            except Exception as e:
                print(f"Error setting check state: {e}")
                return False

        return False

    def headerData(self, section, orientation, role):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                if section == 0:
                    return "#"  # 勾選框列標題
                elif 0 < section <= len(self.columns):
                    return self.columns[section - 1]
            elif orientation == Qt.Vertical:
                return str(section + 1)

        elif role == Qt.TextAlignmentRole and orientation == Qt.Horizontal and section == 0:
            return Qt.AlignCenter

        return None

    def flags(self, index):
        if not index.isValid():
            return Qt.NoItemFlags

        if index.column() == 0:  # 勾選框列
            return super().flags(index) | Qt.ItemIsUserCheckable | Qt.ItemIsEnabled

        return super().flags(index) | Qt.ItemIsEnabled | Qt.ItemIsSelectable

    def set_filter(self, filter_clause):
        """設置 SQL WHERE 子句進行過濾"""
        self.filter_clause = filter_clause
        self.layoutChanged.emit()

    def set_sort(self, column, order):
        """設置排序條件"""
        if column == 0:  # 勾選框列
            self.sort_clause = ""
        else:
            direction = "ASC" if order == Qt.AscendingOrder else "DESC"
            # 因為第一列是勾選框，所以要調整列索引
            actual_col = self.columns[column - 1]
            self.sort_clause = f"ORDER BY {actual_col} {direction}"
        self.layoutChanged.emit()

    def _get_filter_and_sort_clause(self):
        """組合過濾和排序子句"""
        clause = ""
        if self.filter_clause:
            clause += f"WHERE {self.filter_clause}"
        if self.sort_clause:
            clause += f" {self.sort_clause}"
        return clause

    def get_checked_rows(self):
        """獲取已勾選的行的 rowid 列表"""
        return [rowid for rowid, state in self._check_states.items() if state == Qt.Checked]

    def reset_check_states(self):
        """重設所有勾選狀態"""
        cursor = self.db_conn.cursor()
        cursor.execute(f"SELECT rowid FROM {self.table_name}")
        self._check_states = {row[0]: Qt.Unchecked for row in cursor.fetchall()}
        
        # 發出信號以更新第一列
        self.dataChanged.emit(
            self.index(0, 0),
            self.index(self.rowCount() - 1, 0),
            [Qt.CheckStateRole]
        )

# 自定義表格模型
class DataFrameModel(QAbstractTableModel):
    def __init__(self, data):
        super(DataFrameModel, self).__init__()
        self._data = data
        # Initialize check states for each row
        self._check_states = [Qt.Unchecked] * self._data.shape[0]

    def rowCount(self, parent=None):
        return self._data.shape[0]

    def columnCount(self, parent=None):
        # Add 1 for the checkbox column
        return self._data.shape[1] + 1

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None

        row = index.row()
        col = index.column()

        # Handle checkbox column (column 0)
        if col == 0:
            if role == Qt.CheckStateRole:
                return self._check_states[row]
            elif role == Qt.DisplayRole:
                 # Return the row number for sorting purposes
                 return row
            else:
                return None # No display text for checkbox column, but allow sorting by row number

        # Adjust column index for accessing pandas data
        actual_col = col - 1
        value = self._data.iloc[row, actual_col]

        if role == Qt.DisplayRole:
            return str(value)

        elif role == Qt.ToolTipRole:
            column_name = self._data.columns[actual_col]
            if column_name.lower() == 'code' and len(str(value)) > 30:
                return str(value)
            return str(value)

        elif role == Qt.BackgroundRole:
            column_name = self._data.columns[actual_col]
            if column_name.lower() == 'passed':
                if str(value).upper() == 'PASS':
                    return QColor("#c8e6c9")
                elif str(value).upper() == 'FAIL':
                    return QColor("#ffccbc")
            if column_name.lower() == 'sharpe':
                try:
                    sharpe = float(value)
                    if sharpe > 1.5:
                        return QColor("#c8e6c9")
                    elif sharpe > 1.0:
                        return QColor("#fff9c4")
                    else:
                        return QColor("#ffccbc")
                except:
                    pass

        return None

    def setData(self, index, value, role=Qt.EditRole):
        if not index.isValid():
            return False

        # Handle checkbox state changes
        if index.column() == 0 and role == Qt.CheckStateRole:
            self._check_states[index.row()] = Qt.CheckState(value)
            # Emit dataChanged signal for the specific cell
            self.dataChanged.emit(index, index, [role])
            return True

        return False # Other data is read-only

    def flags(self, index):
        if not index.isValid():
            return Qt.NoItemFlags

        # Make checkbox column checkable
        if index.column() == 0:
            # Align checkbox to center and enable checking
            return super().flags(index) | Qt.ItemIsUserCheckable | Qt.ItemIsEnabled

        # Make other cells selectable and enabled
        return super().flags(index) | Qt.ItemIsEnabled | Qt.ItemIsSelectable


    def headerData(self, section, orientation, role):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                # Checkbox column header - provide a display name for sorting
                if section == 0:
                    return "#" # Use '#' as header for the index/checkbox column
                # Original data headers (adjust index)
                return str(self._data.columns[section - 1])
            if orientation == Qt.Vertical:
                return str(self._data.index[section])
        # Add alignment role for checkbox header
        if role == Qt.TextAlignmentRole and orientation == Qt.Horizontal and section == 0:
             return Qt.AlignCenter

        return None

    # Helper method to get checked rows (returns original DataFrame indices)
    def get_checked_rows(self):
        return [i for i, state in enumerate(self._check_states) if state == Qt.Checked]

    # Helper method to reset check states
    def reset_check_states(self):
        self._check_states = [Qt.Unchecked] * self._data.shape[0]
        # Emit signal to refresh the first column
        first_col_top_left = self.index(0, 0)
        first_col_bottom_right = self.index(self.rowCount() - 1, 0)
        self.dataChanged.emit(first_col_top_left, first_col_bottom_right, [Qt.CheckStateRole])

# 自定義詳細文本對話框
class DetailDialog(QDialog):
    def __init__(self, title, text, parent=None):
        super(DetailDialog, self).__init__(parent)
        self.setWindowTitle(title)
        self.resize(600, 400)
        
        layout = QVBoxLayout()
        
        # 文本編輯器，只讀模式
        text_edit = QTextEdit()
        text_edit.setReadOnly(True)
        text_edit.setText(text)
        text_edit.setLineWrapMode(QTextEdit.WidgetWidth)
        
        layout.addWidget(text_edit)
        
        # 添加關閉按鈕
        close_button = QPushButton("關閉")
        close_button.clicked.connect(self.accept)
        
        layout.addWidget(close_button)
        self.setLayout(layout)

# 新增: 欄位選擇對話框
class ColumnSelectionDialog(QDialog):
    def __init__(self, columns, selected_columns, parent=None):
        super().__init__(parent)
        self.setWindowTitle("選擇顯示欄位")
        self.setMinimumWidth(300)

        self.columns = columns
        self.checkboxes = {}
        self.selected_columns = set(selected_columns) # Store initially selected

        layout = QVBoxLayout()

        # Scroll Area for checkboxes
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        content_widget = QWidget()
        checkbox_layout = QVBoxLayout(content_widget)
        checkbox_layout.setContentsMargins(10, 10, 10, 10)
        checkbox_layout.setSpacing(5)

        # Add checkboxes for each column
        for col in self.columns:
            checkbox = QCheckBox(col)
            # Check if the column was previously selected
            checkbox.setChecked(col in self.selected_columns)
            self.checkboxes[col] = checkbox
            checkbox_layout.addWidget(checkbox)

        checkbox_layout.addStretch() # Push checkboxes to the top
        content_widget.setLayout(checkbox_layout)
        scroll_area.setWidget(content_widget)

        layout.addWidget(scroll_area)

        # OK and Cancel buttons
        button_layout = QHBoxLayout()
        ok_button = QPushButton("確定")
        cancel_button = QPushButton("取消")
        ok_button.clicked.connect(self.accept)
        cancel_button.clicked.connect(self.reject)
        button_layout.addStretch()
        button_layout.addWidget(ok_button)
        button_layout.addWidget(cancel_button)

        layout.addLayout(button_layout)
        self.setLayout(layout)

    def get_selected_columns(self):
        # Return the list of column names that are checked
        return [col for col, checkbox in self.checkboxes.items() if checkbox.isChecked()]

# 主窗口
class MainWindow(QMainWindow):
    # Define signals
    import_code_requested = Signal(list) # New signal for codes
    import_data_requested = Signal(pd.DataFrame) # Restore signal for simulation data

    def __init__(self, data_path=None):
        super(MainWindow, self).__init__()
        self.setWindowTitle("WorldQuant Brain 回測結果瀏覽器")
        self.setMinimumSize(1200, 800)

        # 確定要使用的數據路徑
        if data_path is None:
            # 如果沒有提供路徑 (例如，直接運行此文件)，則使用預設相對路徑
            self.data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
        else:
            # 使用從 app.py 傳遞過來的路徑
            self.data_path = data_path

        # 設置主窗口佈局
        main_layout = QVBoxLayout()
        
        # 頂部佈局：導航與搜索
        top_layout = QHBoxLayout()
        
        # 創建標籤區域和搜索區域
        top_left = QHBoxLayout()
        top_right = QHBoxLayout()
        
        # 檔案標籤
        file_label = QLabel("回測檔案:")
        
        # 當前檔案標籤
        self.current_file_label = QLabel("未載入檔案")
        self.current_file_label.setStyleSheet("font-weight: bold; color: #1976D2;")
        
        # 搜尋元素
        search_label = QLabel("搜尋:")
        search_label.setStyleSheet("margin-left: 15px;")
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("輸入關鍵字...")
        self.search_input.textChanged.connect(self.filter_data)
        self.search_input.setMinimumWidth(250)
        self.search_input.setStyleSheet("padding: 4px; border: 1px solid #bdbdbd; border-radius: 4px;")
        
        # 欄位選擇下拉框
        field_label = QLabel("欄位:")
        self.filter_column = QComboBox()
        self.filter_column.addItem("全部欄位")
        self.filter_column.currentIndexChanged.connect(self.filter_data)
        self.filter_column.setMinimumWidth(120)
        
        # 添加刷新按鈕
        refresh_button = QPushButton("刷新")
        refresh_button.setToolTip("重新載入目前選擇的回測結果")
        refresh_button.clicked.connect(self.refresh_current_file)
        refresh_button.setStyleSheet("padding: 4px 12px;")

        # 添加儲存選取按鈕
        self.save_selected_button = QPushButton("儲存選取")
        self.save_selected_button.setToolTip("將勾選的資料儲存為新的CSV檔案")
        self.save_selected_button.clicked.connect(self.save_selected_rows)
        self.save_selected_button.setStyleSheet("padding: 4px 12px;")
        self.save_selected_button.setEnabled(False) # Initially disabled

        # 添加條件過濾元素
        self.condition_label = QLabel("條件過濾:")
        self.condition_column = QComboBox()  # 選擇要過濾的欄位
        self.condition_operator = QComboBox()  # 選擇操作符 >, <, =, >=, <=
        self.condition_operator.addItems([">", "<", "=", ">=", "<="])
        self.condition_value = QLineEdit()  # 輸入過濾值
        self.condition_value.setPlaceholderText("輸入數值...")
        self.condition_value.setMaximumWidth(100)
        
        apply_filter_button = QPushButton("應用過濾")
        apply_filter_button.clicked.connect(self.apply_numeric_filter)
        
        clear_filter_button = QPushButton("清除過濾")
        clear_filter_button.clicked.connect(self.clear_numeric_filter)
        
        # 布局左側標籤區域
        top_left.addWidget(file_label)
        top_left.addWidget(self.current_file_label)
        top_left.addStretch()
        
        # 布局右側搜索區域
        top_right.addWidget(search_label)
        top_right.addWidget(self.search_input)
        top_right.addWidget(field_label)
        top_right.addWidget(self.filter_column)
        top_right.addWidget(self.condition_label)
        top_right.addWidget(self.condition_column)
        top_right.addWidget(self.condition_operator)
        top_right.addWidget(self.condition_value)
        top_right.addWidget(apply_filter_button)
        top_right.addWidget(clear_filter_button)
        top_right.addWidget(refresh_button)

        # 添加欄位顯示選項按鈕及選單
        self.column_view_button = QPushButton("欄位視圖") # Renamed button
        self.column_view_button.setToolTip("選擇表格欄位的顯示方式")
        self.column_view_button.setStyleSheet("padding: 4px 12px;")
        self.column_view_button.setEnabled(False) # Initially disabled

        # 創建選單
        self.column_menu = QMenu(self)
        custom_action = QAction("自訂顯示...", self)
        results_only_action = QAction("只顯示結果欄位", self)
        custom_action.triggered.connect(self.open_custom_column_selector) # Renamed handler
        results_only_action.triggered.connect(self.show_results_only_columns) # New handler
        self.column_menu.addAction(custom_action)
        self.column_menu.addAction(results_only_action)

        # 將選單設置給按鈕
        self.column_view_button.setMenu(self.column_menu)

        top_right.addWidget(self.column_view_button)
        top_right.addWidget(self.save_selected_button)

        # --- Modification Start: Import Button with Menu ---
        self.import_button = QPushButton("匯入 Code") # Renamed button
        self.import_button.setToolTip("將 Code 匯入到生成器")
        self.import_button.setStyleSheet("padding: 4px 12px;")
        self.import_button.setEnabled(False) # Initially disabled

        # Create menu for import options
        self.import_menu = QMenu(self)
        self.import_selected_action = QAction("匯入已選 Code 至生成器", self)
        self.import_all_action = QAction("匯入所有 Code 至生成器", self)
        self.import_data_to_sim_action = QAction("匯入數據至模擬", self) # New action for simulation

        # Connect actions to handlers
        self.import_selected_action.triggered.connect(self.on_import_selected_code_clicked)
        self.import_all_action.triggered.connect(self.on_import_all_code_clicked)
        self.import_data_to_sim_action.triggered.connect(self.on_import_data_to_sim_clicked) # Connect new action

        # Add actions to menu
        self.import_menu.addAction(self.import_selected_action)
        self.import_menu.addAction(self.import_all_action)
        self.import_menu.addSeparator() # Add separator for clarity
        self.import_menu.addAction(self.import_data_to_sim_action)

        # Set menu for the button
        self.import_button.setMenu(self.import_menu)

        top_right.addWidget(self.import_button) # Add the new button with menu
        # --- Modification End ---

        # 組合左右兩側到頂部布局
        top_layout.addLayout(top_left, 1)
        top_layout.addLayout(top_right, 2)
        
        # 添加分隔線
        separator = QFrame()
        separator.setFrameShape(QFrame.HLine)
        separator.setFrameShadow(QFrame.Sunken)
        separator.setStyleSheet("background-color: #e0e0e0; margin: 5px 0;")
        
        main_layout.addLayout(top_layout)
        main_layout.addWidget(separator)
        
        # 中間佈局：左側文件列表和右側內容
        middle_layout = QHBoxLayout()
        
        # 左側文件列表
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setContentsMargins(0, 0, 0, 0)
        
        # 添加文件列表標題
        files_header = QHBoxLayout()
        files_title = QLabel("回測結果文件")
        files_title.setStyleSheet("font-weight: bold; font-size: 14px;")
        files_header.addWidget(files_title)
        files_header.addStretch()
        
        # 設置文件模型
        self.file_model = QFileSystemModel()
        # 使用 self.data_path 設置根路徑
        self.file_model.setRootPath(self.data_path)
        self.file_model.setNameFilters(["*.csv"])
        self.file_model.setNameFilterDisables(False)
        
        # 新增: 設置過濾代理模型
        self.file_proxy_model = FileFilterProxyModel()
        self.file_proxy_model.setSourceModel(self.file_model)

        self.file_view = QTreeView()
        self.file_view.setModel(self.file_proxy_model)
        # 使用 self.data_path 設置根索引 (需轉換為 proxy model 的 index)
        source_root_index = self.file_model.index(self.data_path)
        proxy_root_index = self.file_proxy_model.mapFromSource(source_root_index)
        self.file_view.setRootIndex(proxy_root_index)
        self.file_view.clicked.connect(self.load_file)
        self.file_view.setAnimated(True)
        self.file_view.setHeaderHidden(True)
        self.file_view.setStyleSheet("QTreeView { border: 1px solid #d0d0d0; border-radius: 4px; }")
        for i in range(1, 4):  # 隱藏不需要的列
            self.file_view.hideColumn(i)
            
        # 設置右鍵選單
        self.file_view.setContextMenuPolicy(Qt.CustomContextMenu)
        self.file_view.customContextMenuRequested.connect(self.show_file_context_menu)
            
        left_layout.addLayout(files_header)
        left_layout.addWidget(self.file_view)
        
        # 右側內容標籤頁
        self.tab_widget = QTabWidget()
        
        # 數據表格標籤頁
        self.table_view = QTableView()
        # self.table_view.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch) # Change resize mode later
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setSortingEnabled(True)
        # Set resize mode after model is set, or set specific column widths
        self.table_view.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive) # Allow manual resize
        self.table_view.horizontalHeader().setStretchLastSection(True) # Stretch last column
        
        # 數據圖表標籤頁
        self.chart_widget = QWidget()
        chart_layout = QVBoxLayout()
        
        self.canvas = FigureCanvas(plt.figure(figsize=(10, 8)))
        chart_controls = QHBoxLayout()
        
        self.chart_type = QComboBox()
        self.chart_type.addItems(["夏普比率分佈", "參數分析", "中性化方法比較", "截面分析"])
        self.chart_type.currentIndexChanged.connect(self.update_chart)
        
        chart_controls.addWidget(QLabel("圖表類型:"))
        chart_controls.addWidget(self.chart_type)
        chart_controls.addStretch()
        
        chart_layout.addLayout(chart_controls)
        chart_layout.addWidget(self.canvas)
        self.chart_widget.setLayout(chart_layout)
        
        # 添加標籤頁
        self.tab_widget.addTab(self.table_view, "回測結果表格")
        self.tab_widget.addTab(self.chart_widget, "回測結果視覺化")
        
        # 佈局左右兩側
        splitter = QSplitter(Qt.Horizontal)
        splitter.addWidget(left_widget)
        splitter.addWidget(self.tab_widget)
        splitter.setSizes([200, 1000])  # 設置初始大小比例
        
        middle_layout.addWidget(splitter)
        main_layout.addLayout(middle_layout)
        
        # 底部狀態欄
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        
        # 設置中央窗口
        central_widget = QWidget()
        central_widget.setLayout(main_layout)
        self.setCentralWidget(central_widget)
        
        # 數據相關
        self.current_dataset = None
        self.db_conn = None  # SQLite 連接
        self.table_name = "backtest_data"  # 固定使用的表格名稱
        self.proxy_model = None
        self.click_connected = False  # 跟踪點擊事件是否已連接
        self.visible_columns = [] # 新增: 追蹤可見欄位
        
        # 顯示初始信息
        self.status_bar.showMessage("選擇左側的回測結果文件以開始瀏覽")

    # 刷新當前文件
    def refresh_current_file(self):
        if not hasattr(self, 'last_loaded_index') or self.last_loaded_index is None:
            self.status_bar.showMessage("沒有已載入的回測結果可刷新")
            return
            
        self.load_file(self.last_loaded_index)
        self.status_bar.showMessage("已刷新當前回測結果")

    def load_file(self, index):
        # 將 proxy index 轉為 source index
        if isinstance(index.model(), QSortFilterProxyModel):
            source_index = index.model().mapToSource(index)
        else:
            source_index = index
        # 保存最後載入的索引，用於刷新功能
        self.last_loaded_index = index

        file_path = self.file_model.filePath(source_index)
        file_name = os.path.basename(file_path)

        try:
            # 如果已經有資料庫連線，先關閉它
            if self.db_conn is not None:
                self.db_conn.close()
                self.db_conn = None

            # 建立新的記憶體中的 SQLite 資料庫
            self.db_conn = sqlite3.connect(':memory:')

            # 使用分塊讀取 CSV 檔案
            chunk_size = 10000  # 每次讀取 10,000 行
            chunks = pd.read_csv(file_path, chunksize=chunk_size)

            # 讀取第一個區塊來獲取列名和設置表格結構
            first_chunk = next(chunks)
            columns = first_chunk.columns
            self.current_dataset = first_chunk  # 保存第一個區塊用於圖表顯示

            # 創建表格並插入第一個區塊的資料
            first_chunk.to_sql(self.table_name, self.db_conn, index=False)

            # 讀取並插入剩餘的區塊
            for chunk in chunks:
                chunk.to_sql(self.table_name, self.db_conn, if_exists='append', index=False)

            # 創建 SqliteTableModel
            model = SqliteTableModel()
            model.setup_model(self.db_conn, self.table_name)

            # 設置代理模型
            self.proxy_model = NumericFilterProxyModel()
            self.proxy_model.setSourceModel(model)
            self.proxy_model.setFilterCaseSensitivity(Qt.CaseInsensitive)
            self.table_view.setModel(self.proxy_model)

            # Connect click handler only once
            if not self.click_connected:
                self.table_view.clicked.connect(self.handle_cell_click)
                self.click_connected = True

            # Set fixed width for checkbox column and resize others
            self.table_view.setColumnWidth(0, 40) # Checkbox column width
            # Optionally resize other columns based on content
            # self.table_view.resizeColumnsToContents()
            # Or stretch all columns except the first one
            header = self.table_view.horizontalHeader()
            for i in range(1, self.proxy_model.columnCount()):
                header.setSectionResizeMode(i, QHeaderView.Interactive)
            header.setSectionResizeMode(0, QHeaderView.Fixed) # Keep checkbox column fixed width
            header.setStretchLastSection(False) # Adjust if needed


            # Update status bar
            file_name = os.path.basename(file_path)
            self.status_bar.showMessage(f"已載入 {file_name} | 共 {model.row_count} 條記錄")
            self.current_file_label.setText(file_name)

            # Update filter dropdowns and visible columns tracker
            cursor = self.db_conn.cursor()
            self.filter_column.clear()
            self.filter_column.addItem("全部欄位")
            self.condition_column.clear()
            self.visible_columns = list(model.columns)  # Initially all columns are visible

            for col in model.columns:
                self.filter_column.addItem(col)
                try:
                    # 執行 SQL 查詢來測試欄位是否包含數值
                    query = f"SELECT CAST({col} AS FLOAT) FROM {self.table_name} LIMIT 1"
                    cursor = self.db_conn.cursor()
                    cursor.execute(query)
                    if cursor.fetchone() is not None:
                        self.condition_column.addItem(col)
                except:
                    continue  # Skip if column cannot be cast to float


            # Enable buttons
            self.save_selected_button.setEnabled(True)
            self.column_view_button.setEnabled(True)
            self.import_button.setEnabled(True) # Enable the new import button

            self.apply_column_visibility()

            # Update chart
            self.update_chart()

        except Exception as e:
            QMessageBox.critical(self, "錯誤", f"載入回測結果時發生錯誤：{str(e)}")
            self.save_selected_button.setEnabled(False)
            self.column_view_button.setEnabled(False)
            self.import_button.setEnabled(False) # Disable import button on error
            self.current_dataset = None
            self.proxy_model = None
            self.visible_columns = []

    def filter_data(self):
        if self.proxy_model is None or not isinstance(self.proxy_model.sourceModel(), SqliteTableModel):
            return

        filter_text = self.search_input.text()
        filter_column_name = self.filter_column.currentText()

        source_model = self.proxy_model.sourceModel()
        if not filter_text:
            source_model.set_filter("")
        else:
            # 構建 SQL LIKE 查詢
            if filter_column_name == "全部欄位":
                # 搜尋所有列
                where_clauses = []
                for col in source_model.columns:
                    where_clauses.append(f"{col} LIKE '%{filter_text}%'")
                filter_clause = " OR ".join(where_clauses)
            else:
                # 搜尋特定列
                filter_clause = f"{filter_column_name} LIKE '%{filter_text}%'"

            source_model.set_filter(filter_clause)

        # Update status bar
        filtered_count = source_model.rowCount()
        total_count = source_model.row_count
        self.status_bar.showMessage(f"顯示 {filtered_count}/{total_count} 條記錄 | 過濾條件: {filter_column_name} - '{filter_text}'")

    def update_chart(self):
        if self.proxy_model is None or not isinstance(self.proxy_model.sourceModel(), SqliteTableModel):
            return

        source_model = self.proxy_model.sourceModel()
        filter_clause = source_model._get_filter_and_sort_clause()
        cursor = source_model.db_conn.cursor()

        # 清除當前圖表
        self.canvas.figure.clear()
        ax = self.canvas.figure.add_subplot(111)
        
        chart_type = self.chart_type.currentText()
        
        # 增加字體大小使圖表更清晰
        plt.rcParams.update({'font.size': 12})
        
        if chart_type == "夏普比率分佈":
            try:
                # 使用 SQL 查詢獲取夏普比率
                cursor.execute(f"SELECT CAST(sharpe AS FLOAT) FROM {source_model.table_name} {filter_clause}")
                sharpe_values = [row[0] for row in cursor.fetchall() if row[0] is not None]
                
                if sharpe_values:
                    # 創建直方圖
                    ax.hist(sharpe_values, bins=15, color='skyblue', edgecolor='black')
                    ax.set_title('夏普比率分佈')
                    ax.set_xlabel('夏普比率')
                    ax.set_ylabel('策略數量')
                    
                    # 添加垂直線標記 1.0 和 1.5 的夏普比率
                    ax.axvline(x=1.0, color='orange', linestyle='--', label='夏普=1.0')
                    ax.axvline(x=1.5, color='green', linestyle='--', label='夏普=1.5')
                    ax.legend()
                else:
                    ax.text(0.5, 0.5, '沒有有效的夏普比率數據', ha='center', va='center', transform=ax.transAxes)
            except Exception as e:
                ax.text(0.5, 0.5, f'無法生成夏普比率分佈圖: {str(e)}', ha='center', va='center', transform=ax.transAxes)
            
        elif chart_type == "參數分析":
            try:
                # 使用 SQL 查詢計算各個 decay 值的平均夏普比率
                query = f"""
                    SELECT decay, AVG(CAST(sharpe AS FLOAT)) as avg_sharpe
                    FROM {source_model.table_name}
                    {filter_clause.replace('WHERE', 'WHERE decay IS NOT NULL AND') if filter_clause else 'WHERE decay IS NOT NULL'}
                    GROUP BY decay
                    ORDER BY decay
                """
                cursor.execute(query)
                results = cursor.fetchall()
                
                if results:
                    decays = [str(row[0]) for row in results]
                    avg_sharpes = [row[1] for row in results]
                    
                    # 繪製條形圖
                    bars = ax.bar(decays, avg_sharpes, color='lightgreen')
                    ax.set_title('不同衰減參數的平均夏普比率')
                    ax.set_xlabel('衰減參數')
                    ax.set_ylabel('平均夏普比率')
                    
                    # 為條形圖添加數值標籤
                    for bar in bars:
                        height = bar.get_height()
                        ax.text(bar.get_x() + bar.get_width()/2., height + 0.01,
                               f'{height:.2f}', ha='center', va='bottom')
                else:
                    ax.text(0.5, 0.5, '沒有有效的參數分析數據', ha='center', va='center', transform=ax.transAxes)
            except Exception as e:
                ax.text(0.5, 0.5, f'無法生成參數分析圖: {str(e)}', ha='center', va='center', transform=ax.transAxes)
            
        elif chart_type == "中性化方法比較":
            try:
                # 使用 SQL 查詢計算各個中性化方法的平均夏普比率
                query = f"""
                    SELECT neutralization, AVG(CAST(sharpe AS FLOAT)) as avg_sharpe
                    FROM {source_model.table_name}
                    {filter_clause.replace('WHERE', 'WHERE neutralization IS NOT NULL AND') if filter_clause else 'WHERE neutralization IS NOT NULL'}
                    GROUP BY neutralization
                    ORDER BY avg_sharpe DESC
                """
                cursor.execute(query)
                results = cursor.fetchall()
                
                if results:
                    methods = [str(row[0]) for row in results]
                    avg_sharpes = [row[1] for row in results]
                    
                    # 繪製條形圖
                    bars = ax.bar(methods, avg_sharpes, color='coral')
                    ax.set_title('不同中性化方法的平均夏普比率')
                    ax.set_xlabel('中性化方法')
                    ax.set_ylabel('平均夏普比率')
                    
                    # 為條形圖添加數值標籤
                    for bar in bars:
                        height = bar.get_height()
                        ax.text(bar.get_x() + bar.get_width()/2., height + 0.01,
                               f'{height:.2f}', ha='center', va='bottom')
                else:
                    ax.text(0.5, 0.5, '沒有有效的中性化方法數據', ha='center', va='center', transform=ax.transAxes)
            except Exception as e:
                ax.text(0.5, 0.5, f'無法生成中性化方法比較圖: {str(e)}', ha='center', va='center', transform=ax.transAxes)
            
        elif chart_type == "截面分析":
            try:
                # 使用 SQL 查詢獲取夏普比率和換手率
                query = f"""
                    SELECT CAST(turnover AS FLOAT) as turnover, CAST(sharpe AS FLOAT) as sharpe
                    FROM {source_model.table_name}
                    {filter_clause.replace('WHERE', 'WHERE turnover IS NOT NULL AND sharpe IS NOT NULL AND') if filter_clause else 'WHERE turnover IS NOT NULL AND sharpe IS NOT NULL'}
                """
                cursor.execute(query)
                results = cursor.fetchall()
                
                if results:
                    import numpy as np
                    from scipy import stats
                    
                    turnover_values = [row[0] for row in results]
                    sharpe_values = [row[1] for row in results]
                    
                    # 繪製散點圖
                    ax.scatter(turnover_values, sharpe_values, alpha=0.5, c='blue')
                    ax.set_title('夏普比率與換手率關係')
                    ax.set_xlabel('換手率')
                    ax.set_ylabel('夏普比率')
                    
                    # 添加趨勢線
                    try:
                        if len(turnover_values) > 1:
                            slope, intercept, r_value, p_value, std_err = stats.linregress(turnover_values, sharpe_values)
                            x = np.array([min(turnover_values), max(turnover_values)])
                            ax.plot(x, intercept + slope*x, 'r', 
                                   label=f'趨勢線 (r²={r_value**2:.2f})')
                            ax.legend()
                    except:
                        pass  # 如果無法添加趨勢線，就跳過
                else:
                    ax.text(0.5, 0.5, '沒有有效的截面分析數據', ha='center', va='center', transform=ax.transAxes)
            except Exception as e:
                ax.text(0.5, 0.5, f'無法生成截面分析圖: {str(e)}', ha='center', va='center', transform=ax.transAxes)
        
        # 調整圖表佈局以確保所有元素都顯示
        self.canvas.figure.tight_layout()
        self.canvas.draw()

    # 處理單元格點擊事件
    def handle_cell_click(self, index):
        if self.proxy_model is None or not isinstance(self.proxy_model.sourceModel(), SqliteTableModel):
            self.status_bar.showMessage("無法處理點擊：數據集或模型不存在")
            return

        # Ignore clicks on the checkbox column itself for link/code actions
        if index.column() == 0:
            return

        # Check if the view index is valid before mapping
        if not index.isValid():
            self.status_bar.showMessage("無法處理點擊：無效的視圖索引")
            return

        # Map view index (proxy) to source model index
        source_index = self.proxy_model.mapToSource(index)

        # Check if the source index is valid after mapping
        if not source_index.isValid():
            self.status_bar.showMessage("無法處理點擊：無法映射到有效的源索引")
            return

        source_model = self.proxy_model.sourceModel()
        row = source_index.row()
        col = source_index.column()

        try:
            cursor = source_model.db_conn.cursor()
            # 調整列索引（因為第一列是勾選框）
            column_name = source_model.columns[col - 1]

            # 使用 SQL 查詢獲取當前行的數據
            query = f"SELECT {column_name} FROM {source_model.table_name} {source_model._get_filter_and_sort_clause()} LIMIT 1 OFFSET {row}"
            cursor.execute(query)
            value = cursor.fetchone()

            if value is None:
                return

            value = str(value[0])

            if column_name.lower() == 'link' and value:
                try:
                    webbrowser.open(value)
                    self.status_bar.showMessage(f"已在瀏覽器中打開: {value}")
                except Exception as e:
                    self.status_bar.showMessage(f"無法打開連結: {str(e)}")

            elif column_name.lower() == 'code':
                title = "策略代碼詳細信息"
                try:
                    # 使用 SQL 查詢獲取相關的 link
                    cursor.execute(f"SELECT link FROM {source_model.table_name} {source_model._get_filter_and_sort_clause()} LIMIT 1 OFFSET {row}")
                    link_result = cursor.fetchone()
                    if link_result and link_result[0]:
                        link = str(link_result[0])
                        title = f"策略詳細信息 - {link.split('/')[-1]}"
                except Exception:
                    pass

                dialog = DetailDialog(title, value, self)
                dialog.exec()

            elif len(value) > 30:  # Show details for any long text
                dialog = DetailDialog(f"{column_name} - 詳細內容", value, self)
                dialog.exec()

        except Exception as e:
            self.status_bar.showMessage(f"處理單元格點擊時出錯: {str(e)}")

    # 應用數值過濾
    def apply_numeric_filter(self):
        if self.proxy_model is None or not isinstance(self.proxy_model.sourceModel(), SqliteTableModel):
            self.status_bar.showMessage("當前沒有數據可過濾")
            return

        column_name = self.condition_column.currentText()
        if not column_name:
            self.status_bar.showMessage("請先選擇要過濾的數值欄位")
            return

        operator = self.condition_operator.currentText()
        try:
            value = float(self.condition_value.text())
        except ValueError:
            self.status_bar.showMessage("請輸入有效數字進行過濾")
            return

        # 構建 SQL 過濾條件
        source_model = self.proxy_model.sourceModel()
        current_filter = source_model.filter_clause

        # 構建數值過濾條件
        numeric_filter = f"CAST({column_name} AS FLOAT) {operator} {value}"

        # 如果已經有過濾條件，則將新條件與現有條件組合
        if current_filter:
            combined_filter = f"({current_filter}) AND ({numeric_filter})"
        else:
            combined_filter = numeric_filter

        # 應用過濾
        source_model.set_filter(combined_filter)

        # 更新狀態欄
        filtered_count = source_model.rowCount()
        total_count = source_model.row_count
        self.status_bar.showMessage(f"顯示 {filtered_count}/{total_count} 條記錄 | 數值過濾條件: {column_name} {operator} {value}")

    # 清除數值過濾
    def clear_numeric_filter(self):
        if self.proxy_model is None or not isinstance(self.proxy_model.sourceModel(), SqliteTableModel):
            return

        source_model = self.proxy_model.sourceModel()
        source_model.set_filter("")
        self.condition_value.clear()
        # 如果有文字搜尋，重新套用
        self.filter_data()

    # 修改: 開啟自訂欄位選擇對話框
    def open_custom_column_selector(self): # Renamed method
        if self.current_dataset is None:
            QMessageBox.warning(self, "無數據", "請先載入回測結果文件。")
            return

        # Pass current columns and currently visible columns to the dialog
        dialog = ColumnSelectionDialog(list(self.current_dataset.columns), self.visible_columns, self)
        if dialog.exec():
            self.visible_columns = dialog.get_selected_columns()
            self.apply_column_visibility()
            self.status_bar.showMessage(f"已更新顯示欄位，顯示 {len(self.visible_columns)} 個欄位")

    # 新增: 只顯示結果相關欄位
    def show_results_only_columns(self):
        if self.current_dataset is None:
             QMessageBox.warning(self, "無數據", "請先載入回測結果文件。")
             return

        # Predefined list of columns for "results only" view
        results_columns = ['passed', 'sharpe', 'fitness', 'turnover', 'weight', 'subsharpe', 'correlation', 'link', 'code']

        # Filter the list to include only columns that actually exist in the current dataset
        self.visible_columns = [col for col in results_columns if col in self.current_dataset.columns]

        self.apply_column_visibility()
        self.status_bar.showMessage(f"已切換為只顯示結果欄位，顯示 {len(self.visible_columns)} 個欄位")


    # 新增: 應用欄位可見性
    def apply_column_visibility(self):
        if self.proxy_model is None or self.current_dataset is None:
            return

        header = self.table_view.horizontalHeader()
        # Remember model has +1 column (checkbox) compared to DataFrame
        model_column_count = self.proxy_model.columnCount()
        df_columns = list(self.current_dataset.columns)

        # Iterate through all potential model columns
        for model_col_index in range(model_column_count):
            # Checkbox column (index 0) is always visible
            if model_col_index == 0:
                header.setSectionHidden(model_col_index, False)
                continue

            # Map model column index back to DataFrame column index
            df_col_index = model_col_index - 1

            # Check if the DataFrame column index is valid
            if 0 <= df_col_index < len(df_columns):
                column_name = df_columns[df_col_index]
                # Hide column if its name is not in the visible_columns list
                is_hidden = column_name not in self.visible_columns
                header.setSectionHidden(model_col_index, is_hidden)
            else:
                # Hide any unexpected extra columns in the model
                header.setSectionHidden(model_col_index, True)

    # 新增: 儲存選取的資料列
    def save_selected_rows(self):
        if self.proxy_model is None or not isinstance(self.proxy_model.sourceModel(), SqliteTableModel):
            QMessageBox.warning(self, "無法儲存", "沒有載入的數據集。")
            return

        source_model = self.proxy_model.sourceModel()
        checked_rowids = source_model.get_checked_rows()

        if not checked_rowids:
            QMessageBox.information(self, "沒有選取", "請先勾選要儲存的資料列。")
            return

        # 只詢問檔名，並儲存在 DATA_DIR
        # 取得 DATA_DIR（與 app.py 保持一致）
        DATA_DIR = "data"
        filename, ok = QInputDialog.getText(self, "儲存選取的資料", "請輸入檔名（不含副檔名）:")
        
        if ok and filename.strip():
            file_path = os.path.join(DATA_DIR, filename.strip() + ".csv")
            try:
                # 構建 SQL 查詢
                rowids_str = ",".join(map(str, checked_rowids))
                cursor = source_model.db_conn.cursor()
                
                # 獲取所有欄位的數據
                cursor.execute(f"SELECT * FROM {source_model.table_name} WHERE rowid IN ({rowids_str})")
                rows = cursor.fetchall()
                
                # 使用 pandas 將數據保存為 CSV
                df = pd.DataFrame(rows, columns=source_model.columns)
                df.to_csv(file_path, index=False, encoding='utf-8-sig')
                
                self.status_bar.showMessage(f"已將 {len(df)} 筆選取的資料儲存至 {os.path.basename(file_path)}")
                QMessageBox.information(self, "儲存成功", f"已成功儲存 {len(df)} 筆資料至:\n{file_path}")
                
            except Exception as e:
                QMessageBox.critical(self, "儲存失敗", f"儲存檔案時發生錯誤：\n{str(e)}")
                self.status_bar.showMessage(f"儲存檔案失敗: {str(e)}")
        else:
            self.status_bar.showMessage("已取消儲存選取的資料")

    # --- New Methods for Import Actions ---
    @Slot()
    def on_import_selected_code_clicked(self):
        """處理 '匯入已選 Code' 選項"""
        if self.proxy_model is None or not isinstance(self.proxy_model.sourceModel(), SqliteTableModel):
            QMessageBox.warning(self, "無法匯入", "沒有載入的數據集。")
            self.status_bar.showMessage("無法匯入：未載入數據")
            return

        source_model = self.proxy_model.sourceModel()

        # 獲取已勾選的 rowid 列表
        checked_rowids = source_model.get_checked_rows()
        if not checked_rowids:
            QMessageBox.information(self, "沒有選取", "請先勾選要匯入的資料列。")
            self.status_bar.showMessage("未匯入：沒有勾選資料列")
            return

        try:
            # 構建 SQL 查詢
            rowids_str = ",".join(map(str, checked_rowids))
            cursor = source_model.db_conn.cursor()
            cursor.execute(f"SELECT code FROM {source_model.table_name} WHERE rowid IN ({rowids_str})")
            selected_codes = [str(row[0]) for row in cursor.fetchall()]

            if selected_codes:
                # 發送信號
                self.import_code_requested.emit(selected_codes)
                self.status_bar.showMessage(f"已請求將 {len(selected_codes)} 個選取的 Code 匯入生成器")
            else:
                QMessageBox.warning(self, "無有效代碼", "選取的資料中沒有找到有效的代碼。")
                self.status_bar.showMessage("匯入失敗：無有效代碼")

        except Exception as e:
            QMessageBox.critical(self, "匯入失敗", f"提取選取的 Code 時發生錯誤：\n{str(e)}")
            self.status_bar.showMessage(f"匯入選取的 Code 失敗: {str(e)}")

    @Slot()
    def on_import_all_code_clicked(self):
        """處理 '匯入所有 Code' 選項"""
        if self.proxy_model is None or not isinstance(self.proxy_model.sourceModel(), SqliteTableModel):
            QMessageBox.warning(self, "無法匯入", "沒有載入的數據集。")
            self.status_bar.showMessage("無法匯入：未載入數據")
            return

        source_model = self.proxy_model.sourceModel()

        try:
            # 構建 SQL 查詢，包含當前過濾條件
            filter_clause = source_model._get_filter_and_sort_clause()
            cursor = source_model.db_conn.cursor()
            query = f"SELECT code FROM {source_model.table_name} {filter_clause}"
            cursor.execute(query)
            all_codes = [str(row[0]) for row in cursor.fetchall()]

            if all_codes:
                # 發送信號
                self.import_code_requested.emit(all_codes)
                self.status_bar.showMessage(f"已請求將全部 {len(all_codes)} 個 Code 匯入生成器")
            else:
                QMessageBox.warning(self, "無有效代碼", "當前數據中沒有找到有效的代碼。")
                self.status_bar.showMessage("匯入失敗：無有效代碼")

        except Exception as e:
            QMessageBox.critical(self, "匯入失敗", f"提取所有 Code 時發生錯誤：\n{str(e)}")
            self.status_bar.showMessage(f"匯入所有 Code 失敗: {str(e)}")

    @Slot()
    def on_import_data_to_sim_clicked(self):
        """處理 '匯入數據至模擬' 選項"""
        if self.proxy_model is None or not isinstance(self.proxy_model.sourceModel(), SqliteTableModel):
            QMessageBox.warning(self, "無數據", "請先載入回測結果文件。")
            self.status_bar.showMessage("無法匯入：未載入數據")
            return

        source_model = self.proxy_model.sourceModel()
        try:
            # 構建 SQL 查詢，包含當前過濾條件
            filter_clause = source_model._get_filter_and_sort_clause()
            cursor = source_model.db_conn.cursor()
            # 獲取所有列的數據
            cursor.execute(f"SELECT * FROM {source_model.table_name} {filter_clause}")
            rows = cursor.fetchall()
            
            if rows:
                # 將數據轉換為 DataFrame
                df = pd.DataFrame(rows, columns=source_model.columns)
                # 發送信號
                self.import_data_requested.emit(df)
                self.status_bar.showMessage(f"已請求將 {len(df)} 筆數據匯入模擬")
            else:
                QMessageBox.warning(self, "無數據", "當前過濾條件下沒有可用的數據。")
                self.status_bar.showMessage("匯入失敗：無可用數據")

        except Exception as e:
            QMessageBox.critical(self, "匯入失敗", f"提取數據時發生錯誤：\n{str(e)}")
            self.status_bar.showMessage(f"匯入數據失敗: {str(e)}")

    def show_file_context_menu(self, position):
        """顯示文件右鍵選單"""
        # 取得點擊位置的索引
        index = self.file_view.indexAt(position)
        if not index.isValid():
            return

        # 將 proxy index 轉換為 source index
        source_index = self.file_proxy_model.mapToSource(index)
        file_path = self.file_model.filePath(source_index)
        file_name = os.path.basename(file_path)

        # 創建選單
        menu = QMenu()
        rename_action = menu.addAction("重命名")
        delete_action = menu.addAction("刪除")

        # 執行選單並取得選擇的動作
        action = menu.exec_(self.file_view.viewport().mapToGlobal(position))

        if action == rename_action:
            self.rename_file(file_path)
        elif action == delete_action:
            self.delete_file(file_path)

    def rename_file(self, file_path):
        """重命名文件"""
        old_name = os.path.basename(file_path)
        new_name, ok = QInputDialog.getText(
            self, 
            "重命名文件",
            "請輸入新的文件名稱：",
            text=old_name
        )

        if ok and new_name and new_name != old_name:
            try:
                new_path = os.path.join(os.path.dirname(file_path), new_name)
                # 檢查新文件名是否已存在
                if os.path.exists(new_path):
                    QMessageBox.warning(self, "錯誤", f"文件 '{new_name}' 已存在。")
                    return

                os.rename(file_path, new_path)
                self.status_bar.showMessage(f"已將文件 '{old_name}' 重命名為 '{new_name}'")

                # 如果重命名的是當前載入的文件，更新標籤
                if hasattr(self, 'current_file_label') and old_name == self.current_file_label.text():
                    self.current_file_label.setText(new_name)

            except Exception as e:
                QMessageBox.critical(self, "錯誤", f"重命名文件時發生錯誤：\n{str(e)}")

    def delete_file(self, file_path):
        """刪除文件"""
        file_name = os.path.basename(file_path)
        reply = QMessageBox.question(
            self,
            "確認刪除",
            f"確定要刪除文件 '{file_name}' 嗎？\n此操作無法恢復。",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )

        if reply == QMessageBox.Yes:
            try:
                os.remove(file_path)
                self.status_bar.showMessage(f"已刪除文件 '{file_name}'")

                # 如果刪除的是當前載入的文件，清空顯示
                if hasattr(self, 'current_file_label') and file_name == self.current_file_label.text():
                    self.current_file_label.setText("未載入檔案")
                    self.current_dataset = None
                    if self.proxy_model and isinstance(self.proxy_model.sourceModel(), DataFrameModel):
                        model = self.proxy_model.sourceModel()
                        model._data = pd.DataFrame()  # Empty the data
                        model.layoutChanged.emit()
                    self.save_selected_button.setEnabled(False)
                    self.column_view_button.setEnabled(False)
                    self.import_button.setEnabled(False)

            except Exception as e:
                QMessageBox.critical(self, "錯誤", f"刪除文件時發生錯誤：\n{str(e)}")


# ... (if __name__ == "__main__": block remains the same) ...

if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    # 設置應用程序樣式
    app.setStyle("Fusion")
    
    # 檢查 *預設* 數據目錄是否存在 (因為直接運行時使用預設路徑)
    default_data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    if not os.path.exists(default_data_path):
        QMessageBox.warning(None, "警告", f"找不到預設數據目錄: {default_data_path}\n請確保程式所在目錄中存在data資料夾。")
    
    # 創建 MainWindow 時不傳遞 data_path，讓它使用預設值
    window = MainWindow()
    window.show()
    sys.exit(app.exec())

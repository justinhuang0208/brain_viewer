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
from PySide6.QtCore import Qt, QDir, QModelIndex, QSortFilterProxyModel, Signal, Slot, QAbstractTableModel, QRegularExpression, QTimer # Added QTimer
from PySide6.QtGui import QColor, QFont, QPalette, QIcon, QAction # Added QAction
import pandas as pd
import matplotlib.pyplot as plt
from PySide6.QtGui import QGuiApplication
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

    def mapFromSource(self, sourceIndex):
        # 避免非來源模型的索引引發錯誤警告
        if sourceIndex.model() is not self.sourceModel():
            return QModelIndex()
        return super(NumericFilterProxyModel, self).mapFromSource(sourceIndex)

    def mapToSource(self, proxyIndex):
        # 避免非本代理模型的索引引發錯誤警告
        if proxyIndex.model() is not self:
            return QModelIndex()
        return super(NumericFilterProxyModel, self).mapToSource(proxyIndex)

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

    def mapFromSource(self, sourceIndex):
        if sourceIndex.model() is not self.sourceModel():
            return QModelIndex()
        return super(FileFilterProxyModel, self).mapFromSource(sourceIndex)

    def mapToSource(self, proxyIndex):
        if proxyIndex.model() is not self:
            return QModelIndex()
        return super(FileFilterProxyModel, self).mapToSource(proxyIndex)

# SQLite表格模型
class SqliteTableModel(QAbstractTableModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.db_conn = None
        self.table_name = None
        self.columns = []
        self.row_count = 0
        # self.filter_clause = "" # 移除舊的 filter_clause
        # --- 修改：使用字典儲存不同類型的過濾條件 ---
        self.filter_conditions = {'text': None, 'numeric': None, 'raw': None, 'rowid': None}
        self._raw_filter_clause = "" # 臨時：用於 '全部欄位' 的 raw SQL
        # -----------------------------------------
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
        clause, params = self._get_filter_and_sort_clause() # 修改：獲取子句和參數
        query = f"SELECT COUNT(*) FROM {self.table_name} {clause}"
        try:
            cursor.execute(query, params) # 修改：使用參數化查詢
            return cursor.fetchone()[0]
        except Exception as e:
            print(f"Error counting rows: {e}")
            return 0

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
                # --- 修改：先獲取 rowid ---
                rowid = self._get_rowid_for_row(row)
                if rowid is not None:
                    return self._check_states.get(rowid, Qt.Unchecked)
            return None

        # 獲取實際數據
        try:
            # --- 修改：先獲取 rowid ---
            rowid = self._get_rowid_for_row(row)
            if rowid is None:
                return None

            cursor = self.db_conn.cursor()
            # 調整列索引（因為第一列是勾選框）
            actual_col = self.columns[col - 1]
            # --- 修改：使用 rowid 獲取數據 ---
            query = f'SELECT "{actual_col}" FROM {self.table_name} WHERE rowid = ?'
            cursor.execute(query, (rowid,))
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
                # --- 修改：先獲取 rowid ---
                rowid = self._get_rowid_for_row(index.row())
                if rowid is not None:
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

    # --- 新增：獨立更新不同類型過濾器的方法 ---
    def update_text_filter(self, column, text):
        """更新文字過濾條件"""
        self.filter_conditions['text'] = {'col': column, 'op': 'LIKE', 'val': f'%{text}%'}
        self.filter_conditions['raw'] = None # 清除 raw SQL
        self.filter_conditions['rowid'] = None # 清除 rowid 過濾
        self.beginResetModel()
        self.endResetModel()

    def update_numeric_filter(self, column, operator, value):
        """更新數值過濾條件"""
        self.filter_conditions['numeric'] = {'col': column, 'op': operator, 'val': value}
        self.filter_conditions['raw'] = None # 清除 raw SQL
        self.filter_conditions['rowid'] = None # 清除 rowid 過濾
        self.layoutChanged.emit()

    def clear_text_filter(self):
        """清除文字過濾條件"""
        if self.filter_conditions['text'] is not None:
            self.filter_conditions['text'] = None
            self.beginResetModel()
            self.endResetModel()

    def clear_numeric_filter(self):
        """清除數值過濾條件"""
        if self.filter_conditions['numeric'] is not None:
            self.filter_conditions['numeric'] = None
            self.layoutChanged.emit()

    def set_filter_raw_sql(self, sql_clause):
        """設置原始 SQL WHERE 子句 (用於 '全部欄位' 臨時方案)"""
        # 警告：此方法不使用參數化查詢，可能存在風險和效能問題
        self.filter_conditions = {'text': None, 'numeric': None, 'raw': sql_clause, 'rowid': None}
        self._raw_filter_clause = sql_clause # 保存 raw SQL 以便狀態欄顯示
        self.layoutChanged.emit()

    def set_filter_by_rowids(self, rowids):
        """根據 rowid 列表設置過濾"""
        if rowids is None or not isinstance(rowids, (list, tuple)):
             self.filter_conditions['rowid'] = None
        else:
             self.filter_conditions['rowid'] = {'col': 'rowid', 'op': 'IN', 'val': tuple(rowids)}
        # 清除其他過濾器
        self.filter_conditions['text'] = None
        self.filter_conditions['numeric'] = None
        self.filter_conditions['raw'] = None
        self.layoutChanged.emit()
    # --- 新增結束 ---

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

    def _build_where_clause(self):
        """(私有) 根據 filter_conditions 建立 WHERE 子句和參數列表"""
        clauses = []
        params = []

        # 優先級：raw > rowid > text/numeric
        if self.filter_conditions.get('raw'):
            # 警告：Raw SQL 不使用參數化
            print("警告：正在使用 raw SQL 過濾，未進行參數化。")
            return f"WHERE {self.filter_conditions['raw']}", []
        elif self.filter_conditions.get('rowid'):
            cond = self.filter_conditions['rowid']
            val = cond.get('val')
            if isinstance(val, (list, tuple)) and val:
                placeholders = ','.join('?' * len(val))
                clauses.append(f'rowid IN ({placeholders})')
                params.extend(val)
            else: # 如果 rowid 列表為空，則不返回任何結果
                 clauses.append('1 = 0') # False condition
        else:
            # 組合 text 和 numeric 過濾
            text_cond = self.filter_conditions.get('text')
            numeric_cond = self.filter_conditions.get('numeric')

            if text_cond:
                col = text_cond.get('col')
                op = text_cond.get('op', 'LIKE')
                val = text_cond.get('val')
                if col and col in self.columns:
                    clauses.append(f'"{col}" {op} ?')
                    params.append(val)

            if numeric_cond:
                col = numeric_cond.get('col')
                op = numeric_cond.get('op', '=')
                val = numeric_cond.get('val')
                if col and col in self.columns:
                    # 數值比較通常不需要 CAST，除非欄位類型是 TEXT
                    # 為了安全起見，可以加上 CAST
                    clauses.append(f'CAST("{col}" AS REAL) {op} ?')
                    params.append(val)

        if not clauses:
            return "", []

        return "WHERE " + " AND ".join(clauses), params

    def _get_filter_and_sort_clause(self):
        """(私有) 組合過濾和排序子句及參數"""
        where_clause, params = self._build_where_clause()
        full_clause = where_clause
        if self.sort_clause:
            full_clause += f" {self.sort_clause}"
        return full_clause, params

    def _get_rowid_for_row(self, view_row):
        """(私有) 根據視圖行號獲取對應的 rowid"""
        if not self.db_conn or not self.table_name:
            return None
        try:
            cursor = self.db_conn.cursor()
            clause, params = self._get_filter_and_sort_clause()
            query = f"SELECT rowid FROM {self.table_name} {clause} LIMIT 1 OFFSET {view_row}"
            cursor.execute(query, params)
            result = cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            print(f"Error getting rowid for view_row {view_row}: {e}")
            return None

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
        # 新增：載入狀態旗標
        self.loading = False

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
        # --- 修改：使用計時器延遲過濾 ---
        # self.search_input.textChanged.connect(self.filter_data) # Removed direct connection
        self.search_input.textChanged.connect(self.on_search_text_changed) # Connect to timer handler
        self.search_input.setMinimumWidth(250)
        self.search_input.setStyleSheet("padding: 4px; border: 1px solid #bdbdbd; border-radius: 4px;")

        # 創建用於延遲過濾的計時器
        self.filter_timer = QTimer(self)
        self.filter_timer.setSingleShot(True)
        self.filter_timer.setInterval(300) # 300 毫秒延遲
        self.filter_timer.timeout.connect(self.filter_data)
        # --- 修改結束 ---
        # 添加刷新按鈕
        refresh_button = QPushButton("刷新")
        refresh_button.setToolTip("重新載入目前選擇的回測結果")
        refresh_button.clicked.connect(self.refresh_current_file)
        refresh_button.setStyleSheet("padding: 4px 12px;")

        # 添加儲存選取按鈕
        # 合併「反轉選取」與「儲存選取」為下拉按鈕
        # (移除選取操作相關程式碼)

        # 合併「應用過濾」與「清除過濾」為下拉按鈕
        self.filter_menu_button = QPushButton("過濾操作")
        self.filter_menu_button.setToolTip("數值過濾相關操作")
        self.filter_menu_button.setStyleSheet("padding: 4px 12px;")
        self.filter_menu = QMenu(self)
        self.action_apply_filter = QAction("應用過濾", self)
        self.action_clear_filter = QAction("清除過濾", self)
        self.action_apply_filter.triggered.connect(self.apply_numeric_filter)
        self.action_clear_filter.triggered.connect(self.clear_numeric_filter)
        self.filter_menu.addAction(self.action_apply_filter)
        self.filter_menu.addAction(self.action_clear_filter)
        self.filter_menu_button.setMenu(self.filter_menu)

        # 添加條件過濾元素
        self.condition_label = QLabel("條件過濾:")
        self.condition_column = QComboBox()  # 選擇要過濾的欄位
        self.condition_operator = QComboBox()  # 選擇操作符 >, <, =, >=, <=
        self.condition_operator.addItems([">", "<", "=", ">=", "<="])
        self.condition_value = QLineEdit()  # 輸入過濾值
        self.condition_value.setPlaceholderText("輸入數值...")
        self.condition_value.setMaximumWidth(100)
        # 布局左側標籤區域
        top_left.addWidget(file_label)
        top_left.addWidget(self.current_file_label)
        top_left.addStretch()
        
        # 布局右側搜索區域
        top_right.addWidget(search_label)
        top_right.addWidget(self.search_input)
        # top_right.addWidget(field_label) # Removed
        # top_right.addWidget(self.filter_column) # Removed
        top_right.addWidget(self.condition_label)
        top_right.addWidget(self.condition_column)
        top_right.addWidget(self.condition_operator)
        top_right.addWidget(self.condition_value)
        top_right.addWidget(self.filter_menu_button)
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
        # === 合併選取操作下拉按鈕結束 ===
        
        # --- Modification Start: Import Button with Menu ---
        self.import_button = QPushButton("匯入 Code 至...") # Renamed button
        self.import_button.setToolTip("將 Code 匯入到生成器，或匯入數據至模擬表格")
        self.import_button.setStyleSheet("padding: 4px 12px;")
        self.import_button.setEnabled(False) # Initially disabled
        
        # Create menu for import options
        self.import_menu = QMenu(self)
        self.import_selected_action = QAction("匯入已選 Code 至生成器", self)
        self.import_all_action = QAction("匯入所有 Code 至生成器", self)
        # 新增：匯入選擇的數據至模擬
        self.import_selected_data_to_sim_action = QAction("匯入已選數據至模擬", self)
        self.import_data_to_sim_action = QAction("匯入所有數據至模擬", self) # 修改文字
        
        # Connect actions to handlers
        self.import_selected_action.triggered.connect(self.on_import_selected_code_clicked)
        self.import_all_action.triggered.connect(self.on_import_all_code_clicked)
        self.import_selected_data_to_sim_action.triggered.connect(self.on_import_selected_data_to_sim_clicked)
        self.import_data_to_sim_action.triggered.connect(self.on_import_data_to_sim_clicked) # Connect new action
        
        # Add actions to menu
        self.import_menu.addAction(self.import_selected_action)
        self.import_menu.addAction(self.import_all_action)
        self.import_menu.addSeparator() # Add separator for clarity
        
        # 新增：將匯入選擇的數據至模擬放在數據相關動作區
        self.import_menu.addAction(self.import_selected_data_to_sim_action)
        self.import_menu.addAction(self.import_data_to_sim_action)
        
        # Set menu for the button
        self.import_button.setMenu(self.import_menu)
        
        top_right.addWidget(self.import_button) # Add the new button with menu
        # --- Modification End ---
        
        # === 新增：匯出 Code 為 List 按鈕 ===
        # === 修改：匯出 Code 為 List 按鈕改為下拉選單按鈕 ===
        self.export_code_list_button = QPushButton("匯出...")
        self.export_code_list_button.setToolTip("將目前檢視的 CSV 轉成特定格式複製到剪貼板")
        self.export_code_list_button.setStyleSheet("padding: 4px 12px;")
        self.export_code_list_button.setEnabled(False)

        self.export_code_menu = QMenu(self)
        self.action_export_code_list = QAction("匯出 Code 為 List 格式", self)
        self.action_export_code_list.triggered.connect(self.export_code_column_as_list)
        self.export_code_menu.addAction(self.action_export_code_list)

        self.action_export_params_list = QAction("匯出參數為 Parameters 格式", self)
        self.action_export_params_list.triggered.connect(self.export_params_as_data_list)
        self.export_code_menu.addAction(self.action_export_params_list)

        self.export_code_list_button.setMenu(self.export_code_menu)
        top_right.addWidget(self.export_code_list_button)

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

        # 為表格視圖啟用右鍵選單
        self.table_view.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table_view.customContextMenuRequested.connect(self.show_table_context_menu)

    # 刷新當前文件
    def refresh_current_file(self):
        if not hasattr(self, 'last_loaded_index') or self.last_loaded_index is None:
            self.status_bar.showMessage("沒有已載入的回測結果可刷新")
            return
            
        self.load_file(self.last_loaded_index)
        self.status_bar.showMessage("已刷新當前回測結果")

    def load_file(self, index):
        # 防止重複載入
        if self.loading:
            self.status_bar.showMessage("檔案正在載入中，請稍候...")
            return

        # 設置載入標誌
        self.loading = True
        self.condition_value.clear()

        try:
            # 將 proxy index 轉為 source index
            if isinstance(index.model(), QSortFilterProxyModel):
                source_index = index.model().mapToSource(index)
            else:
                source_index = index
            # 保存最後載入的索引，用於刷新功能
            self.last_loaded_index = index

            file_path = self.file_model.filePath(source_index)
            file_name = os.path.basename(file_path)

            # 檢查檔案是否存在和可讀
            if not os.path.exists(file_path):
                QMessageBox.critical(self, "檔案不存在", f"找不到檔案: {file_path}")
                # --- 新增：清空狀態 ---
                self.current_dataset = None
                self.proxy_model = None
                self.visible_columns = []
                self.table_view.setModel(None)
                self.column_view_button.setEnabled(False)
                self.import_button.setEnabled(False)
                self.export_code_list_button.setEnabled(False)
                self.current_file_label.setText("錯誤")
                self.status_bar.showMessage(f"檔案不存在: {file_name}")
                if self.db_conn is not None:
                    self.db_conn.close()
                self.db_conn = None
                self.loading = False
                # --- 新增結束 ---
                return

            # 檢查檔案大小
            try:
                file_size = os.path.getsize(file_path)
            except OSError as e:
                self.status_bar.showMessage(f"無法讀取檔案大小：{str(e)}")
                QMessageBox.critical(self, "檔案讀取錯誤", f"無法讀取檔案大小: {str(e)}")
                # --- 新增：清空狀態 ---
                self.current_dataset = None
                self.proxy_model = None
                self.visible_columns = []
                self.table_view.setModel(None)
                self.column_view_button.setEnabled(False)
                self.import_button.setEnabled(False)
                self.export_code_list_button.setEnabled(False)
                self.current_file_label.setText(file_name)
                if self.db_conn is not None:
                    self.db_conn.close()
                self.db_conn = None
                self.loading = False
                # --- 新增結束 ---
                return

            if file_size == 0:
                QMessageBox(self).information("空檔案", "此 CSV 檔案為空，無法載入資料。").exec_()
                self.current_file_label.setText(file_name)
                self.status_bar.showMessage(f"{file_name} 為空檔案，未載入任何資料。")
                # --- 新增：清空狀態 ---
                self.current_dataset = None
                self.proxy_model = None
                self.visible_columns = []
                self.table_view.setModel(None)
                self.column_view_button.setEnabled(False)
                self.import_button.setEnabled(False)
                self.export_code_list_button.setEnabled(False)
                if self.db_conn is not None:
                    self.db_conn.close()
                self.db_conn = None
                self.loading = False
                # --- 新增結束 ---
                return

            # 關閉現有連接
            if self.db_conn is not None:
                self.db_conn.close()
                self.db_conn = None

            # 建立新的資料庫連接
            self.db_conn = sqlite3.connect(':memory:')

            # 改進CSV檔案讀取 - 增加更多檢查與錯誤處理
            chunk_size = 10000
            try:
                # 先嘗試讀取檔案前幾行來驗證格式
                with open(file_path, 'r', encoding='utf-8-sig') as f:
                    first_lines = []
                    for i in range(5):  # 讀取前5行
                        try:
                            line = next(f)
                            first_lines.append(line)
                        except StopIteration:
                            break
                    
                    # 檢查是否只有表頭或空行
                    if not first_lines or all(line.strip() == '' for line in first_lines):
                        QMessageBox.information(self, "空檔案", "此CSV檔案不包含有效資料。")
                        self.current_file_label.setText(file_name)
                        self.status_bar.showMessage(f"{file_name} 不包含有效資料。")
                        # --- 新增：清空狀態 ---
                        self.current_dataset = None
                        self.proxy_model = None
                        self.visible_columns = []
                        self.table_view.setModel(None)
                        self.column_view_button.setEnabled(False)
                        self.import_button.setEnabled(False)
                        self.export_code_list_button.setEnabled(False)
                        if self.db_conn is not None:
                            self.db_conn.close()
                        self.db_conn = None
                        self.loading = False
                        # --- 新增結束 ---
                        return
                
                # 嘗試使用pandas讀取檔案
                chunks = pd.read_csv(file_path, chunksize=chunk_size)
                try:
                    first_chunk = next(chunks)
                except StopIteration:
                    QMessageBox.information(self, "空檔案", "此 CSV 檔案沒有有效資料。")
                    self.current_file_label.setText(file_name)
                    self.status_bar.showMessage(f"{file_name} 沒有有效資料。")
                    # --- 新增：清空狀態 ---
                    self.current_dataset = None
                    self.proxy_model = None
                    self.visible_columns = []
                    self.table_view.setModel(None)
                    self.column_view_button.setEnabled(False)
                    self.import_button.setEnabled(False)
                    self.export_code_list_button.setEnabled(False)
                    if self.db_conn is not None:
                        self.db_conn.close()
                    self.db_conn = None
                    self.loading = False
                    # --- 新增結束 ---
                    return

                if first_chunk.empty:
                    QMessageBox.information(self, "空檔案", "此 CSV 檔案不包含任何資料列。")
                    self.current_file_label.setText(file_name)
                    self.status_bar.showMessage(f"{file_name} 不包含任何資料列。")
                    # --- 新增：清空狀態 ---
                    self.current_dataset = None
                    self.proxy_model = None
                    self.visible_columns = []
                    self.table_view.setModel(None)
                    self.column_view_button.setEnabled(False)
                    self.import_button.setEnabled(False)
                    self.export_code_list_button.setEnabled(False)
                    if self.db_conn is not None:
                        self.db_conn.close()
                    self.db_conn = None
                    self.loading = False
                    # --- 新增結束 ---
                    return
                    
                # 以下是成功讀取的後續處理...
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
                header = self.table_view.horizontalHeader()
                for i in range(1, self.proxy_model.columnCount()):
                    header.setSectionResizeMode(i, QHeaderView.Interactive)
                header.setSectionResizeMode(0, QHeaderView.Fixed) # Keep checkbox column fixed width
                header.setStretchLastSection(False) # Adjust if needed

                # Update status bar
                self.status_bar.showMessage(f"已載入 {file_name} | 共 {model.row_count} 條記錄")
                self.current_file_label.setText(file_name)

                # Update filter dropdowns and visible columns tracker
                cursor = self.db_conn.cursor()
                self.condition_column.clear()
                self.visible_columns = list(model.columns)  # Initially all columns are visible

                for col in model.columns:
                    if col.lower() in ['link', 'code', 'region']:
                        continue
                    try:
                        query = f"SELECT CAST(\"{col}\" AS FLOAT) FROM {self.table_name} LIMIT 1"
                        cursor = self.db_conn.cursor()
                        cursor.execute(query)
                        if cursor.fetchone() is not None:
                            self.condition_column.addItem(col)
                    except:
                        continue

                # Enable buttons
                self.column_view_button.setEnabled(True)
                self.import_button.setEnabled(True)
                self.export_code_list_button.setEnabled(True)
                self.action_export_code_list.setEnabled(True)
                self.action_export_params_list.setEnabled(True)

                self.apply_column_visibility()
                self.update_chart()
                # --- 成功讀取結束 ---
                
            except pd.errors.EmptyDataError:
                QMessageBox.information(self, "空檔案", "此 CSV 檔案為空或無有效資料。")
                self.current_file_label.setText(file_name)
                self.status_bar.showMessage(f"{file_name} 為空檔案或無有效資料。")
                # --- 新增：清空狀態 ---
                self.current_dataset = None
                self.proxy_model = None
                self.visible_columns = []
                self.table_view.setModel(None)
                self.column_view_button.setEnabled(False)
                self.import_button.setEnabled(False)
                self.export_code_list_button.setEnabled(False)
                if self.db_conn is not None:
                    self.db_conn.close()
                self.db_conn = None
                self.loading = False
                # --- 新增結束 ---
                return
            except (pd.errors.ParserError, UnicodeDecodeError) as e:
                QMessageBox.critical(self, "格式錯誤", f"CSV 檔案格式錯誤或無法解析：{str(e)}")
                self.current_file_label.setText(file_name)
                self.status_bar.showMessage(f"{file_name} 格式錯誤: {str(e)}")
                # --- 新增：清空狀態 ---
                self.current_dataset = None
                self.proxy_model = None
                self.visible_columns = []
                self.table_view.setModel(None)
                self.column_view_button.setEnabled(False)
                self.import_button.setEnabled(False)
                self.export_code_list_button.setEnabled(False)
                if self.db_conn is not None:
                    self.db_conn.close()
                self.db_conn = None
                self.loading = False
                # --- 新增結束 ---
                return
            except Exception as e:
                # 捕獲所有其他可能的異常
                QMessageBox.critical(self, "未知錯誤", f"讀取CSV檔案時發生未知錯誤: {str(e)}")
                self.current_file_label.setText(file_name)
                self.status_bar.showMessage(f"讀取 {file_name} 時發生錯誤: {str(e)}")
                # --- 新增：清空狀態 ---
                self.current_dataset = None
                self.proxy_model = None
                self.visible_columns = []
                self.table_view.setModel(None)
                self.column_view_button.setEnabled(False)
                self.import_button.setEnabled(False)
                self.export_code_list_button.setEnabled(False)
                if self.db_conn is not None:
                    self.db_conn.close()
                self.db_conn = None
                self.loading = False
                # --- 新增結束 ---
                return
                
            # ... 其餘代碼保持不變 ...

        except Exception as e:
            QMessageBox.critical(self, "錯誤", f"載入回測結果時發生錯誤：{str(e)}")
            self.current_file_label.setText(file_name if 'file_name' in locals() else "錯誤")
            self.status_bar.showMessage(f"載入過程中發生錯誤: {str(e)}")
            # 重置模型和資料
            self.current_dataset = None
            self.proxy_model = None
            self.visible_columns = []
            self.table_view.setModel(None)
            # --- 新增：禁用按鈕 ---
            self.column_view_button.setEnabled(False)
            self.import_button.setEnabled(False)
            self.export_code_list_button.setEnabled(False)
            if self.db_conn is not None:
                self.db_conn.close()
            self.db_conn = None
            # --- 新增結束 ---
            
        finally:
            # 重置loading狀態
            self.loading = False
            try:
                # 安全地觸發過濾
                if not self.loading and hasattr(self, 'filter_data'):
                    self.filter_data()
            except Exception as e:
                print(f"過濾數據時發生錯誤: {str(e)}")

    # --- 新增：處理搜尋框文字變更，啟動計時器 ---
    def on_search_text_changed(self):
        """當搜尋框文字改變時，重新啟動過濾計時器"""
        self.filter_timer.start()
    # --- 新增結束 ---

    def export_code_column_as_list(self):
        # 匯出目前載入的 CSV 檔案的 'code' 欄位為 Python list 並複製到剪貼簿
        try:
            # 檢查是否有載入檔案
            if not hasattr(self, 'last_loaded_index') or self.last_loaded_index is None:
                QMessageBox.warning(self, "無法匯出", "請先載入回測結果檔案。")
                return
    
            # 取得目前檔案路徑
            if isinstance(self.last_loaded_index.model(), QSortFilterProxyModel):
                source_index = self.last_loaded_index.model().mapToSource(self.last_loaded_index)
            else:
                source_index = self.last_loaded_index
            file_path = self.file_model.filePath(source_index)
    
            if not os.path.exists(file_path):
                QMessageBox.critical(self, "錯誤", f"找不到檔案：{file_path}")
                return
    
            # 讀取 CSV 並提取 'code' 欄位
            import pandas as pd
            try:
                df = pd.read_csv(file_path, usecols=['code'])
            except ValueError as ve:
                # 欄位不存在
                QMessageBox.critical(self, "錯誤", f"CSV 檔案中找不到 'code' 欄位。")
                return
            except Exception as e:
                QMessageBox.critical(self, "錯誤", f"讀取 CSV 檔案時發生錯誤：{str(e)}")
                return
    
            code_list = df['code'].dropna().astype(str).tolist()
            py_list_str = str(code_list)
    
            # 複製到剪貼簿（使用 Qt 內建 clipboard）
            clipboard = QGuiApplication.clipboard()
            clipboard.setText(py_list_str)
    
            QMessageBox.information(self, "匯出成功", "CSV 'code' 欄位內容已轉換為 Python list 並複製到剪貼板")
        except Exception as e:
            QMessageBox.critical(self, "錯誤", f"匯出時發生未預期錯誤：{str(e)}")
    
    def export_params_as_data_list(self):
        # 匯出目前載入的 CSV 檔案的參數欄位為 Python list of dict 並複製到剪貼簿
        try:
            # 檢查是否有載入資料
            if self.proxy_model is None or self.db_conn is None:
                QMessageBox.warning(self, "無法匯出", "請先載入回測結果檔案。")
                return

            source_model = self.proxy_model.sourceModel()
            if not hasattr(source_model, "columns") or not hasattr(source_model, "db_conn"):
                QMessageBox.warning(self, "無法匯出", "目前資料模型不支援此操作。")
                return

            # 必要欄位
            required_fields = ['code', 'decay', 'delay', 'neutralization', 'region', 'truncation', 'universe']
            available_fields = [col for col in required_fields if col in getattr(source_model, "columns", [])]
            missing_fields = [col for col in required_fields if col not in available_fields]
            if missing_fields:
                QMessageBox.critical(self, "欄位缺失", f"資料中缺少必要欄位：{', '.join(missing_fields)}")
                return

            # 查詢目前過濾條件下的資料
            filter_clause, params = source_model._get_filter_and_sort_clause() # 修改：接收參數
            cursor = source_model.db_conn.cursor()
            fields_sql = ", ".join(available_fields)
            query = f"SELECT {fields_sql} FROM {source_model.table_name} {filter_clause}"
            try:
                cursor.execute(query, params) # 修改：傳入參數
                rows = cursor.fetchall()
            except Exception as e:
                QMessageBox.critical(self, "查詢錯誤", f"查詢資料庫時發生錯誤：{str(e)}")
                return

            # 組成 Python list of dict 格式
            data_list = []
            for row in rows:
                item = {}
                for idx, col in enumerate(available_fields):
                    val = row[idx]
                    # 字串欄位用 repr()，數值直接輸出
                    if col in ['code', 'neutralization', 'region', 'universe']:
                        item[col] = repr(str(val)) if val is not None else "''"
                    else:
                        # 處理 None
                        item[col] = val if val is not None else "None"
                data_list.append(item)

            # 格式化為 Python 變數
            lines = ["DATA = ["]
            for d in data_list:
                line = "    {"
                line += ", ".join(f"'{k}': {v}" for k, v in d.items())
                line += "},"
                lines.append(line)
            lines.append("]")
            py_data_str = "\n".join(lines)

            # 複製到剪貼簿
            clipboard = QGuiApplication.clipboard()
            clipboard.setText(py_data_str)

            QMessageBox.information(self, "匯出成功", "參數資料已轉換為 Python list 並複製到剪貼簿。")
        except Exception as e:
            QMessageBox.critical(self, "錯誤", f"匯出參數時發生未預期錯誤：{str(e)}")

    def filter_data(self):
        # --- 修正：防止在載入時觸發過濾 ---
        if self.loading:
            return
        # ---------------------------------
        if self.proxy_model is None or not isinstance(self.proxy_model.sourceModel(), SqliteTableModel):
            return

        filter_text = self.search_input.text().strip()
        source_model = self.proxy_model.sourceModel()
        if 'code' not in source_model.columns:
            self.status_bar.showMessage("錯誤：資料中缺少 'code' 欄位，無法進行搜尋。")
            self.proxy_model.setFilterRegularExpression(QRegularExpression())
            self._update_status_bar()
            return
        if not filter_text:
            self.proxy_model.setFilterRegularExpression(QRegularExpression())
        else:
            code_col = source_model.columns.index('code') + 1
            self.proxy_model.setFilterKeyColumn(code_col)
            regex = QRegularExpression(filter_text, QRegularExpression.CaseInsensitiveOption)
            self.proxy_model.setFilterRegularExpression(regex)

        self._update_status_bar() # 更新狀態欄

    def update_chart(self):
        if self.proxy_model is None or not isinstance(self.proxy_model.sourceModel(), SqliteTableModel):
            return

        source_model = self.proxy_model.sourceModel()
        full_clause, params = source_model._get_filter_and_sort_clause() # 修改：獲取子句和參數
        cursor = source_model.db_conn.cursor()
        # 清除當前圖表
        if self.proxy_model is None or not hasattr(self, "canvas") or self.db_conn is None:
            return
        self.canvas.figure.clear()
        ax = self.canvas.figure.add_subplot(111)

        chart_type = self.chart_type.currentText()

        # 增加字體大小使圖表更清晰
        plt.rcParams.update({'font.size': 12})

        if chart_type == "夏普比率分佈":
            try:
                # 使用 SQL 查詢獲取夏普比率
                cursor.execute(f"SELECT CAST(sharpe AS FLOAT) FROM {source_model.table_name} {full_clause}", params) # 修改：使用 full_clause 和 params
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
                # 組合 WHERE 子句
                extra_where = "decay IS NOT NULL"
                query_clause = ""
                if not full_clause:
                    query_clause = f"WHERE {extra_where}"
                elif "WHERE" in full_clause.upper():
                     # 確保只替換第一個 WHERE
                    parts = full_clause.split("WHERE", 1)
                    if len(parts) == 1: # 如果 WHERE 在開頭
                         parts = full_clause.split("where", 1) # 嘗試小寫
                    if len(parts) == 2:
                         query_clause = f"{parts[0]}WHERE {extra_where} AND {parts[1]}"
                    else: # 無法分割，可能 WHERE 不在預期位置，保守處理
                         query_clause = f"{full_clause} AND {extra_where}" # 可能產生無效 SQL，但比 replace 安全
                else: # 沒有 WHERE，但 full_clause 不為空 (例如只有 ORDER BY)
                    query_clause = f"WHERE {extra_where} {full_clause}"

                query = f"""
                    SELECT decay, AVG(CAST(sharpe AS FLOAT)) as avg_sharpe
                    FROM {source_model.table_name}
                    {query_clause}
                    GROUP BY decay
                    ORDER BY decay
                """
                cursor.execute(query, params) # 修改：傳入 params
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
                # 組合 WHERE 子句
                extra_where = "neutralization IS NOT NULL"
                query_clause = ""
                if not full_clause:
                    query_clause = f"WHERE {extra_where}"
                elif "WHERE" in full_clause.upper():
                    parts = full_clause.split("WHERE", 1)
                    if len(parts) == 1: parts = full_clause.split("where", 1)
                    if len(parts) == 2:
                         query_clause = f"{parts[0]}WHERE {extra_where} AND {parts[1]}"
                    else:
                         query_clause = f"{full_clause} AND {extra_where}"
                else:
                    query_clause = f"WHERE {extra_where} {full_clause}"

                query = f"""
                    SELECT neutralization, AVG(CAST(sharpe AS FLOAT)) as avg_sharpe
                    FROM {source_model.table_name}
                    {query_clause}
                    GROUP BY neutralization
                    ORDER BY avg_sharpe DESC
                """
                cursor.execute(query, params) # 修改：傳入 params
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
                # 組合 WHERE 子句
                extra_where = "turnover IS NOT NULL AND sharpe IS NOT NULL"
                query_clause = ""
                if not full_clause:
                    query_clause = f"WHERE {extra_where}"
                elif "WHERE" in full_clause.upper():
                    parts = full_clause.split("WHERE", 1)
                    if len(parts) == 1: parts = full_clause.split("where", 1)
                    if len(parts) == 2:
                         query_clause = f"{parts[0]}WHERE {extra_where} AND {parts[1]}"
                    else:
                         query_clause = f"{full_clause} AND {extra_where}"
                else:
                    query_clause = f"WHERE {extra_where} {full_clause}"

                query = f"""
                    SELECT CAST(turnover AS FLOAT) as turnover, CAST(sharpe AS FLOAT) as sharpe
                    FROM {source_model.table_name}
                    {query_clause}
                """
                cursor.execute(query, params) # 修改：傳入 params
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
        # --- 新增：更嚴格的索引檢查 ---
        if not index.isValid():
             # print("Debug: handle_cell_click received invalid index") # 可選的調試信息
             return # 無效索引，直接返回

        # 檢查索引是否來自正確的代理模型
        if index.model() is not self.proxy_model:
             print(f"警告: handle_cell_click 收到來自錯誤模型的索引 (預期: {self.proxy_model}, 收到: {index.model()})")
             self.status_bar.showMessage("內部錯誤：處理點擊時模型不匹配")
             return
        # --- 新增結束 ---

        if self.proxy_model is None or not isinstance(self.proxy_model.sourceModel(), SqliteTableModel):
            # 這個檢查理論上可以移除，因為上面的檢查更嚴格，但保留也無妨
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
            # --- 修改：直接使用 source_model.data() 獲取值 ---
            # 調整列索引（因為第一列是勾選框）
            column_name = source_model.columns[col - 1]

            # 使用 source_model.data() 獲取值
            value = source_model.data(source_index, Qt.DisplayRole) # 使用 source_index

            if value is None:
                 # 嘗試獲取 ToolTipRole 作為備用 (如果 DisplayRole 為空但有 ToolTip)
                 value = source_model.data(source_index, Qt.ToolTipRole)
                 if value is None:
                     return # 如果兩個 Role 都沒有值，則返回

            value = str(value) # 確保是字串
            # --- 修改結束 ---

            if column_name.lower() == 'link' and value:
                try:
                    webbrowser.open(value)
                    self.status_bar.showMessage(f"已在瀏覽器中打開: {value}")
                except Exception as e:
                    self.status_bar.showMessage(f"無法打開連結: {str(e)}")

            elif column_name.lower() == 'code':
                title = "策略代碼詳細信息"
                try:
                    # --- 修改：嘗試從同一行獲取 link (如果存在) ---
                    link_col_index = -1
                    try:
                        # 找到 'link' 欄位在 source_model.columns 中的索引
                        link_df_index = source_model.columns.index('link')
                        # 轉換為 source_model 的列索引 (+1 因為勾選框)
                        link_col_index = link_df_index + 1
                    except ValueError:
                        pass # 'link' 欄位不存在

                    if link_col_index != -1:
                        # 創建 'link' 欄位的 source_index
                        link_source_index = source_model.index(source_index.row(), link_col_index)
                        if link_source_index.isValid():
                             link_value = source_model.data(link_source_index, Qt.DisplayRole)
                             if link_value:
                                 link = str(link_value)
                                 try:
                                     # 嘗試從連結提取標題部分
                                     title = f"策略詳細信息 - {link.split('/')[-1]}"
                                 except:
                                     pass # 如果連結格式不符，保持預設標題
                    # --- 修改結束 ---
                except Exception as e:
                     print(f"獲取 link 時出錯: {e}") # 打印錯誤以便調試
                     pass # 即使獲取 link 失敗，也要顯示 code

                dialog = DetailDialog(title, value, self)
                dialog.exec()

            elif len(value) > 30:  # Show details for any long text
                dialog = DetailDialog(f"{column_name} - 詳細內容", value, self)
                dialog.exec()

        except Exception as e:
            self.status_bar.showMessage(f"處理單元格點擊時出錯: {str(e)}")

    # 應用數值過濾
    def apply_numeric_filter(self):
        # --- 修正：防止在載入時觸發過濾 ---
        if self.loading:
            return
        # ---------------------------------
        if self.proxy_model is None or not isinstance(self.proxy_model.sourceModel(), SqliteTableModel):
            self.status_bar.showMessage("當前沒有數據可過濾")
            return

        column_name = self.condition_column.currentText()
        if not column_name:
            self.status_bar.showMessage("請先選擇要過濾的數值欄位")
            return

        operator = self.condition_operator.currentText()
        value_text = self.condition_value.text()
        try:
             # 嘗試轉換為浮點數
             value = float(value_text)
        except ValueError:
             # 如果不能轉為浮點數，則視為字串處理 (雖然下拉選單限制了欄位，但以防萬一)
             # 或者直接報錯？在此情境下報錯更合理
             self.status_bar.showMessage("請輸入有效數字進行過濾")
             return

        # --- 修改：調用模型的 update_numeric_filter ---
        source_model = self.proxy_model.sourceModel()
        source_model.update_numeric_filter(column_name, operator, value)
        self._update_status_bar() # 更新狀態欄

    # 清除數值過濾
    def clear_numeric_filter(self):
        if self.proxy_model is None or not isinstance(self.proxy_model.sourceModel(), SqliteTableModel):
            return

        # --- 修改：調用模型的 clear_numeric_filter ---
        self.condition_value.clear() # 清除輸入框
        source_model = self.proxy_model.sourceModel()
        source_model.clear_numeric_filter()
        self._update_status_bar() # 更新狀態欄

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
        if self.proxy_model is None or self.current_dataset is None or self.db_conn is None:
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

    # 新增：附加選取至檔案
    def append_selected_rows(self):
        if self.proxy_model is None or not isinstance(self.proxy_model.sourceModel(), SqliteTableModel):
            QMessageBox.warning(self, "無法附加", "沒有載入的數據集。")
            return

        source_model = self.proxy_model.sourceModel()
        checked_rowids = source_model.get_checked_rows()

        if not checked_rowids:
            QMessageBox.information(self, "沒有選取", "請先勾選要附加的資料列。")
            return

        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "選擇要附加的 CSV 檔案",
            "",
            "CSV 檔案 (*.csv)"
        )

        if not file_path:
            self.status_bar.showMessage("已取消附加操作")
            return

        try:
            rowids_str = ",".join(map(str, checked_rowids))
            cursor = source_model.db_conn.cursor()
            cursor.execute(f"SELECT * FROM {source_model.table_name} WHERE rowid IN ({rowids_str})")
            rows = cursor.fetchall()

            if not rows:
                QMessageBox.information(self, "沒有資料", "選取的資料列查無資料。")
                self.status_bar.showMessage("附加失敗：查無資料")
                return

            df = pd.DataFrame(rows, columns=source_model.columns)
            df.to_csv(file_path, mode='a', header=False, index=False, encoding='utf-8-sig')

            self.status_bar.showMessage(f"已將 {len(df)} 筆選取的資料附加至 {os.path.basename(file_path)}")
            QMessageBox.information(self, "附加成功", f"已成功將 {len(df)} 筆資料附加至:\n{file_path}")

        except Exception as e:
            self.status_bar.showMessage(f"附加檔案失敗: {str(e)}")
            QMessageBox.critical(self, "附加失敗", f"附加檔案時發生錯誤：\n{str(e)}")

    # 新增：刪除選取的資料列
    def delete_selected_rows(self):
        if self.proxy_model is None or not isinstance(self.proxy_model.sourceModel(), SqliteTableModel):
            QMessageBox.warning(self, "無法刪除", "沒有載入的數據集。")
            return

        source_model = self.proxy_model.sourceModel()
        checked_rowids = source_model.get_checked_rows()

        if not checked_rowids:
            QMessageBox.information(self, "沒有選取", "請先勾選要刪除的資料列。")
            return

        reply = QMessageBox.question(
            self,
            "確認刪除",
            f"確定要刪除已勾選的 {len(checked_rowids)} 筆資料嗎？此操作無法復原。",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )

        if reply != QMessageBox.Yes:
            self.status_bar.showMessage("已取消刪除操作")
            return

        try:
            rowids_str = ",".join(map(str, checked_rowids))
            cursor = source_model.db_conn.cursor()
            # 1. 先查詢要刪除的所有欄位資料
            cursor.execute(f"SELECT * FROM {source_model.table_name} WHERE rowid IN ({rowids_str})")
            rows = cursor.fetchall()
            if not rows:
                QMessageBox.information(self, "查無資料", "選取的資料列查無資料。")
                return
            df_to_delete = pd.DataFrame(rows, columns=source_model.columns)

            # 2. 取得目前載入的 CSV 檔案路徑
            if not hasattr(self, 'last_loaded_index') or self.last_loaded_index is None:
                raise Exception("找不到目前載入的 CSV 檔案索引。")
            if isinstance(self.last_loaded_index.model(), QSortFilterProxyModel):
                source_index = self.last_loaded_index.model().mapToSource(self.last_loaded_index)
            else:
                source_index = self.last_loaded_index
            file_path = self.file_model.filePath(source_index)

            # 3. 讀取 CSV 檔案
            try:
                df_csv = pd.read_csv(file_path, dtype=str, keep_default_na=False)
            except Exception as e:
                QMessageBox.critical(self, "讀取 CSV 失敗", f"讀取原始 CSV 檔案時發生錯誤：\n{str(e)}")
                return

            # 4. 型態與欄位順序對齊
            df_to_delete = df_to_delete.astype(str)
            df_to_delete = df_to_delete[df_csv.columns]

            # 5. 進行 anti-join，移除完全匹配的行
            try:
                df_merged = df_csv.merge(df_to_delete, how='left', indicator=True)
                df_csv_updated = df_merged[df_merged['_merge'] == 'left_only'].drop(columns=['_merge'])
            except Exception as e:
                QMessageBox.critical(self, "資料處理錯誤", f"比對刪除資料時發生錯誤：\n{str(e)}")
                return

            # 6. 覆蓋寫回 CSV
            try:
                df_csv_updated.to_csv(file_path, index=False, encoding='utf-8-sig')
            except Exception as e:
                QMessageBox.critical(self, "寫入 CSV 失敗", f"寫回原始 CSV 檔案時發生錯誤：\n{str(e)}")
                return

            # 7. 刪除 SQLite 資料
            cursor.execute(f"DELETE FROM {source_model.table_name} WHERE rowid IN ({rowids_str})")
            source_model.db_conn.commit()
            # 8. 刷新元數據與勾選狀態
            source_model.refresh_metadata()
            self.proxy_model.invalidate()
            self.status_bar.showMessage(f"已刪除 {len(checked_rowids)} 筆資料（含永久刪除 CSV）")
            QMessageBox.information(self, "刪除成功", f"已成功永久刪除 {len(checked_rowids)} 筆資料。")
        except Exception as e:
            self.status_bar.showMessage(f"刪除資料失敗: {str(e)}")
            QMessageBox.critical(self, "刪除失敗", f"刪除資料時發生錯誤：\n{str(e)}")

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
            full_clause, params = source_model._get_filter_and_sort_clause() # 修改：接收參數
            cursor = source_model.db_conn.cursor()
            query = f"SELECT code FROM {source_model.table_name} {full_clause}" # 修改：使用完整子句
            cursor.execute(query, params) # 修改：傳入參數
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
    def on_import_selected_data_to_sim_clicked(self):
        """處理 '匯入選擇的數據至模擬' 選項"""
        if self.proxy_model is None or not isinstance(self.proxy_model.sourceModel(), SqliteTableModel):
            QMessageBox.warning(self, "無法匯入", "請先載入回測結果文件。")
            self.status_bar.showMessage("無法匯入：未載入數據")
            return

        source_model = self.proxy_model.sourceModel()
        checked_rowids = source_model.get_checked_rows()
        if not checked_rowids:
            QMessageBox.information(self, "沒有選取", "請先勾選要匯入的資料列。")
            self.status_bar.showMessage("未匯入：沒有勾選資料列")
            return

        try:
            rowids_str = ",".join(map(str, checked_rowids))
            cursor = source_model.db_conn.cursor()
            cursor.execute(f"SELECT * FROM {source_model.table_name} WHERE rowid IN ({rowids_str})")
            rows = cursor.fetchall()

            if rows:
                df = pd.DataFrame(rows, columns=source_model.columns)
                self.import_data_requested.emit(df)
                self.status_bar.showMessage(f"已請求將 {len(df)} 筆選定的數據追加到模擬器")
            else:
                QMessageBox.warning(self, "無數據", "選定的資料列查無數據。")
                self.status_bar.showMessage("匯入失敗：無可用數據")
        except Exception as e:
            QMessageBox.critical(self, "匯入失敗", f"提取選定數據時發生錯誤：\n{str(e)}")
            self.status_bar.showMessage(f"匯入選定數據失敗: {str(e)}")

    @Slot()
    def on_import_data_to_sim_clicked(self):
        """處理 '匯入所有數據至模擬' 選項"""
        if self.proxy_model is None or not isinstance(self.proxy_model.sourceModel(), SqliteTableModel):
            QMessageBox.warning(self, "無數據", "請先載入回測結果文件。")
            self.status_bar.showMessage("無法匯入：未載入數據")
            return

        source_model = self.proxy_model.sourceModel()
        try:
            # 構建 SQL 查詢，包含當前過濾條件
            filter_clause, params = source_model._get_filter_and_sort_clause() # 修改：接收參數
            cursor = source_model.db_conn.cursor()
            # 獲取所有列的數據
            cursor.execute(f"SELECT * FROM {source_model.table_name} {filter_clause}", params) # 修改：傳入參數
            rows = cursor.fetchall()
            
            if rows:
                # 將數據轉換為 DataFrame
                df = pd.DataFrame(rows, columns=source_model.columns)
                # 發送信號
                self.import_data_requested.emit(df)
                self.status_bar.showMessage(f"已請求將 {len(df)} 筆數據追加到模擬器")
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
        # --- 獲取檔名和副檔名 ---
        name_without_ext, ext = os.path.splitext(old_name)
        # -----------------------
        new_name, ok = QInputDialog.getText(
            self,
            "重命名文件",
            "請輸入新的文件名稱：",
            text=name_without_ext # --- 只顯示檔名 ---
        )

        if ok and new_name and new_name != name_without_ext: # --- 比較修改後的檔名 ---
            try:
                # --- 組成新的文件路徑，保留原始副檔名 ---
                new_path = os.path.join(os.path.dirname(file_path), new_name + ext)
                # --------------------------------------
                # 檢查新文件名是否已存在
                if os.path.exists(new_path):
                    QMessageBox.warning(self, "錯誤", f"文件 '{os.path.basename(new_path)}' 已存在。") # --- 顯示完整的重命名後檔案名稱 ---
                    return

                os.rename(file_path, new_path)
                self.status_bar.showMessage(f"已將文件 '{old_name}' 重命名為 '{os.path.basename(new_path)}'") # --- 顯示完整的重命名前後檔案名稱 ---

                # 如果重命名的是當前載入的文件，更新標籤
                if hasattr(self, 'current_file_label') and old_name == self.current_file_label.text():
                    self.current_file_label.setText(os.path.basename(new_path)) # --- 更新為新的檔案名稱(含副檔名) ---

            except Exception as e:
                QMessageBox.critical(self, "錯誤", f"重命名文件時發生錯誤：\\n{str(e)}")

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
                    # (移除選取操作相關程式碼)
                    self.column_view_button.setEnabled(False)
                    self.import_button.setEnabled(False)

            except Exception as e:
                QMessageBox.critical(self, "錯誤", f"刪除文件時發生錯誤：\n{str(e)}")

    def show_table_context_menu(self, position):
        """顯示表格右鍵選單，包含批次勾選/取消勾選與選取操作"""
        if self.proxy_model is None or not isinstance(self.proxy_model.sourceModel(), SqliteTableModel):
            return

        selected_indexes = self.table_view.selectedIndexes()
        if not selected_indexes:
            return

        view_rows = sorted(list(set(index.row() for index in selected_indexes if index.isValid())))
        if not view_rows:
            return

        all_checked = True
        source_model = self.proxy_model.sourceModel()

        for view_row in view_rows: # view_row is a row number in the proxy model
            try:
                # Get the QModelIndex for column 0 of the current proxy_model view_row
                proxy_model_index_col0 = self.proxy_model.index(view_row, 0)
                if not proxy_model_index_col0.isValid():
                    # This case should ideally not happen if view_row comes from valid selectedIndexes
                    all_checked = False
                    break

                # Map the proxy model index to the source model index
                source_model_index_col0 = self.proxy_model.mapToSource(proxy_model_index_col0)
                if not source_model_index_col0.isValid():
                    # This can occur if the source model structure has changed and the proxy
                    # hasn't fully updated, or if the row is filtered out by the proxy
                    # but was somehow part of selected_indexes.
                    print(f"Warning: mapToSource returned invalid source index for proxy row {view_row} in show_table_context_menu.")
                    all_checked = False
                    break # Or use 'continue' if you want to check other rows

                # source_model_index_col0.row() is the row number in the source model's
                # current internal view (after its own filtering/sorting).
                # Use the source model's helper to get the rowid.
                rowid = source_model._get_rowid_for_row(source_model_index_col0.row())

                if rowid is not None:
                    check_state = source_model._check_states.get(rowid, Qt.Unchecked)
                    if check_state != Qt.Checked:
                        all_checked = False
                        break  # Exit the loop as soon as one non-checked item is found
                else:
                    # _get_rowid_for_row failed to find a rowid for this source_model_row.
                    # This indicates a potential inconsistency or an edge case.
                    print(f"Warning: _get_rowid_for_row returned None for source model row {source_model_index_col0.row()} (derived from proxy row {view_row}).")
                    all_checked = False
                    break # Exit the loop

            except Exception as e:
                import traceback
                print(f"Error checking state for proxy row {view_row} in show_table_context_menu: {type(e).__name__} - {str(e)}")
                print(traceback.format_exc()) # Print full traceback for detailed debugging
                all_checked = False
                break # Exit the loop on any error
        
        # 建立右鍵選單
        context_menu = QMenu(self)
        # 1. 批次勾選/取消勾選
        target_state = Qt.Unchecked if all_checked else Qt.Checked
        action_text = "取消勾選選取項" if all_checked else "勾選選取項"
        toggle_check_action = QAction(action_text, self)
        toggle_check_action.triggered.connect(lambda: self.toggle_selected_rows_check_state(view_rows, target_state))
        context_menu.addAction(toggle_check_action)

        # 分隔線
        context_menu.addSeparator()

        # 2. 反轉選取
        action_invert = QAction("反轉選取", self)
        action_invert.triggered.connect(self.invert_selection)
        context_menu.addAction(action_invert)

        # 3. 儲存選取為新檔案
        action_save = QAction("儲存選取為新檔案", self)
        action_save.triggered.connect(self.save_selected_rows)
        context_menu.addAction(action_save)

        # 4. 附加選取至檔案
        action_append = QAction("附加選取至檔案", self)
        action_append.triggered.connect(self.append_selected_rows)
        context_menu.addAction(action_append)

        # 5. 刪除選取
        action_delete = QAction("刪除選取", self)
        action_delete.triggered.connect(self.delete_selected_rows)
        context_menu.addAction(action_delete)

        # 顯示選單
        context_menu.exec_(self.table_view.viewport().mapToGlobal(position))

    def toggle_selected_rows_check_state(self, view_rows, target_state):
        """切換指定視圖行的勾選狀態"""
        if self.proxy_model is None or not isinstance(self.proxy_model.sourceModel(), SqliteTableModel):
            return

        source_model = self.proxy_model.sourceModel()
        updated_count = 0
        # Use model's begin/end reset for potentially better performance on large updates
        # source_model.beginResetModel() # Or use begin/endInsertRows if applicable

        for view_row in view_rows:
            view_index_col0 = self.proxy_model.index(view_row, 0)
            if view_index_col0.isValid():
                # setData on proxy model triggers setData on source model
                success = self.proxy_model.setData(view_index_col0, target_state, Qt.CheckStateRole)
                if success:
                    updated_count += 1

        # source_model.endResetModel()

        # Update status bar after changes
        if updated_count > 0:
             # Re-fetch checked count after updates
             checked_count = len(source_model.get_checked_rows())
             self.status_bar.showMessage(f"已更新 {updated_count} 項勾選狀態 | 目前共勾選 {checked_count} 項")
        else:
             self.status_bar.showMessage("未更新勾選狀態 (可能無效選擇或更新失敗)")


    # === 新增：反轉選取功能 ===
    def invert_selection(self):
        if self.proxy_model is None or not isinstance(self.proxy_model.sourceModel(), SqliteTableModel):
            QMessageBox.warning(self, "無法反轉", "目前沒有有效的回測結果表格。")
            return

        source_model = self.proxy_model.sourceModel()
        # 反轉所有 rowid 的勾選狀態
        for rowid in source_model._check_states:
            state = source_model._check_states[rowid]
            if state == Qt.Checked:
                source_model._check_states[rowid] = Qt.Unchecked
            else:
                source_model._check_states[rowid] = Qt.Checked

        # 更新所有可見行的勾選框狀態
        proxy_row_count = self.proxy_model.rowCount()
        if proxy_row_count > 0:
            first_index = self.proxy_model.index(0, 0)
            last_index = self.proxy_model.index(proxy_row_count - 1, 0)
            self.proxy_model.dataChanged.emit(first_index, last_index, [Qt.CheckStateRole])

        # 更新狀態列
        checked_count = len(source_model.get_checked_rows())
        self.status_bar.showMessage(f"已反轉選取狀態 | 目前共勾選 {checked_count} 項")
    # === 新增結束 ===

    # --- 移除舊的組合過濾方法 ---
    # def _apply_combined_filter(self): ... (已移除) ...

    # --- 新增/修改：統一更新狀態欄的方法 ---
    def _update_status_bar(self):
        if self.proxy_model is None or not isinstance(self.proxy_model.sourceModel(), SqliteTableModel):
            # 考慮在沒有模型時顯示更清晰的狀態
            file_label_text = self.current_file_label.text()
            if file_label_text == "未載入檔案":
                 self.status_bar.showMessage("選擇左側的回測結果文件以開始瀏覽")
            else:
                 # 可能檔案載入失敗或為空
                 self.status_bar.showMessage(f"檔案: {file_label_text} | 無有效數據或載入失敗")
            return

        source_model = self.proxy_model.sourceModel()
        # 確保 total_count 已被設置
        total_count = getattr(source_model, 'row_count', 0)
        filtered_count = source_model.rowCount() # rowCount 現在會考慮 filter_conditions

        # 獲取過濾條件描述
        filter_desc = []
        conditions = getattr(source_model, 'filter_conditions', {})
        raw_sql_active = conditions.get('raw')
        rowid_filter_active = conditions.get('rowid')

        if raw_sql_active:
             # 假設 raw SQL 只用於 '全部欄位' 文字過濾
             filter_desc.append(f"文字過濾(全部): '{self.search_input.text()}'")
             # 數值過濾在 raw SQL 模式下被忽略
             filter_desc.append("數值過濾: 無 (因使用全部欄位文字過濾)")
        elif rowid_filter_active:
             num_rowids = len(rowid_filter_active.get('val', []))
             filter_desc.append(f"RowID 過濾: {num_rowids} 個")
             filter_desc.append("文字過濾: 無")
             filter_desc.append("數值過濾: 無")
        else:
             # 處理參數化條件
             text_filter = conditions.get('text')
             numeric_filter = conditions.get('numeric')

             if text_filter:
                 # --- 修改：狀態欄固定顯示搜尋 'code' ---
                 search_term = str(text_filter.get('val', '')).strip('%')
                 # filter_desc.append(f"文字過濾({text_filter.get('col')}): '{search_term}'") # Old
                 filter_desc.append(f"Code 搜尋: '{search_term}'") # New
                 # --- 修改結束 ---
             else:
                 filter_desc.append("Code 搜尋: 無") # Update label

             if numeric_filter:
                 filter_desc.append(f"數值過濾({numeric_filter.get('col')} {numeric_filter.get('op')} {numeric_filter.get('val')})")
             else:
                 filter_desc.append("數值過濾: 無")

        status_text = f"顯示 {filtered_count}/{total_count} 條記錄 | {' | '.join(filter_desc)}"
        self.status_bar.showMessage(status_text)


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

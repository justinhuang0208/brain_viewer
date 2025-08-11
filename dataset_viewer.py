import sys
import os
import sqlite3
import pandas as pd
import json
import threading
from dotenv import load_dotenv
import google.generativeai as genai
from PySide6.QtWidgets import (QApplication, QMainWindow, QTableView, QTreeView,
                              QSplitter, QVBoxLayout, QHBoxLayout, QWidget,
                              QLineEdit, QLabel, QComboBox, QFileSystemModel,
                              QHeaderView, QPushButton, QStatusBar, QTabWidget,
                              QMessageBox, QDialog, QTextEdit, QVBoxLayout, QFrame,
                              QMenu, QProgressBar)
from PySide6.QtCore import Qt, QDir, QModelIndex, QSortFilterProxyModel, Signal, Slot, QAbstractTableModel, QThread
from PySide6.QtGui import QColor, QFont, QPalette, QIcon, QAction
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import matplotlib

# 載入環境變數
load_dotenv()

# 初始化 Gemini API
try:
    api_key = os.getenv('GEMINI_API_KEY')
    if api_key and api_key != 'your_gemini_api_key_here':
        genai.configure(api_key=api_key)
        GEMINI_AVAILABLE = True
    else:
        GEMINI_AVAILABLE = False
        print("警告: 未找到有效的 GEMINI_API_KEY，語意搜索功能將不可用")
except Exception as e:
    GEMINI_AVAILABLE = False
    print(f"Gemini API 初始化失敗: {str(e)}")

def set_chinese_font():
    chinese_fonts = ['Microsoft JhengHei', 'DFKai-SB', 'SimHei', 'STHeiti', 'STSong', 'Arial Unicode MS', 'SimSun']
    
    if os.name == 'nt':
        from matplotlib.font_manager import FontManager
        fm = FontManager()
        available_fonts = set([f.name for f in fm.ttflist])
        
        for font in chinese_fonts:
            if font in available_fonts:
                print(f"使用中文字體: {font}")
                matplotlib.rcParams['font.family'] = font
                return True
                
    try:
        plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Arial Unicode MS', 'sans-serif']
        plt.rcParams['axes.unicode_minus'] = False  # 解決負號顯示問題
        return True
    except:
        return False

set_chinese_font()

class SemanticSearchWorker(QThread):
    finished = Signal(list)
    error = Signal(str)
    
    def __init__(self, query, field_data):
        super().__init__()
        self.query = query
        self.field_data = field_data
        
    def run(self):
        try:
            print(f"開始語意搜索，查詢: {self.query}")
            print(f"字段數據數量: {len(self.field_data)}")
            
            # 準備發送給 Gemini 的數據
            prompt = self.create_search_prompt(self.query, self.field_data)
            print(f"提示詞長度: {len(prompt)}")
            
            # 調用 Gemini API
            model = genai.GenerativeModel('gemini-2.5-flash-lite')
            response = model.generate_content(prompt)
            print(f"Gemini 回應原始內容: {response.text}")
            
            # 解析回應
            result_fields = self.parse_response(response.text)
            print(f"解析後的字段列表: {result_fields}")
            print(f"找到的字段數量: {len(result_fields)}")
            
            self.finished.emit(result_fields)
            
        except Exception as e:
            print(f"語意搜索執行錯誤: {str(e)}")
            self.error.emit(f"語意搜索時發生錯誤: {str(e)}")
    
    def create_search_prompt(self, query, field_data):
        """創建發送給 Gemini 的提示詞"""
        # 將字段數據轉換為結構化格式
        fields_info = []
        for field, description in field_data:
            fields_info.append({
                "field": field,
                "description": description
            })
        
        prompt = f"""
你是一個金融數據字段專家。請根據用戶的查詢需求，從以下字段列表中找出相關的字段。

用戶查詢：{query}

可用字段列表：
{json.dumps(fields_info, ensure_ascii=False, indent=2)}

請分析用戶的查詢意圖，並返回相關的字段名稱列表。請只返回一個 JSON 格式的字段名稱數組，不要包含其他文字說明。

例如：["field1", "field2", "field3"]

請確保返回的是有效的 JSON 格式。
"""
        return prompt
    
    def parse_response(self, response_text):
        """解析 Gemini 的回應"""
        print(f"開始解析回應，原始長度: {len(response_text)}")
        print(f"原始回應前500字符: {response_text[:500]}")
        
        try:
            # 清理回應文本，移除可能的markdown標記
            cleaned_response = response_text.strip()
            
            if cleaned_response.startswith('```json'):
                cleaned_response = cleaned_response[7:]
                print("移除了 ```json 開頭")
            elif cleaned_response.startswith('```'):
                cleaned_response = cleaned_response[3:]
                print("移除了 ``` 開頭")
            
            if cleaned_response.endswith('```'):
                cleaned_response = cleaned_response[:-3]
                print("移除了 ``` 結尾")
            
            cleaned_response = cleaned_response.strip()
            print(f"清理後的回應: {cleaned_response}")
            
            # 嘗試解析 JSON
            result = json.loads(cleaned_response)
            print(f"JSON 解析成功，類型: {type(result)}")
            
            if isinstance(result, list):
                print(f"返回列表，內容: {result}")
                cleaned_result = [str(item).strip() for item in result if item]
                print(f"清理後的列表: {cleaned_result}")
                return cleaned_result
            elif isinstance(result, dict):
                print(f"返回字典，內容: {result}")
                values = []
                for key, value in result.items():
                    if isinstance(value, list):
                        values.extend([str(v).strip() for v in value if v])
                    else:
                        values.append(str(value).strip())
                print(f"從字典提取的值: {values}")
                return values
            else:
                print(f"不是列表或字典類型，而是: {type(result)}，值: {result}")
                return []
                
        except json.JSONDecodeError as e:
            print(f"JSON 解析失敗: {str(e)}")
            
            # 如果無法解析為 JSON，嘗試多種方式提取字段名稱
            import re
            
            # 方法1：提取雙引號中的內容
            fields = re.findall(r'"([^"]+)"', response_text)
            print(f"方法1 - 通過雙引號提取的字段: {fields}")
            
            if not fields:
                # 方法2：提取單引號中的內容
                fields = re.findall(r"'([^']+)'", response_text)
                print(f"方法2 - 通過單引號提取的字段: {fields}")
            
            if not fields:
                # 方法3：查找可能的列表格式
                list_match = re.search(r'\[(.*?)\]', response_text, re.DOTALL)
                if list_match:
                    list_content = list_match.group(1)
                    # 分割並清理
                    fields = [item.strip().strip('"').strip("'") 
                             for item in list_content.split(',') if item.strip()]
                    print(f"方法3 - 通過列表格式提取的字段: {fields}")
            
            if not fields:
                # 方法4：嘗試按行分割並查找可能的字段名
                lines = response_text.split('\n')
                for line in lines:
                    line = line.strip()
                    if line and not line.startswith(('```', '//', '#', '以下', '例如')):
                        # 移除常見的前綴符號
                        line = re.sub(r'^[-*•]\s*', '', line)
                        if line:
                            fields.append(line.strip('"').strip("'"))
                print(f"方法4 - 通過行分割提取的字段: {fields}")
            
            # 過濾和清理結果
            cleaned_fields = []
            for field in fields:
                field = str(field).strip()
                if field and len(field) > 0 and field not in ['field1', 'field2', 'field3', 'example']:
                    cleaned_fields.append(field)
            
            print(f"最終清理後的字段列表: {cleaned_fields}")
            return cleaned_fields
            
        except Exception as e:
            print(f"解析過程中發生其他錯誤: {str(e)}")
            return []

# 自定義 SQLite 表格模型
class SqliteTableModel(QAbstractTableModel):
    def __init__(self, db_path, table_name):
        super(SqliteTableModel, self).__init__()
        self.db_path = db_path
        self.table_name = table_name
        self.columns = ["Select"]  # 添加一個 checkbox 列
        self.checked_rows = set()  # 存儲被勾選的行
        self.cache = {}  # 用於緩存查詢結果
        self.total_rows = 0
        self.conn = None
        self.original_columns = []  # 存儲原始列名
        self.setup_connection()

    def setup_connection(self):
        """建立數據庫連接和初始化"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            cursor = self.conn.cursor()
            
            # 獲取列名
            cursor.execute(f"PRAGMA table_info({self.table_name})")
            self.original_columns = [info[1] for info in cursor.fetchall()]
            self.columns.extend(self.original_columns)  # 將原始列名添加到 checkbox 列之後
            
            # 獲取總行數
            cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            self.total_rows = cursor.fetchone()[0]
            
        except Exception as e:
            print(f"數據庫連接錯誤: {str(e)}")

    def rowCount(self, parent=None):
        return self.total_rows

    def columnCount(self, parent=None):
        return len(self.columns)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None

        row = index.row()
        col = index.column()

        if role == Qt.CheckStateRole and col == 0:  # 第一列是 checkbox
            is_checked = row in self.checked_rows
            # print(f"Data requested: row {row}, col {col}, role CheckStateRole. Is checked? {is_checked}. Returning: {'Checked' if is_checked else 'Unchecked'}") # Debug print removed
            return Qt.Checked if is_checked else Qt.Unchecked

        if col == 0:  # 第一列不顯示文字
            return None

        column_name = self.original_columns[col - 1]  # 因為第一列是 checkbox，所以要減 1

        # 使用緩存系統
        cache_key = (row, col)
        if cache_key not in self.cache:
            try:
                cursor = self.conn.cursor()
                cursor.execute(
                    f"SELECT {column_name} FROM {self.table_name} LIMIT 1 OFFSET {row}"
                )
                value = cursor.fetchone()[0]
                self.cache[cache_key] = value
            except Exception as e:
                print(f"數據查詢錯誤: {str(e)}")
                return None
        else:
            value = self.cache[cache_key]

        if role == Qt.DisplayRole and col > 0:
            return str(value)

        elif role == Qt.EditRole:
            if column_name == 'Coverage':
                try:
                    return float(str(value).replace('%', ''))
                except ValueError:
                    print(f"警告: 無法將 Coverage 列第 {row} 行的值 '{value}' 轉換為浮點數")
                    return 0
            elif column_name in ['Users', 'Alphas']:
                try:
                    return int(value)
                except (ValueError, TypeError):
                    print(f"警告: 無法將 {column_name} 列第 {row} 行的值 '{value}' 轉換為整數")
                    return 0
            try:
                return float(value)
            except (ValueError, TypeError):
                return str(value)

        elif role == Qt.ToolTipRole:
            if column_name == 'Description' and len(str(value)) > 20:
                return str(value)
            return str(value)

        elif role == Qt.BackgroundRole:
            if column_name == 'Coverage':
                try:
                    coverage = int(str(value).replace('%', ''))
                    if coverage > 90:
                        return QColor("#c8e6c9")  # 淺綠色
                    elif coverage > 70:
                        return QColor("#fff9c4")  # 淺黃色
                    else:
                        return QColor("#ffccbc")  # 淺紅色
                except:
                    pass
            elif column_name in ['Users', 'Alphas']:
                try:
                    value_int = int(value)
                    if value_int > 500:
                        return QColor("#e3f2fd")  # 淺藍色
                except:
                    pass
        return None

    def headerData(self, section, orientation, role):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                if section == 0:
                    return ""  # Checkbox 列的標題為空
                return str(self.columns[section])
            if orientation == Qt.Vertical:
                return str(section + 1)
        elif role == Qt.ToolTipRole and orientation == Qt.Horizontal and section == 0:
            return "點擊勾選要匯入到策略生成器的字段"
        return None

    def sort(self, column, order):
        """實現排序功能"""
        try:
            direction = "ASC" if order == Qt.AscendingOrder else "DESC"
            column_name = self.columns[column]
            
            # 根據不同列類型使用不同的排序邏輯
            if column_name == 'Coverage':
                order_by = f"CAST(REPLACE({column_name}, '%', '') AS FLOAT)"
            elif column_name in ['Users', 'Alphas']:
                order_by = f"CAST({column_name} AS INTEGER)"
            else:
                order_by = column_name
            
            # 執行排序查詢
            cursor = self.conn.cursor()
            cursor.execute(f"""
                CREATE TEMP TABLE IF NOT EXISTS sorted_rows AS
                SELECT rowid, *
                FROM {self.table_name}
                ORDER BY {order_by} {direction}
            """)
            
            # 清除緩存
            self.cache.clear()
            
            # 通知視圖數據已更改
            self.layoutAboutToBeChanged.emit()
            self.layoutChanged.emit()
            
        except Exception as e:
            print(f"排序錯誤: {str(e)}")

    def get_value(self, row, column_name):
        """獲取指定行和列的值"""
        # 修正: 如果查詢的是虛擬 Select 欄位，直接返回 None
        if column_name == "Select":
            return None
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                f"SELECT {column_name} FROM {self.table_name} LIMIT 1 OFFSET {row}"
            )
            return cursor.fetchone()[0]
        except Exception as e:
            print(f"獲取數據錯誤: {str(e)}")
            return None

    def setData(self, index, value, role=Qt.EditRole):
        """處理資料更改，特別是 checkbox 的狀態改變"""
        if not index.isValid():
            return False

        if role == Qt.CheckStateRole and index.column() == 0:
            # Note: 'index.row()' here refers to the source model row index
            row = index.row()
            if value == Qt.Checked:
                self.checked_rows.add(row)
                # print(f"Checked row {row}, total checked: {len(self.checked_rows)}") # Debug print removed
            else:
                self.checked_rows.discard(row)
                # print(f"Unchecked row {row}, total checked: {len(self.checked_rows)}") # Debug print removed
            # Emit dataChanged specifically for the CheckStateRole
            self.dataChanged.emit(index, index, [Qt.CheckStateRole])
            return True

        # Return False for unhandled roles/columns
        return False

    def flags(self, index):
        """設定單元格的屬性，使第一列可被勾選"""
        if not index.isValid():
            return Qt.NoItemFlags
        if index.column() == 0:
            return Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsUserCheckable
        return Qt.ItemIsEnabled | Qt.ItemIsSelectable
    
    def get_checked_fields(self):
        """獲取所有被勾選的字段名稱"""
        checked_fields = []
        try:
            cursor = self.conn.cursor()
            field_col_index = self.original_columns.index('Field')
            for row in sorted(self.checked_rows):
                cursor.execute(
                    f"SELECT Field FROM {self.table_name} LIMIT 1 OFFSET {row}"
                )
                field = cursor.fetchone()[0]
                checked_fields.append(field)
        except Exception as e:
            print(f"獲取勾選字段時出錯: {str(e)}")
        return checked_fields

# Custom Proxy Model to handle flags correctly
class CheckableSortFilterProxyModel(QSortFilterProxyModel):
    def flags(self, index):
        if not index.isValid():
            return Qt.NoItemFlags

        # Ensure the checkbox column (column 0 in the proxy) remains checkable
        if index.column() == 0:
            # Get flags from source, but ensure ItemIsUserCheckable is set
            source_index = self.mapToSource(index)
            source_flags = self.sourceModel().flags(source_index)
            return source_flags | Qt.ItemIsUserCheckable # Explicitly add checkable flag
        else:
            # For other columns, return the default flags provided by the proxy
            return super(CheckableSortFilterProxyModel, self).flags(index)

# 自定義詳細文本對話框
class DetailDialog(QDialog):
    def __init__(self, title, text, parent=None):
        super(DetailDialog, self).__init__(parent)
        self.setWindowTitle(title)
        self.resize(500, 300)
        
        layout = QVBoxLayout()
        
        # 文本編輯器，只讀模式
        text_edit = QTextEdit()
        text_edit.setReadOnly(True)
        text_edit.setText(text)
        text_edit.setLineWrapMode(QTextEdit.WidgetWidth)
        
        layout.addWidget(text_edit)
        
        # Close button
        close_button = QPushButton("Close")
        close_button.clicked.connect(self.accept)
        
        layout.addWidget(close_button)
        self.setLayout(layout)

# 主窗口
class MainWindow(QMainWindow):
    # 定義新的信號，用於將選中的字段發送到生成器
    fields_selected_for_generator = Signal(list)

    def __init__(self):
        super(MainWindow, self).__init__()
        self.setWindowTitle("WorldQuant Brain Dataset Viewer")
        self.setMinimumSize(1200, 800)
        
        # 設置主窗口佈局
        main_layout = QVBoxLayout()
        
        # 頂部佈局：導航與搜索
        top_layout = QHBoxLayout()
        
        # 創建標籤區域和搜索區域
        top_left = QHBoxLayout()
        top_right = QHBoxLayout()
        
        # 資料集標籤 - 使用小標籤，不是大標題
        dataset_label = QLabel("Dataset:")
        
        # 當前檔案標籤 - 顯示當前載入的檔案
        self.current_file_label = QLabel("No file loaded")
        self.current_file_label.setStyleSheet("font-weight: bold; color: #1976D2;")
        
        # 搜尋元素
        search_label = QLabel("Search:")
        search_label.setStyleSheet("margin-left: 15px;")
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Enter keyword...")
        self.search_input.textChanged.connect(self.on_search_input_changed)
        self.search_input.returnPressed.connect(self.on_search_enter_pressed)
        self.search_input.setMinimumWidth(250)
        self.search_input.setStyleSheet("padding: 4px; border: 1px solid #bdbdbd; border-radius: 4px;")
        
        # 搜索模式下拉按鈕
        self.search_mode_button = QPushButton("Normal Search")
        self.search_mode_button.setToolTip("Select search mode")
        self.search_mode_button.setStyleSheet("padding: 4px 8px; background-color: #2196F3; color: white; border: none; border-radius: 4px;")
        self.search_mode_button.setMinimumWidth(100)
        
        # 創建搜索模式選單
        self.search_mode_menu = QMenu(self)
        
        # 一般搜尋選項
        self.normal_search_action = QAction("Normal Search", self)
        self.normal_search_action.setCheckable(True)
        self.normal_search_action.setChecked(True)  # 預設選中
        self.normal_search_action.triggered.connect(lambda: self.set_search_mode("normal"))
        
        # AI搜尋選項
        self.ai_search_action = QAction("AI Search", self)
        self.ai_search_action.setCheckable(True)
        self.ai_search_action.setEnabled(GEMINI_AVAILABLE)
        self.ai_search_action.triggered.connect(lambda: self.set_search_mode("ai"))
        if not GEMINI_AVAILABLE:
            self.ai_search_action.setText("AI Search (API key required)")
        
        # 將動作添加到選單
        self.search_mode_menu.addAction(self.normal_search_action)
        self.search_mode_menu.addAction(self.ai_search_action)
        self.search_mode_button.setMenu(self.search_mode_menu)
        
        # 進度條（用於顯示AI搜索進度）
        self.search_progress = QProgressBar()
        self.search_progress.setVisible(False)
        self.search_progress.setMaximum(0)  # 無限進度條
        self.search_progress.setStyleSheet("QProgressBar { border: 1px solid #bdbdbd; border-radius: 4px; } QProgressBar::chunk { background-color: #4CAF50; }")
        
        # 欄位選擇下拉框
        field_label = QLabel("Column:")
        self.filter_column = QComboBox()
        self.filter_column.addItem("All Columns")
        self.filter_column.addItem("Field")
        self.filter_column.addItem("Description")
        self.filter_column.addItem("Type")
        self.filter_column.currentIndexChanged.connect(self.filter_data)
        self.filter_column.setMinimumWidth(120)
        
        # 添加刷新按鈕
        refresh_button = QPushButton("Refresh")
        refresh_button.setToolTip("Reload the currently selected dataset")
        refresh_button.clicked.connect(self.refresh_current_dataset)
        refresh_button.setStyleSheet("padding: 4px 12px;")
        
        # 布局左側標籤區域
        top_left.addWidget(dataset_label)
        top_left.addWidget(self.current_file_label)
        top_left.addStretch()
        
        # 布局右側搜索區域
        top_right.addWidget(search_label)
        top_right.addWidget(self.search_input)
        top_right.addWidget(self.search_mode_button)
        top_right.addWidget(self.search_progress)
        # 添加匯出按鈕
        self.export_button = QPushButton("Import Selected Fields to Generator")
        self.export_button.clicked.connect(self.export_checked_fields)
        self.export_button.setEnabled(False)  # 初始時禁用
        self.export_button.setStyleSheet("padding: 4px 12px;")
        self.export_button.setToolTip("Please check fields to import")

        top_right.addWidget(field_label)
        top_right.addWidget(self.filter_column)
        top_right.addWidget(refresh_button)
        top_right.addWidget(self.export_button)
        
        # 組合左右兩側到頂部布局
        top_layout.addLayout(top_left, 1)  # 1:2 比例
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
        files_title = QLabel("Dataset Files")
        files_title.setStyleSheet("font-weight: bold; font-size: 14px;")
        files_header.addWidget(files_title)
        files_header.addStretch()
        
        self.file_model = QFileSystemModel()
        self.file_model.setRootPath(QDir.currentPath() + "/datasets")
        self.file_model.setNameFilters(["*_fields_formatted.csv"])
        self.file_model.setNameFilterDisables(False)
        
        self.file_view = QTreeView()
        self.file_view.setModel(self.file_model)
        self.file_view.setRootIndex(self.file_model.index(QDir.currentPath() + "/datasets"))
        self.file_view.clicked.connect(self.load_dataset)
        self.file_view.setAnimated(True)
        self.file_view.setHeaderHidden(True)
        self.file_view.setStyleSheet("QTreeView { border: 1px solid #d0d0d0; border-radius: 4px; }")
        for i in range(1, 4):  # 隱藏不需要的列
            self.file_view.hideColumn(i)
            
        left_layout.addLayout(files_header)
        left_layout.addWidget(self.file_view)
        
        # 右側內容標籤頁
        self.tab_widget = QTabWidget()
        
        # 數據表格標籤頁
        self.table_view = QTableView()
        self.table_view.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setSortingEnabled(True)
        self.table_view.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table_view.customContextMenuRequested.connect(self.show_context_menu)
        
        # 數據圖表標籤頁
        self.chart_widget = QWidget()
        chart_layout = QVBoxLayout()
        
        self.canvas = FigureCanvas(plt.figure(figsize=(10, 8)))
        chart_controls = QHBoxLayout()
        
        self.chart_type = QComboBox()
        self.chart_type.addItems(["Coverage Distribution", "Users", "Alphas", "Type Distribution"])
        self.chart_type.currentIndexChanged.connect(self.update_chart)
        
        chart_controls.addWidget(QLabel("Chart Type:"))
        chart_controls.addWidget(self.chart_type)
        chart_controls.addStretch()
        
        chart_layout.addLayout(chart_controls)
        chart_layout.addWidget(self.canvas)
        self.chart_widget.setLayout(chart_layout)
        
        # 添加標籤頁
        self.tab_widget.addTab(self.table_view, "Data Table")
        self.tab_widget.addTab(self.chart_widget, "Data Visualization")
        
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
        self.proxy_model = None
        self.click_connected = False  # 跟踪點擊事件是否已連接
        
        # 搜索模式相關
        self.current_search_mode = "normal"  # "normal" 或 "ai"
        
        # 語意搜索相關
        self.semantic_search_worker = None
        self.semantic_search_results = []  # 存儲語意搜索結果
        self.is_semantic_search_active = False  # 標記是否正在進行語意搜索
        
        # 顯示初始信息
        self.status_bar.showMessage("Select a dataset file on the left to begin")

    # 新增刷新當前資料集功能
    def refresh_current_dataset(self):
        if not hasattr(self, 'last_loaded_index') or self.last_loaded_index is None:
            self.status_bar.showMessage("No loaded dataset to refresh")
            return
            
        self.load_dataset(self.last_loaded_index)
        self.status_bar.showMessage("已刷新當前資料集")
    
    def set_search_mode(self, mode):
        """Set search mode"""
        self.current_search_mode = mode
        
        # 更新按鈕文字和樣式
        if mode == "normal":
            self.search_mode_button.setText("Normal Search")
            self.search_mode_button.setStyleSheet("padding: 4px 8px; background-color: #2196F3; color: white; border: none; border-radius: 4px;")
            self.normal_search_action.setChecked(True)
            self.ai_search_action.setChecked(False)
            
            # 更新搜索框提示文字
            self.search_input.setPlaceholderText("Enter keyword...")
            
            # 如果當前有語意搜索結果，重置到普通搜索
            if self.is_semantic_search_active or self.semantic_search_results:
                self.reset_to_normal_search()
                
        elif mode == "ai":
            self.search_mode_button.setText("AI Search")
            self.search_mode_button.setStyleSheet("padding: 4px 8px; background-color: #4CAF50; color: white; border: none; border-radius: 4px;")
            self.normal_search_action.setChecked(False)
            self.ai_search_action.setChecked(True)
            
            # 更新搜索框提示文字
            self.search_input.setPlaceholderText("Describe the fields you are looking for...")
        
        # 觸發當前搜索
        self.trigger_search()
    
    def trigger_search(self):
        """Trigger search based on current mode"""
        if self.current_search_mode == "normal":
            self.filter_data()
        elif self.current_search_mode == "ai":
            query = self.search_input.text().strip()
            if query:
                self.perform_semantic_search()
            else:
                # 如果沒有輸入內容，顯示所有字段
                self.reset_to_normal_search()
    
    def on_search_input_changed(self):
        """Handle search input changes"""
        if self.current_search_mode == "normal":
            self.filter_data()
        elif self.current_search_mode == "ai":
            # AI搜尋模式下，暫時不執行即時搜索，等用戶輸入完成
            # 可以在這裡添加防抖邏輯，但目前保持簡單
            pass
    
    def on_search_enter_pressed(self):
        """Handle Enter key in search input"""
        if self.current_search_mode == "ai":
            query = self.search_input.text().strip()
            if query:
                self.perform_semantic_search()
            else:
                QMessageBox.warning(self, "Warning", "Please enter a search keyword")
        # 一般搜尋模式下，Enter鍵無特殊處理，因為textChanged已經處理了
    
    def create_sqlite_database(self, df, db_path):
        """將 DataFrame 轉換為 SQLite 數據庫"""
        conn = sqlite3.connect(db_path)
        
        # 確保Coverage是字符串並包含%
        if 'Coverage' in df.columns:
            df['Coverage'] = df['Coverage'].astype(str)
            df['Coverage'] = df['Coverage'].apply(lambda x: x if '%' in x else f"{x}%")
        
        try:
            # 將數據寫入SQLite
            df.to_sql('dataset', conn, if_exists='replace', index=False)
            conn.commit()
        finally:
            conn.close()

    def load_dataset(self, index):
        """Load and display dataset"""
        # 保存最後載入的索引，用於刷新功能
        self.last_loaded_index = index
        
        csv_path = self.file_model.filePath(index)
        try:
            # 建立對應的SQLite數據庫路徑
            db_path = csv_path.replace('.csv', '.db')
            
            # 讀取CSV數據並創建SQLite數據庫
            df = pd.read_csv(csv_path)
            self.create_sqlite_database(df, db_path)
            
            # 設置SQLite數據模型
            model = SqliteTableModel(db_path, 'dataset')
            
            # 設置代理模型用於過濾 - 使用自定義的子類
            self.proxy_model = CheckableSortFilterProxyModel()
            self.proxy_model.setSourceModel(model)
            self.proxy_model.setFilterCaseSensitivity(Qt.CaseInsensitive)
            # 使用 EditRole 作為排序依據，以保留 % 顯示
            self.proxy_model.setSortRole(Qt.EditRole)
            # Use EditRole for sorting so DisplayRole can show '%' correctly
            self.proxy_model.setSortRole(Qt.EditRole)

            self.table_view.setModel(self.proxy_model)

            # 連接數據變更信號（用於追蹤 checkbox 狀態變化）
            model.dataChanged.connect(self.on_data_changed)

            # --- 開始修改欄位寬度 ---
            header = self.table_view.horizontalHeader()
            columns = model.columns
            field_index = -1
            description_index = -1

            try:
                # 嘗試查找 'Field' 和 'Description' 欄位的索引 (忽略大小寫)
                field_index = next((i for i, col in enumerate(columns) if col.lower() == 'field'), -1)
                description_index = next((i for i, col in enumerate(columns) if col.lower() == 'description'), -1)
            except StopIteration:
                pass # 如果找不到欄位，索引將保持 -1

            # --- 設定固定且可拖動的欄位寬度 ---
            fixed_field_width = 200  # Fields 欄位的固定寬度 (像素)
            fixed_description_width = 350 # Description 欄位的固定寬度 (像素)

            for i, col in enumerate(columns):
                if i == field_index:
                    # 設置 'Field' 欄位為可互動調整，並設定固定初始寬度
                    header.setSectionResizeMode(i, QHeaderView.Interactive)
                    header.resizeSection(i, fixed_field_width)
                elif i == description_index:
                    # 設置 'Description' 欄位為可互動調整，並設定固定初始寬度
                    header.setSectionResizeMode(i, QHeaderView.Interactive)
                    header.resizeSection(i, fixed_description_width)
                else:
                   # 設置 checkbox 欄位的固定寬度
                   if i == 0:  # checkbox 列
                       header.setSectionResizeMode(i, QHeaderView.Fixed)
                       header.resizeSection(i, 30)  # 設置為 30 像素寬
                   else:
                       # 其他欄位保持自動拉伸
                       header.setSectionResizeMode(i, QHeaderView.Stretch)
            # --- 結束修改欄位寬度 ---

            # 連接表格點擊事件 - 先斷開之前的連接
            if self.click_connected:
                try:
                    # 斷開單擊事件連接
                    self.table_view.clicked.disconnect(self.handle_table_click)
                except (TypeError, RuntimeError): # RuntimeError if not connected
                    pass
                try:
                    # 斷開雙擊事件連接
                    self.table_view.doubleClicked.disconnect(self.handle_cell_click)
                except (TypeError, RuntimeError):
                    pass

            # 重新連接事件
            self.table_view.clicked.connect(self.handle_table_click) # 連接單擊事件
            self.table_view.doubleClicked.connect(self.handle_cell_click) # 連接雙擊事件
            self.click_connected = True # 更新標記

            # 更新當前數據集和狀態欄
            self.current_dataset = model
            file_name = os.path.basename(csv_path)
            self.status_bar.showMessage(f"Loaded {file_name} | {model.total_rows} rows")
            
            # 更新當前檔案標籤
            self.current_file_label.setText(file_name)
            
            # 添加欄位到過濾下拉框
            self.filter_column.clear()
            self.filter_column.addItem("All Columns")
            for col in model.columns:
                if col not in ["Coverage", "Users", "Alphas"]:
                    self.filter_column.addItem(col)
                
            # 更新圖表
            self.update_chart()
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error loading dataset: {str(e)}")
            
    def filter_data(self):
        if self.proxy_model is None:
            return
            
        # Only in normal search mode
        if self.current_search_mode != "normal":
            return
            
        # 如果用戶開始輸入普通搜索，重置語意搜索狀態
        if self.is_semantic_search_active or self.semantic_search_results:
            self.reset_to_normal_search()
            
        filter_text = self.search_input.text()
        filter_column = self.filter_column.currentText()
        
        if filter_column == "All Columns":
            # 搜索所有列 (注意：QSortFilterProxyModel的-1模式可能不適用於所有Qt版本)
            # 我們改為手動實現全欄位搜索
            self.proxy_model.setFilterKeyColumn(-1)
        else:
            try:
                column_index = self.current_dataset.columns.index(filter_column)
                self.proxy_model.setFilterKeyColumn(column_index)
            except (ValueError, AttributeError):
                # 如果找不到列或current_dataset不是SqliteTableModel，默認使用第一列
                self.proxy_model.setFilterKeyColumn(0)
        
        # 設置過濾文本
        self.proxy_model.setFilterFixedString(filter_text)
        
        # 更新狀態欄以顯示過濾後的記錄數
        filtered_count = self.proxy_model.rowCount()
        total_count = self.current_dataset.total_rows
        self.status_bar.showMessage(f"Showing {filtered_count}/{total_count} rows | Filter: {filter_column} - '{filter_text}'")
    
    def reset_to_normal_search(self):
        """Reset to normal search mode"""
        if not self.current_dataset:
            return
            
        # 如果當前是語意搜索的代理模型，需要重新創建普通的代理模型
        if hasattr(self.proxy_model, 'semantic_fields'):
            # 創建新的普通代理模型
            new_proxy = CheckableSortFilterProxyModel()
            new_proxy.setSourceModel(self.current_dataset)
            new_proxy.setFilterCaseSensitivity(Qt.CaseInsensitive)
            new_proxy.setSortRole(Qt.EditRole)
            
            # 替換代理模型
            old_proxy = self.proxy_model
            self.proxy_model = new_proxy
            self.table_view.setModel(self.proxy_model)
            
            # 連接數據變更信號
            self.current_dataset.dataChanged.connect(self.on_data_changed)
            
            # 設置列寬
            self.setup_table_columns()
            
            # 清理舊的代理模型
            if old_proxy:
                old_proxy.deleteLater()
        
        # 清除語意搜索狀態
        self.semantic_search_results = []
        self.is_semantic_search_active = False
        
        # 更新過濾下拉框，移除"語意搜索結果"選項
        current_text = self.filter_column.currentText()
        if current_text == "語意搜索結果":
            self.filter_column.setCurrentText("All Columns")

    def update_chart(self):
        if self.current_dataset is None:
            return
            
        # 清除當前圖表
        self.canvas.figure.clear()
        ax = self.canvas.figure.add_subplot(111)
        
        chart_type = self.chart_type.currentText()
        model = self.current_dataset
        
        # 增加字體大小使圖表更清晰
        plt.rcParams.update({'font.size': 12})
        
        try:
            cursor = model.conn.cursor()
            
            if chart_type == "Coverage Distribution":
                # 使用SQL查詢獲取Coverage數據
                cursor.execute("""
                    SELECT CAST(REPLACE(Coverage, '%', '') AS FLOAT) as coverage_value 
                    FROM dataset
                    WHERE Coverage IS NOT NULL
                """)
                coverage_values = [row[0] for row in cursor.fetchall()]
                
                if coverage_values:
                    # 創建直方圖
                    ax.hist(coverage_values, bins=10, color='skyblue', edgecolor='black')
                    ax.set_title('Coverage Distribution')
                    ax.set_xlabel('Coverage (%)')
                    ax.set_ylabel('Field Count')
                
            elif chart_type == "Users":
                # 獲取前15個最多使用者的記錄
                cursor.execute("""
                    SELECT Field, Users 
                    FROM dataset 
                    ORDER BY CAST(Users AS INTEGER) DESC 
                    LIMIT 15
                """)
                results = cursor.fetchall()
                
                if results:
                    fields = [row[0] for row in results]
                    users = [row[1] for row in results]
                    
                    # 繪製條形圖
                    bars = ax.barh(fields, users, color='lightgreen')
                    ax.set_title('Top 15 Fields by Users')
                    ax.set_xlabel('Users')
                    ax.set_ylabel('Field')
                    ax.invert_yaxis()  # 反轉Y軸使最大值在頂部
                    
                    # 為條形圖添加數值標籤
                    for bar in bars:
                        width = bar.get_width()
                        ax.text(width, bar.get_y() + bar.get_height()/2, 
                               f' {int(width)}', va='center')
                
            elif chart_type == "Alphas":
                # 獲取前15個最多Alpha的記錄
                cursor.execute("""
                    SELECT Field, Alphas 
                    FROM dataset 
                    ORDER BY CAST(Alphas AS INTEGER) DESC 
                    LIMIT 15
                """)
                results = cursor.fetchall()
                
                if results:
                    fields = [row[0] for row in results]
                    alphas = [row[1] for row in results]
                    
                    # 繪製條形圖
                    bars = ax.barh(fields, alphas, color='coral')
                    ax.set_title('Top 15 Fields by Alphas')
                    ax.set_xlabel('Alphas')
                    ax.set_ylabel('Field')
                    ax.invert_yaxis()  # 反轉Y軸使最大值在頂部
                    
                    # 為條形圖添加數值標籤
                    for bar in bars:
                        width = bar.get_width()
                        ax.text(width, bar.get_y() + bar.get_height()/2, 
                               f' {int(width)}', va='center')
                
            elif chart_type == "Type Distribution":
                # 計算每種類型的數量
                cursor.execute("""
                    SELECT Type, COUNT(*) as count 
                    FROM dataset 
                    GROUP BY Type
                    ORDER BY count DESC
                """)
                results = cursor.fetchall()
                
                if results:
                    types = [row[0] for row in results]
                    counts = [row[1] for row in results]
                    
                    # 計算百分比
                    total = sum(counts)
                    percentages = [count/total*100 for count in counts]
                    
                    # 繪製餅圖
                    wedges, texts, autotexts = ax.pie(
                        counts, 
                        labels=types, 
                        autopct='%1.1f%%',
                        shadow=False, 
                        startangle=90,
                        colors=plt.cm.Paired.colors,
                        wedgeprops={'edgecolor': 'white', 'linewidth': 1}
                    )
                    ax.set_title('Field Type Distribution')
                    ax.axis('equal')
                    
                    # 增加餅圖標籤的可讀性
                    for text in texts + autotexts:
                        text.set_fontsize(11)
                        
        except Exception as e:
            print(f"Error plotting chart: {str(e)}")
            
        # 調整圖表佈局以確保所有元素都顯示
        self.canvas.figure.tight_layout()
        self.canvas.draw()

    def handle_cell_click(self, index): # Renamed conceptually, now handles double-click
        """Handle double-click to show details"""
        # 確保當前有數據集和模型
        if self.current_dataset is None or self.proxy_model is None:
            self.status_bar.showMessage("Cannot handle double-click: dataset or model not ready")
            return

        # 獲取視圖列索引
        view_column_index = index.column()

        # 雙擊 Checkbox 列不做任何事
        if view_column_index == 0:
            return

        # --- 以下是處理非 Checkbox 列雙擊的邏輯 ---

        # 獲取源模型的索引和列名
        source_index = self.proxy_model.mapToSource(index)
        if not source_index.isValid():
             self.status_bar.showMessage("Cannot handle double-click: invalid source index")
             return

        source_column_index = source_index.column()
        if source_column_index == 0: # Checkbox 列
             return
        original_col_idx = source_column_index - 1
        if original_col_idx < 0 or original_col_idx >= len(self.current_dataset.original_columns):
             self.status_bar.showMessage(f"Cannot handle double-click: mapped column {original_col_idx} out of range")
             return

        column_name = self.current_dataset.original_columns[original_col_idx]

        # 獲取數據
        try:
            source_row = source_index.row()

            if source_row >= self.current_dataset.total_rows:
                self.status_bar.showMessage(f"Cannot handle double-click: row index {source_row} out of range")
                return

            value = self.current_dataset.get_value(source_row, column_name)

            if value is None:
                 self.status_bar.showMessage("Cannot get cell data")
                 return

            # 調試信息
            self.status_bar.showMessage(f"Double-click: col={column_name}, row={source_row}, len={len(str(value))}")

            # 只有當內容超過一定長度或特定列時才顯示詳細信息
            show_detail = False
            if column_name.lower() == 'description' or 'description' in column_name.lower():
                show_detail = True
            elif len(str(value)) > 15: # 閾值可以調整
                show_detail = True

            if show_detail:
                try:
                    title = f"{column_name} - Details"
                    # 如果是 Description 列，嘗試獲取 Field 名稱作為標題的一部分
                    if column_name.lower() == 'description' or 'description' in column_name.lower():
                        field_name = "Unknown Field"
                        field_col_name = next((col for col in self.current_dataset.original_columns if col.lower() == 'field'), None)
                        if field_col_name:
                            field_name_val = self.current_dataset.get_value(source_row, field_col_name)
                            if field_name_val: # 確保字段名不是 None 或空
                                field_name = str(field_name_val)
                        title = f"{field_name} - Detailed Description"

                    # 使用防重複邏輯：檢查是否已經有相同的對話框打開
                    for child in self.children():
                        if isinstance(child, DetailDialog) and child.isVisible():
                            child.close()  # 關閉之前的對話框
                            break

                    dialog = DetailDialog(title, str(value), self)
                    dialog.exec()
                except Exception as e:
                    self.status_bar.showMessage(f"Error showing details: {str(e)}")
            # else: # 如果不需要顯示詳細信息，可以在這裡添加其他雙擊行為，或保持不變
            #     pass

        except Exception as e:
            self.status_bar.showMessage(f"Error handling double-click: {str(e)}")

    def handle_table_click(self, index):
        """處理表格單擊事件，主要用於切換 checkbox 狀態"""
        if not index.isValid():
            return

        # 檢查是否點擊了第一列 (checkbox 列)
        if index.column() == 0:
            # 獲取源模型索引
            source_index = self.proxy_model.mapToSource(index)
            if not source_index.isValid():
                return

            # 獲取源模型和當前狀態
            source_model = self.proxy_model.sourceModel()
            current_state = source_model.data(source_index, Qt.CheckStateRole)

            # 切換狀態
            new_state = Qt.Unchecked if current_state == Qt.Checked else Qt.Checked

            # 更新模型數據
            source_model.setData(source_index, new_state, Qt.CheckStateRole)
            # setData 會自動觸發 dataChanged 信號，無需手動發送

    def show_context_menu(self, pos):
        """Show context menu; actions depend on check states of selected rows"""
        menu = QMenu(self)
        selected_indexes = self.table_view.selectedIndexes()
        if not selected_indexes:
            return

        # 取得唯一行 (視圖行)
        view_rows = set(index.row() for index in selected_indexes)
        if not view_rows:
            return

        # 檢查是否所有選中行都已勾選
        source_model = self.proxy_model.sourceModel()
        all_checked = True
        some_checked = False # 新增: 檢查是否有任何一個被勾選
        valid_source_rows = [] # 儲存有效的源行號

        for view_row in view_rows:
            source_index = self.proxy_model.mapToSource(self.proxy_model.index(view_row, 0))
            if source_index.isValid():
                source_row = source_index.row()
                valid_source_rows.append(source_row) # 記錄有效的源行
                if source_row in source_model.checked_rows:
                    some_checked = True
                else:
                    all_checked = False
            else:
                 # 如果有任何一個選中的視圖行無法映射到有效的源行，則不認為全部勾選
                 all_checked = False


        # 如果沒有有效的源行，則不顯示選單
        if not valid_source_rows:
             return

        if all_checked:
            # If all checked, show Uncheck Selected Items
            action = menu.addAction("Uncheck Selected Items")
            action.triggered.connect(lambda: self.check_selected_items(False, view_rows)) # 傳遞 view_rows
        elif some_checked:
             # If some checked, also show Uncheck Selected Items (prefer uncheck)
             action = menu.addAction("Uncheck Selected Items")
             action.triggered.connect(lambda: self.check_selected_items(False, view_rows)) # 傳遞 view_rows
        else:
            # If none checked, show Check Selected Items
            action = menu.addAction("Check Selected Items")
            action.triggered.connect(lambda: self.check_selected_items(True, view_rows)) # 傳遞 view_rows

        menu.exec_(self.table_view.viewport().mapToGlobal(pos))

    def check_selected_items(self, check=True, view_rows=None):
        """Check or uncheck the specified view rows"""
        # 如果沒有傳遞 view_rows，則從當前選擇獲取
        if view_rows is None:
            selected_indexes = self.table_view.selectedIndexes()
            if not selected_indexes:
                return
            view_rows = set(index.row() for index in selected_indexes)

        if not view_rows:
            return

        source_model = self.proxy_model.sourceModel()
        min_row = float('inf')
        max_row = float('-inf')

        for view_row in view_rows:
            source_index = self.proxy_model.mapToSource(self.proxy_model.index(view_row, 0))
            if source_index.isValid():
                source_row = source_index.row()
                if check:
                    source_model.checked_rows.add(source_row)
                else:
                    source_model.checked_rows.discard(source_row)
                min_row = min(min_row, source_row)
                max_row = max(max_row, source_row)

        # 優化 dataChanged 信號發送範圍
        if min_row <= max_row:
            source_model.dataChanged.emit(
                source_model.index(min_row, 0),
                source_model.index(max_row, 0),
                [Qt.CheckStateRole] # 只更新 CheckStateRole
            )
        else:
             # 如果沒有有效的行被更改，可能需要刷新整個視圖或特定區域
             # 這裡我們選擇刷新第一列，以防萬一
             source_model.dataChanged.emit(
                 source_model.index(0, 0),
                 source_model.index(source_model.rowCount()-1, 0),
                 [Qt.CheckStateRole]
             )

    def on_data_changed(self, topLeft, bottomRight):
        """當數據變更時（如 checkbox 狀態改變）更新 UI 狀態"""
        if self.current_dataset:
            checked_count = len(self.current_dataset.checked_rows)
            if checked_count > 0:
                self.export_button.setEnabled(True)
                self.export_button.setToolTip(f"Selected {checked_count} field(s)")
            else:
                self.export_button.setEnabled(False)
                self.export_button.setToolTip("Please check fields to import")

    def export_checked_fields(self):
        """Export checked fields to Strategy Generator"""
        if not self.current_dataset:
            return
            
        checked_fields = self.current_dataset.get_checked_fields()
        if checked_fields:
            self.fields_selected_for_generator.emit(checked_fields)
            # QMessageBox.information(self, "成功", f"已將 {len(checked_fields)} 個字段匯入到策略生成器")
            # 取消所有勾選
            self.current_dataset.checked_rows.clear()
            # 更新表格顯示
            self.current_dataset.dataChanged.emit(
                self.current_dataset.index(0, 0),
                self.current_dataset.index(self.current_dataset.rowCount()-1, 0)
            )
        else:
            QMessageBox.warning(self, "Warning", "No fields checked")

    def perform_semantic_search(self):
        """Perform semantic search"""
        if not GEMINI_AVAILABLE:
            QMessageBox.warning(self, "Warning", "Semantic search unavailable. Check GEMINI_API_KEY.")
            return
            
        if not self.current_dataset:
            QMessageBox.warning(self, "Warning", "Please load a dataset first")
            return
            
        query = self.search_input.text().strip()
        if not query:
            QMessageBox.warning(self, "Warning", "Please enter a search keyword")
            return
            
        # 獲取所有字段和描述數據
        try:
            cursor = self.current_dataset.conn.cursor()
            cursor.execute("SELECT Field, Description FROM dataset")
            field_data = cursor.fetchall()
            
            if not field_data:
                QMessageBox.warning(self, "Warning", "No searchable fields in dataset")
                return
                
            # 開始語意搜索
            self.start_semantic_search(query, field_data)
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error fetching data: {str(e)}")
    
    def start_semantic_search(self, query, field_data):
        """Start semantic search worker thread"""
        # 顯示進度條並禁用按鈕
        self.search_progress.setVisible(True)
        self.search_mode_button.setEnabled(False)
        self.search_mode_button.setText("Searching...")
        self.is_semantic_search_active = True
        
        # 創建並啟動工作線程
        self.semantic_search_worker = SemanticSearchWorker(query, field_data)
        self.semantic_search_worker.finished.connect(self.on_semantic_search_finished)
        self.semantic_search_worker.error.connect(self.on_semantic_search_error)
        self.semantic_search_worker.start()
        
        self.status_bar.showMessage(f"Semantic search in progress: {query}")
    
    def on_semantic_search_finished(self, result_fields):
        """Handle semantic search completion"""
        print(f"語意搜索完成回調，收到字段: {result_fields}")
        print(f"字段數量: {len(result_fields) if result_fields else 0}")
        
        # Hide progress and restore controls
        self.search_progress.setVisible(False)
        self.search_mode_button.setEnabled(True)
        self.search_mode_button.setText("AI Search")
        self.is_semantic_search_active = False
        
        if result_fields:
            print(f"開始應用語意過濾，字段列表: {result_fields}")
            self.semantic_search_results = result_fields
            self.apply_semantic_filter(result_fields)
            self.status_bar.showMessage(f"Semantic search finished: found {len(result_fields)} related fields")
        else:
            print("沒有找到相關字段")
            self.status_bar.showMessage("Semantic search finished: no related fields found")
            QMessageBox.information(self, "Search Results", "No fields found for the query")
    
    def on_semantic_search_error(self, error_message):
        """Handle semantic search error"""
        print(f"語意搜索錯誤: {error_message}")
        
        # Hide progress and restore controls
        self.search_progress.setVisible(False)
        self.search_mode_button.setEnabled(True)
        self.search_mode_button.setText("AI Search")
        self.is_semantic_search_active = False
        
        QMessageBox.critical(self, "Search Error", error_message)
        self.status_bar.showMessage("Semantic search failed")
    
    def apply_semantic_filter(self, result_fields):
        """應用語意搜索的過濾結果"""
        print(f"開始應用語意過濾，收到字段: {result_fields}")
        
        if not self.proxy_model or not self.current_dataset:
            print("代理模型或當前數據集為空，無法應用過濾")
            return
        
        print(f"當前數據集總行數: {self.current_dataset.total_rows}")
        print(f"當前數據集列: {self.current_dataset.original_columns}")
        
        # 測試前幾行的字段值與語意字段的匹配情況
        print("\n=== 測試字段匹配 ===")
        try:
            cursor = self.current_dataset.conn.cursor()
            cursor.execute("SELECT Field FROM dataset LIMIT 10")
            sample_fields = [row[0] for row in cursor.fetchall()]
            print(f"數據集中的前10個字段: {sample_fields}")
            
            print(f"AI搜索返回的字段: {result_fields}")
            
            print("\n匹配測試:")
            matched_count = 0
            for i, ai_field in enumerate(result_fields):
                ai_str = str(ai_field).strip()
                matched = False
                match_details = []
                
                # 檢查這個AI字段是否能在數據集的任何字段中找到匹配
                for j, dataset_field in enumerate(sample_fields):
                    dataset_str = str(dataset_field).strip()
                    
                    # 測試各種匹配方式
                    if ai_str == dataset_str:
                        matched = True
                        match_details.append(f"精確匹配數據集字段[{j}] '{dataset_str}'")
                        break
                    elif ai_str.lower() == dataset_str.lower():
                        matched = True
                        match_details.append(f"大小寫不敏感匹配數據集字段[{j}] '{dataset_str}'")
                        break
                    elif ai_str.lower() in dataset_str.lower():
                        matched = True
                        match_details.append(f"部分匹配：數據集字段[{j}] '{dataset_str}' 包含 AI字段")
                        break
                    elif dataset_str.lower() in ai_str.lower():
                        matched = True
                        match_details.append(f"部分匹配：AI字段包含數據集字段[{j}] '{dataset_str}'")
                        break
                
                if matched:
                    matched_count += 1
                
                status = "✓" if matched else "✗"
                match_info = match_details[0] if match_details else "在前10個字段中無匹配"
                print(f"  {status} AI字段[{i}] '{ai_str}' -> {match_info}")
            
            print(f"\n匹配摘要: {matched_count}/{len(result_fields)} 個AI字段在前10個數據集字段中找到匹配")
            
            # 如果前10個字段中匹配數很少，嘗試搜索更多字段
            if matched_count < len(result_fields) * 0.5:
                print("\n前10個字段匹配率較低，擴展搜索範圍到前50個字段...")
                cursor.execute("SELECT Field FROM dataset LIMIT 50")
                extended_fields = [row[0] for row in cursor.fetchall()]
                
                extended_matched = 0
                for ai_field in result_fields:
                    ai_str = str(ai_field).strip()
                    for dataset_field in extended_fields:
                        dataset_str = str(dataset_field).strip()
                        if (ai_str.lower() == dataset_str.lower() or 
                            ai_str.lower() in dataset_str.lower() or 
                            dataset_str.lower() in ai_str.lower()):
                            extended_matched += 1
                            break
                
                print(f"擴展搜索結果: {extended_matched}/{len(result_fields)} 個AI字段在前50個數據集字段中找到匹配")
                
        except Exception as e:
            print(f"測試字段匹配時發生錯誤: {str(e)}")
        
        print("=== 測試結束 ===\n")
        
        # 創建自定義過濾邏輯
        class SemanticFilterProxyModel(CheckableSortFilterProxyModel):
            def __init__(self, semantic_fields):
                super().__init__()
                self.semantic_fields = semantic_fields
                self.match_count = 0  # 統計匹配的行數
                self.total_checked = 0  # 統計檢查的總行數
                print(f"SemanticFilterProxyModel 初始化，語意字段: {semantic_fields}")
                
            def filterAcceptsRow(self, source_row, source_parent):
                self.total_checked += 1
                
                # 限制調試輸出，只顯示前5行的詳細信息
                debug_detail = source_row < 5
                
                # 獲取源模型
                source_model = self.sourceModel()
                if not source_model:
                    if debug_detail:
                        print(f"行 {source_row}: 源模型為空")
                    return True
                    
                # 獲取Field列的值
                try:
                    field_value = source_model.get_value(source_row, 'Field')
                    if field_value is None:
                        if debug_detail:
                            print(f"行 {source_row}: 字段值為 None")
                        return False
                    
                    field_str = str(field_value).strip()
                    
                    if debug_detail:
                        print(f"行 {source_row}: 數據集字段 '{field_str}'")
                        print(f"  要匹配的AI字段: {self.semantic_fields}")
                    
                    # 檢查 Gemini 返回的每個字段是否與當前行的字段匹配
                    for i, semantic_field in enumerate(self.semantic_fields):
                        semantic_str = str(semantic_field).strip()
                        
                        # 1. 精確匹配
                        if field_str == semantic_str:
                            if debug_detail:
                                print(f"  ✓ 精確匹配: 數據集 '{field_str}' == AI搜索[{i}] '{semantic_str}'")
                            self.match_count += 1
                            return True
                        
                        # 2. 大小寫不敏感匹配
                        if field_str.lower() == semantic_str.lower():
                            if debug_detail:
                                print(f"  ✓ 大小寫不敏感匹配: 數據集 '{field_str}' ~= AI搜索[{i}] '{semantic_str}'")
                            self.match_count += 1
                            return True
                        
                        # 3. 移除特殊字符後匹配
                        import re
                        field_clean = re.sub(r'[^a-zA-Z0-9]', '', field_str.lower())
                        semantic_clean = re.sub(r'[^a-zA-Z0-9]', '', semantic_str.lower())
                        if field_clean == semantic_clean and field_clean:
                            if debug_detail:
                                print(f"  ✓ 清理後匹配: 數據集 '{field_str}' -> '{field_clean}' == AI搜索[{i}] '{semantic_str}' -> '{semantic_clean}'")
                            self.match_count += 1
                            return True
                        
                        # 4. 部分匹配：AI字段包含在數據集字段中
                        field_lower = field_str.lower()
                        semantic_lower = semantic_str.lower()
                        
                        if len(semantic_str) >= 3 and semantic_lower in field_lower:
                            if debug_detail:
                                print(f"  ✓ 部分匹配: 數據集字段 '{field_str}' 包含 AI搜索[{i}] '{semantic_str}'")
                            self.match_count += 1
                            return True
                        
                        # 5. 部分匹配：數據集字段包含在AI字段中
                        if len(field_str) >= 3 and field_lower in semantic_lower:
                            if debug_detail:
                                print(f"  ✓ 部分匹配: AI搜索[{i}] '{semantic_str}' 包含 數據集字段 '{field_str}'")
                            self.match_count += 1
                            return True
                        
                        # 6. 單詞級別匹配（分割後檢查）
                        field_words = set(re.findall(r'\w+', field_lower))
                        semantic_words = set(re.findall(r'\w+', semantic_lower))
                        
                        # 如果有共同單詞且單詞長度>=3
                        common_words = field_words & semantic_words
                        if common_words and any(len(word) >= 3 for word in common_words):
                            if debug_detail:
                                print(f"  ✓ 單詞匹配: 數據集 '{field_str}' 與 AI搜索[{i}] '{semantic_str}' 有共同單詞 {common_words}")
                            self.match_count += 1
                            return True
                    
                    if debug_detail:
                        print(f"  ✗ 未匹配: 數據集字段 '{field_str}' 與任何AI搜索結果都不匹配")
                    elif source_row == 5:
                        print(f"... (後續行的詳細調試信息已省略)")
                        print(f"已檢查 {self.total_checked} 行，找到 {self.match_count} 個匹配")
                        
                    return False
                    
                except Exception as e:
                    if debug_detail:
                        print(f"行 {source_row}: 獲取字段值時出錯: {str(e)}")
                    return False
            
            def get_statistics(self):
                return f"匹配統計: {self.match_count}/{self.total_checked} 行匹配"

        # 替換代理模型
        old_proxy = self.proxy_model
        print("創建新的語意過濾代理模型")
        self.proxy_model = SemanticFilterProxyModel(result_fields)
        self.proxy_model.setSourceModel(self.current_dataset)
        self.proxy_model.setFilterCaseSensitivity(Qt.CaseInsensitive)
        self.proxy_model.setSortRole(Qt.EditRole)
        
        # 更新表格視圖
        print("更新表格視圖模型")
        self.table_view.setModel(self.proxy_model)
        
        # 連接數據變更信號
        self.current_dataset.dataChanged.connect(self.on_data_changed)
        
        # 設置列寬
        print("設置表格列寬")
        self.setup_table_columns()
        
        # 檢查過濾後的行數
        filtered_rows = self.proxy_model.rowCount()
        print(f"過濾後顯示行數: {filtered_rows}")
        
        # 顯示匹配統計信息
        if hasattr(self.proxy_model, 'get_statistics'):
            print(self.proxy_model.get_statistics())
        
        # 如果沒有匹配結果，提供建議
        if filtered_rows == 0:
            print("⚠️  沒有找到匹配的字段！")
            print("建議:")
            print("1. 檢查AI返回的字段名稱是否與數據集中的實際字段名稱相符")
            print("2. 嘗試使用更通用的搜索詞")
            print("3. 檢查數據集是否包含相關字段")
        else:
            print(f"✅ 成功找到 {filtered_rows} 個匹配的字段")
        
        # 清理舊的代理模型
        if old_proxy:
            old_proxy.deleteLater()
        
        print("語意過濾應用完成")
    
    def setup_table_columns(self):
        """設置表格列寬"""
        if not self.current_dataset:
            return
            
        header = self.table_view.horizontalHeader()
        columns = self.current_dataset.columns
        field_index = -1
        description_index = -1

        try:
            field_index = next((i for i, col in enumerate(columns) if col.lower() == 'field'), -1)
            description_index = next((i for i, col in enumerate(columns) if col.lower() == 'description'), -1)
        except StopIteration:
            pass

        fixed_field_width = 200
        fixed_description_width = 350

        for i, col in enumerate(columns):
            if i == field_index:
                header.setSectionResizeMode(i, QHeaderView.Interactive)
                header.resizeSection(i, fixed_field_width)
            elif i == description_index:
                header.setSectionResizeMode(i, QHeaderView.Interactive)
                header.resizeSection(i, fixed_description_width)
            else:
                if i == 0:  # checkbox 列
                    header.setSectionResizeMode(i, QHeaderView.Fixed)
                    header.resizeSection(i, 30)
                else:
                    header.setSectionResizeMode(i, QHeaderView.Stretch)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    # 設置應用程序樣式
    app.setStyle("Fusion")
    
    # 檢查數據集目錄是否存在
    if not os.path.exists("datasets"):
        QMessageBox.critical(None, "錯誤", "找不到 'datasets' 資料夾! 請確保程式運行在正確的目錄中。")
        sys.exit(1)
        
    window = MainWindow()
    window.show()
    sys.exit(app.exec())


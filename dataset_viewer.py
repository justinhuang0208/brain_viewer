import sys
import os
import sqlite3
import pandas as pd
from PySide6.QtWidgets import (QApplication, QMainWindow, QTableView, QTreeView,
                              QSplitter, QVBoxLayout, QHBoxLayout, QWidget,
                              QLineEdit, QLabel, QComboBox, QFileSystemModel,
                              QHeaderView, QPushButton, QStatusBar, QTabWidget,
                              QMessageBox, QDialog, QTextEdit, QVBoxLayout, QFrame,
                              QMenu)
from PySide6.QtCore import Qt, QDir, QModelIndex, QSortFilterProxyModel, Signal, Slot, QAbstractTableModel
from PySide6.QtGui import QColor, QFont, QPalette, QIcon
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import matplotlib

# 設置matplotlib中文支持
# 嘗試設置繁體中文字體
def set_chinese_font():
    # 嘗試多種中文字體，按優先順序
    chinese_fonts = ['Microsoft JhengHei', 'DFKai-SB', 'SimHei', 'STHeiti', 'STSong', 'Arial Unicode MS', 'SimSun']
    
    # 在Windows上查找可用字體
    if os.name == 'nt':
        from matplotlib.font_manager import FontManager
        fm = FontManager()
        available_fonts = set([f.name for f in fm.ttflist])
        
        for font in chinese_fonts:
            if font in available_fonts:
                print(f"使用中文字體: {font}")
                matplotlib.rcParams['font.family'] = font
                return True
                
    # 如果找不到特定中文字體，嘗試使用系統預設配置
    try:
        # 設置繁體中文
        plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Arial Unicode MS', 'sans-serif']
        plt.rcParams['axes.unicode_minus'] = False  # 解決負號顯示問題
        return True
    except:
        return False

# 嘗試設置中文字體
set_chinese_font()

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
        
        # 添加關閉按鈕
        close_button = QPushButton("關閉")
        close_button.clicked.connect(self.accept)
        
        layout.addWidget(close_button)
        self.setLayout(layout)

# 主窗口
class MainWindow(QMainWindow):
    # 定義新的信號，用於將選中的字段發送到生成器
    fields_selected_for_generator = Signal(list)

    def __init__(self):
        super(MainWindow, self).__init__()
        self.setWindowTitle("WorldQuant Brain 數據集瀏覽器")
        self.setMinimumSize(1200, 800)
        
        # 設置主窗口佈局
        main_layout = QVBoxLayout()
        
        # 頂部佈局：導航與搜索
        top_layout = QHBoxLayout()
        
        # 創建標籤區域和搜索區域
        top_left = QHBoxLayout()
        top_right = QHBoxLayout()
        
        # 資料集標籤 - 使用小標籤，不是大標題
        dataset_label = QLabel("資料集:")
        
        # 當前檔案標籤 - 顯示當前載入的檔案
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
        self.filter_column.addItem("Field")
        self.filter_column.addItem("Description")
        self.filter_column.addItem("Type")
        self.filter_column.currentIndexChanged.connect(self.filter_data)
        self.filter_column.setMinimumWidth(120)
        
        # 添加刷新按鈕
        refresh_button = QPushButton("刷新")
        refresh_button.setToolTip("重新載入目前選擇的資料集")
        refresh_button.clicked.connect(self.refresh_current_dataset)
        refresh_button.setStyleSheet("padding: 4px 12px;")
        
        # 布局左側標籤區域
        top_left.addWidget(dataset_label)
        top_left.addWidget(self.current_file_label)
        top_left.addStretch()
        
        # 布局右側搜索區域
        top_right.addWidget(search_label)
        top_right.addWidget(self.search_input)
        # 添加匯出按鈕
        self.export_button = QPushButton("匯入選中字段到生成器")
        self.export_button.clicked.connect(self.export_checked_fields)
        self.export_button.setEnabled(False)  # 初始時禁用
        self.export_button.setStyleSheet("padding: 4px 12px;")
        self.export_button.setToolTip("請先勾選要匯入的字段")

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
        files_title = QLabel("數據集文件")
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
        self.chart_type.addItems(["覆蓋率分佈", "使用者數量", "Alpha數量", "類型分佈"])
        self.chart_type.currentIndexChanged.connect(self.update_chart)
        
        chart_controls.addWidget(QLabel("圖表類型:"))
        chart_controls.addWidget(self.chart_type)
        chart_controls.addStretch()
        
        chart_layout.addLayout(chart_controls)
        chart_layout.addWidget(self.canvas)
        self.chart_widget.setLayout(chart_layout)
        
        # 添加標籤頁
        self.tab_widget.addTab(self.table_view, "數據表格")
        self.tab_widget.addTab(self.chart_widget, "數據視覺化")
        
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
        
        # 顯示初始信息
        self.status_bar.showMessage("選擇左側的數據集文件以開始瀏覽")

    # 新增刷新當前資料集功能
    def refresh_current_dataset(self):
        if not hasattr(self, 'last_loaded_index') or self.last_loaded_index is None:
            self.status_bar.showMessage("沒有已載入的資料集可刷新")
            return
            
        self.load_dataset(self.last_loaded_index)
        self.status_bar.showMessage("已刷新當前資料集")

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
        """載入並顯示數據集"""
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
            self.status_bar.showMessage(f"已載入 {file_name} | 共 {model.total_rows} 條記錄")
            
            # 更新當前檔案標籤
            self.current_file_label.setText(file_name)
            
            # 添加欄位到過濾下拉框
            self.filter_column.clear()
            self.filter_column.addItem("全部欄位")
            for col in model.columns:
                self.filter_column.addItem(col)
                
            # 更新圖表
            self.update_chart()
            
        except Exception as e:
            QMessageBox.critical(self, "錯誤", f"載入數據集時發生錯誤：{str(e)}")
            
    def filter_data(self):
        if self.proxy_model is None:
            return
            
        filter_text = self.search_input.text()
        filter_column = self.filter_column.currentText()
        
        if filter_column == "全部欄位":
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
        self.status_bar.showMessage(f"顯示 {filtered_count}/{total_count} 條記錄 | 過濾條件: {filter_column} - '{filter_text}'")
        
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
            
            if chart_type == "覆蓋率分佈":
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
                    ax.set_title('覆蓋率分佈')
                    ax.set_xlabel('覆蓋率 (%)')
                    ax.set_ylabel('變數數量')
                
            elif chart_type == "使用者數量":
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
                    ax.set_title('使用者數量最多的前15個變數')
                    ax.set_xlabel('使用者數量')
                    ax.set_ylabel('變數名稱')
                    ax.invert_yaxis()  # 反轉Y軸使最大值在頂部
                    
                    # 為條形圖添加數值標籤
                    for bar in bars:
                        width = bar.get_width()
                        ax.text(width, bar.get_y() + bar.get_height()/2, 
                               f' {int(width)}', va='center')
                
            elif chart_type == "Alpha數量":
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
                    ax.set_title('Alpha數量最多的前15個變數')
                    ax.set_xlabel('Alpha數量')
                    ax.set_ylabel('變數名稱')
                    ax.invert_yaxis()  # 反轉Y軸使最大值在頂部
                    
                    # 為條形圖添加數值標籤
                    for bar in bars:
                        width = bar.get_width()
                        ax.text(width, bar.get_y() + bar.get_height()/2, 
                               f' {int(width)}', va='center')
                
            elif chart_type == "類型分佈":
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
                    ax.set_title('變數類型分佈')
                    ax.axis('equal')
                    
                    # 增加餅圖標籤的可讀性
                    for text in texts + autotexts:
                        text.set_fontsize(11)
                        
        except Exception as e:
            print(f"繪製圖表時發生錯誤: {str(e)}")
            
        # 調整圖表佈局以確保所有元素都顯示
        self.canvas.figure.tight_layout()
        self.canvas.draw()

    def handle_cell_click(self, index): # Renamed conceptually, now handles double-click
        """處理單元格雙擊事件，顯示詳細信息"""
        # 確保當前有數據集和模型
        if self.current_dataset is None or self.proxy_model is None:
            self.status_bar.showMessage("無法處理雙擊：數據集或模型不存在")
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
             self.status_bar.showMessage("無法處理雙擊：無效的源索引")
             return

        source_column_index = source_index.column()
        if source_column_index == 0: # Checkbox 列
             return
        original_col_idx = source_column_index - 1
        if original_col_idx < 0 or original_col_idx >= len(self.current_dataset.original_columns):
             self.status_bar.showMessage(f"無法處理雙擊：計算出的原始列索引 {original_col_idx} 超出範圍")
             return

        column_name = self.current_dataset.original_columns[original_col_idx]

        # 獲取數據
        try:
            source_row = source_index.row()

            if source_row >= self.current_dataset.total_rows:
                self.status_bar.showMessage(f"無法處理雙擊：行索引 {source_row} 超出範圍")
                return

            value = self.current_dataset.get_value(source_row, column_name)

            if value is None:
                 self.status_bar.showMessage("無法獲取單元格數據")
                 return

            # 調試信息
            self.status_bar.showMessage(f"雙擊了: 列={column_name}, 行={source_row}, 值長度={len(str(value))}")

            # 只有當內容超過一定長度或特定列時才顯示詳細信息
            show_detail = False
            if column_name.lower() == 'description' or 'description' in column_name.lower():
                show_detail = True
            elif len(str(value)) > 15: # 閾值可以調整
                show_detail = True

            if show_detail:
                try:
                    title = f"{column_name} - 詳細內容"
                    # 如果是 Description 列，嘗試獲取 Field 名稱作為標題的一部分
                    if column_name.lower() == 'description' or 'description' in column_name.lower():
                        field_name = "未知字段"
                        field_col_name = next((col for col in self.current_dataset.original_columns if col.lower() == 'field'), None)
                        if field_col_name:
                            field_name_val = self.current_dataset.get_value(source_row, field_col_name)
                            if field_name_val: # 確保字段名不是 None 或空
                                field_name = str(field_name_val)
                        title = f"{field_name} - 詳細描述"

                    # 使用防重複邏輯：檢查是否已經有相同的對話框打開
                    for child in self.children():
                        if isinstance(child, DetailDialog) and child.isVisible():
                            child.close()  # 關閉之前的對話框
                            break

                    dialog = DetailDialog(title, str(value), self)
                    dialog.exec()
                except Exception as e:
                    self.status_bar.showMessage(f"顯示詳細信息時出錯: {str(e)}")
            # else: # 如果不需要顯示詳細信息，可以在這裡添加其他雙擊行為，或保持不變
            #     pass

        except Exception as e:
            self.status_bar.showMessage(f"處理單元格雙擊時出錯: {str(e)}")

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
        """顯示右鍵選單，根據選中行的勾選狀態動態顯示選項"""
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
            # 如果全都已勾選，顯示「取消勾選選中項目」
            action = menu.addAction("取消勾選選中項目")
            action.triggered.connect(lambda: self.check_selected_items(False, view_rows)) # 傳遞 view_rows
        elif some_checked:
             # 如果部分勾選，也顯示「取消勾選選中項目」(優先取消勾選)
             action = menu.addAction("取消勾選選中項目")
             action.triggered.connect(lambda: self.check_selected_items(False, view_rows)) # 傳遞 view_rows
        else:
            # 如果全都未勾選，顯示「勾選選中項目」
            action = menu.addAction("勾選選中項目")
            action.triggered.connect(lambda: self.check_selected_items(True, view_rows)) # 傳遞 view_rows

        menu.exec_(self.table_view.viewport().mapToGlobal(pos))

    def check_selected_items(self, check=True, view_rows=None):
        """勾選或取消勾選指定的視圖行項目"""
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
                self.export_button.setToolTip(f"已選擇 {checked_count} 個字段")
            else:
                self.export_button.setEnabled(False)
                self.export_button.setToolTip("請先勾選要匯入的字段")

    def export_checked_fields(self):
        """匯出已勾選的字段到策略生成器"""
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
            QMessageBox.warning(self, "警告", "未勾選任何字段")

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


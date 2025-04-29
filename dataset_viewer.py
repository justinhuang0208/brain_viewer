import sys
import os
import pandas as pd
from PySide6.QtWidgets import (QApplication, QMainWindow, QTableView, QTreeView,
                              QSplitter, QVBoxLayout, QHBoxLayout, QWidget,
                              QLineEdit, QLabel, QComboBox, QFileSystemModel,
                              QHeaderView, QPushButton, QStatusBar, QTabWidget,
                              QMessageBox, QDialog, QTextEdit, QVBoxLayout, QFrame)
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

# 自定義表格模型
class DataFrameModel(QAbstractTableModel):
    def __init__(self, data):
        super(DataFrameModel, self).__init__()
        self._data = data

    def rowCount(self, parent=None):
        return self._data.shape[0]

    def columnCount(self, parent=None):
        return self._data.shape[1]

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None

        value = self._data.iloc[index.row(), index.column()]
        column_name = self._data.columns[index.column()]

        if role == Qt.DisplayRole:
            return str(value)

        # 添加排序角色處理 (使用 EditRole 替代 SortRole for Qt6)
        elif role == Qt.EditRole:
            row = index.row() # 獲取行號用於調試
            if column_name == 'Coverage':
                try:
                    # 移除 '%' 並轉換為浮點數
                    return float(str(value).replace('%', ''))
                except ValueError:
                    print(f"警告: 無法將 Coverage 列第 {row} 行的值 '{value}' 轉換為浮點數。排序時將使用 0。")
                    return 0 # 轉換失敗時返回 0
            elif column_name in ['Users', 'Alphas']:
                try:
                    # 轉換為整數
                    return int(value)
                except (ValueError, TypeError):
                    print(f"警告: 無法將 {column_name} 列第 {row} 行的值 '{value}' 轉換為整數。排序時將使用 0。")
                    return 0 # 轉換失敗時返回 0
            # 對於其他欄位，嘗試轉換為數值，否則返回原始值
            try:
                # 嘗試將其他欄位也轉換為數值進行排序
                return float(value)
            except (ValueError, TypeError):
                # 如果無法轉換為數值，則按字符串排序
                return str(value)

        # 添加工具提示角色
        elif role == Qt.ToolTipRole:
            # 為所有單元格添加工具提示，特別是Description列
            if column_name == 'Description' and len(str(value)) > 20:
                return str(value)
            return str(value)

        elif role == Qt.BackgroundRole:
            # 根據覆蓋率添加顏色
            if column_name == 'Coverage':
                try:
                    # 提取百分比數值
                    coverage = int(str(value).replace('%', ''))
                    if coverage > 90:
                        return QColor("#c8e6c9")  # 淺綠色，高覆蓋率
                    elif coverage > 70:
                        return QColor("#fff9c4")  # 淺黃色，中等覆蓋率
                    else:
                        return QColor("#ffccbc")  # 淺紅色，低覆蓋率
                except:
                    pass

            # 根據Users和Alphas列來著色
            if column_name in ['Users', 'Alphas']:
                try:
                    value_int = int(value)
                    if value_int > 500:
                        return QColor("#e3f2fd")  # 淺藍色，高使用量
                except:
                    pass
                
        return None

    def headerData(self, section, orientation, role):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                return str(self._data.columns[section])
            if orientation == Qt.Vertical:
                return str(self._data.index[section])
        return None

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
        top_right.addWidget(field_label)
        top_right.addWidget(self.filter_column)
        top_right.addWidget(refresh_button)
        
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

    def load_dataset(self, index):
        # 保存最後載入的索引，用於刷新功能
        self.last_loaded_index = index
        
        file_path = self.file_model.filePath(index)
        try:
            # 讀取CSV數據
            df = pd.read_csv(file_path)
            
            # 處理數值欄位
            if 'Coverage' in df.columns:
                # 確保Coverage是字符串並包含%
                df['Coverage'] = df['Coverage'].astype(str)
                df['Coverage'] = df['Coverage'].apply(lambda x: x if '%' in x else f"{x}%")
                
            # 設置數據模型
            model = DataFrameModel(df)
            
            # 設置代理模型用於過濾
            self.proxy_model = QSortFilterProxyModel()
            self.proxy_model.setSourceModel(model)
            self.proxy_model.setFilterCaseSensitivity(Qt.CaseInsensitive)

            self.table_view.setModel(self.proxy_model)

            # --- 開始修改欄位寬度 ---
            header = self.table_view.horizontalHeader()
            columns = list(df.columns)
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
                    # 其他欄位保持自動拉伸
                    header.setSectionResizeMode(i, QHeaderView.Stretch)
            # --- 結束修改欄位寬度 ---

            # 連接表格點擊事件 - 先斷開之前的連接
            if self.click_connected:
                try:
                    self.table_view.clicked.disconnect()
                except TypeError:
                    # 如果還沒有連接，忽略錯誤
                    pass
                    
            self.table_view.clicked.connect(self.handle_cell_click)
            self.click_connected = True
            
            # 更新當前數據集和狀態欄
            self.current_dataset = df
            file_name = os.path.basename(file_path)
            self.status_bar.showMessage(f"已載入 {file_name} | 共 {len(df)} 條記錄")
            
            # 更新當前檔案標籤
            self.current_file_label.setText(file_name)
            
            # 添加欄位到過濾下拉框
            self.filter_column.clear()
            self.filter_column.addItem("全部欄位")
            for col in df.columns:
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
                column_index = list(self.current_dataset.columns).index(filter_column)
                self.proxy_model.setFilterKeyColumn(column_index)
            except ValueError:
                # 如果找不到列，默認使用第一列
                self.proxy_model.setFilterKeyColumn(0)
        
        # 設置過濾文本
        self.proxy_model.setFilterFixedString(filter_text)
        
        # 更新狀態欄以顯示過濾後的記錄數
        filtered_count = self.proxy_model.rowCount()
        total_count = len(self.current_dataset)
        self.status_bar.showMessage(f"顯示 {filtered_count}/{total_count} 條記錄 | 過濾條件: {filter_column} - '{filter_text}'")
        
    def update_chart(self):
        if self.current_dataset is None:
            return
            
        # 清除當前圖表
        self.canvas.figure.clear()
        ax = self.canvas.figure.add_subplot(111)
        
        chart_type = self.chart_type.currentText()
        df = self.current_dataset
        
        # 增加字體大小使圖表更清晰
        plt.rcParams.update({'font.size': 12})
        
        if chart_type == "覆蓋率分佈":
            # 提取覆蓋率數值
            coverage = df['Coverage'].str.replace('%', '').astype(float)
            
            # 創建直方圖
            ax.hist(coverage, bins=10, color='skyblue', edgecolor='black')
            ax.set_title('覆蓋率分佈')
            ax.set_xlabel('覆蓋率 (%)')
            ax.set_ylabel('變數數量')
            
        elif chart_type == "使用者數量":
            # 按使用者數量排序取前15個
            top_users = df.sort_values('Users', ascending=False).head(15)
            
            # 繪製條形圖
            bars = ax.barh(top_users['Field'], top_users['Users'], color='lightgreen')
            ax.set_title('使用者數量最多的前15個變數')
            ax.set_xlabel('使用者數量')
            ax.set_ylabel('變數名稱')
            ax.invert_yaxis()  # 反轉Y軸使最大值在頂部
            
            # 為條形圖添加數值標籤
            for bar in bars:
                width = bar.get_width()
                label_x_pos = width
                ax.text(label_x_pos, bar.get_y() + bar.get_height()/2, f' {int(width)}',
                       va='center')
            
        elif chart_type == "Alpha數量":
            # 按Alpha數量排序取前15個
            top_alphas = df.sort_values('Alphas', ascending=False).head(15)
            
            # 繪製條形圖
            bars = ax.barh(top_alphas['Field'], top_alphas['Alphas'], color='coral')
            ax.set_title('Alpha數量最多的前15個變數')
            ax.set_xlabel('Alpha數量')
            ax.set_ylabel('變數名稱')
            ax.invert_yaxis()  # 反轉Y軸使最大值在頂部
            
            # 為條形圖添加數值標籤
            for bar in bars:
                width = bar.get_width()
                label_x_pos = width
                ax.text(label_x_pos, bar.get_y() + bar.get_height()/2, f' {int(width)}',
                       va='center')
            
        elif chart_type == "類型分佈":
            # 計算每種類型的數量
            type_counts = df['Type'].value_counts()
            
            # 繪製餅圖 - 移除陰影效果並調整圖表參數
            wedges, texts, autotexts = ax.pie(type_counts, labels=type_counts.index, autopct='%1.1f%%', 
                  shadow=False, startangle=90, colors=plt.cm.Paired.colors, 
                  wedgeprops={'edgecolor': 'white', 'linewidth': 1})
            ax.set_title('變數類型分佈')
            ax.axis('equal')  # 確保餅圖是圓形的
            
            # 增加餅圖標籤的可讀性
            for text in texts + autotexts:
                text.set_fontsize(11)
            
        # 調整圖表佈局以確保所有元素都顯示
        self.canvas.figure.tight_layout()
        self.canvas.draw()

    # 處理單元格點擊事件
    def handle_cell_click(self, index):
        # 確保當前有數據集和模型
        if self.current_dataset is None or self.proxy_model is None:
            self.status_bar.showMessage("無法處理點擊：數據集或模型不存在")
            return
            
        # 獲取列名
        column_index = index.column()
        if column_index >= len(self.current_dataset.columns):
            self.status_bar.showMessage(f"無法處理點擊：列索引 {column_index} 超出範圍")
            return
            
        column_name = self.current_dataset.columns[column_index]
        
        # 獲取數據
        try:
            # 由於使用了代理模型，需要先映射到源模型的索引
            source_index = self.proxy_model.mapToSource(index)
            source_row = source_index.row()
            
            if source_row >= len(self.current_dataset):
                self.status_bar.showMessage(f"無法處理點擊：行索引 {source_row} 超出範圍")
                return
                
            value = self.current_dataset.iloc[source_row, column_index]
            
            # 調試信息
            self.status_bar.showMessage(f"點擊了: 列={column_name}, 行={source_row}, 值長度={len(str(value))}")
            
            # 如果是描述欄位或包含"description"的欄位（不區分大小寫），顯示詳細信息對話框
            if column_name.lower() == 'description' or 'description' in column_name.lower():
                try:
                    field_name = "未知字段"
                    field_index = -1
                    
                    # 嘗試查找"Field"列，不區分大小寫
                    for i, col in enumerate(self.current_dataset.columns):
                        if col.lower() == 'field':
                            field_index = i
                            break
                    
                    if field_index >= 0:
                        field_name = self.current_dataset.iloc[source_row, field_index]
                    
                    # 使用防重複邏輯：檢查是否已經有相同的對話框打開
                    for child in self.children():
                        if isinstance(child, DetailDialog) and child.isVisible():
                            child.close()  # 關閉之前的對話框
                            break
                            
                    dialog = DetailDialog(f"{field_name} - 詳細描述", str(value), self)
                    dialog.exec()
                except Exception as e:
                    self.status_bar.showMessage(f"顯示詳細信息時出錯: {str(e)}")
            # 對其他任何欄位，只要內容超過特定長度，也顯示詳細信息
            elif len(str(value)) > 15:
                try:
                    # 使用欄位名作為標題
                    dialog = DetailDialog(f"{column_name} - 詳細內容", str(value), self)
                    dialog.exec()
                except Exception as e:
                    self.status_bar.showMessage(f"顯示詳細信息時出錯: {str(e)}")
        except Exception as e:
            self.status_bar.showMessage(f"處理單元格點擊時出錯: {str(e)}")

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

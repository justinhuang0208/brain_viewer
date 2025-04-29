#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
WorldQuant Brain 策略生成器
GUI工具，用於設計策略模板並生成策略文件
"""

import sys
import os
import csv
import json
import pandas as pd
import datetime
from PySide6.QtWidgets import (QApplication, QMainWindow, QVBoxLayout, QHBoxLayout, 
                             QWidget, QLabel, QComboBox, QLineEdit, QTextEdit,
                             QPushButton, QFileDialog, QMessageBox, QTabWidget,
                             QGroupBox, QFormLayout, QCheckBox, QSpinBox, 
                             QDoubleSpinBox, QListWidget, QListWidgetItem,
                              QSplitter, QFrame, QTableWidget, QTableWidgetItem,
                              QHeaderView, QAbstractItemView, QInputDialog)
from PySide6.QtCore import Qt, QDir, Signal, Slot # Keep Signal and Slot
from PySide6.QtGui import QFont, QColor

import re
from PySide6.QtGui import QSyntaxHighlighter, QTextCharFormat, QColor, QFont
# 獲取腳本所在目錄的絕對路徑
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# 定義常量 - 使用絕對路徑
DATASETS_DIR = os.path.join(SCRIPT_DIR, "datasets")
TEMPLATES_DIR = os.path.join(SCRIPT_DIR, "templates")
ALPHAS_DIR = os.path.join(SCRIPT_DIR, "alphas")

class FieldSelector(QWidget):
    """用於選擇數據字段的小部件"""
    
    field_selected = Signal(str)  # 字段選擇信號
    
    def __init__(self, parent=None):
        super(FieldSelector, self).__init__(parent)
        
        self.csv_file_path = None  # 當前選擇的CSV文件路徑
        self.field_data = {}       # 用於存儲字段信息
        
        # 確保目錄存在
        if not os.path.exists(DATASETS_DIR):
            os.makedirs(DATASETS_DIR)
        if not os.path.exists(TEMPLATES_DIR):
            os.makedirs(TEMPLATES_DIR)
        if not os.path.exists(ALPHAS_DIR):
            os.makedirs(ALPHAS_DIR)
            
        self.init_ui()
        self.load_dataset_list()
        
    def init_ui(self):
        layout = QVBoxLayout(self)
        
        # CSV文件選擇區域
        file_group = QGroupBox("數據集選擇")
        file_layout = QVBoxLayout()
        
        # 數據集下拉選擇框
        file_select_layout = QHBoxLayout()
        file_select_layout.addWidget(QLabel("選擇數據集:"))
        self.dataset_combo = QComboBox()
        self.dataset_combo.currentIndexChanged.connect(self.on_dataset_selected)
        file_select_layout.addWidget(self.dataset_combo, 1)
        
        self.refresh_btn = QPushButton("刷新列表")
        self.refresh_btn.clicked.connect(self.load_dataset_list)
        file_select_layout.addWidget(self.refresh_btn)
        
        file_layout.addLayout(file_select_layout)
        file_group.setLayout(file_layout)
        
        # 添加字段選擇區域
        field_group = QGroupBox("選擇的字段")
        field_layout = QVBoxLayout()
        
        # 字段類型選擇
        type_layout = QHBoxLayout()
        type_layout.addWidget(QLabel("字段類型:"))
        self.type_combo = QComboBox()
        self.type_combo.addItems(["矩陣(MATRIX)", "向量(VECTOR)", "全部"])
        self.type_combo.currentIndexChanged.connect(self.filter_fields_by_type)
        type_layout.addWidget(self.type_combo)
        
        # 搜索框
        search_layout = QHBoxLayout()
        search_layout.addWidget(QLabel("搜索:"))
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("輸入字段名或描述...")
        self.search_input.textChanged.connect(self.filter_fields)
        search_layout.addWidget(self.search_input)
        
        # 字段列表
        self.field_list = QListWidget()
        self.field_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.field_list.itemDoubleClicked.connect(self.on_field_double_click)
        
        # 字段列表操作按鈕
        field_list_buttons = QHBoxLayout()
        self.select_all_btn = QPushButton("全選/取消全選")
        self.select_all_btn.clicked.connect(self.toggle_select_all)
        field_list_buttons.addWidget(self.select_all_btn)
        
        self.add_all_btn = QPushButton("加入全部可見字段")
        self.add_all_btn.clicked.connect(self.add_all_visible_fields)
        field_list_buttons.addWidget(self.add_all_btn)
        
        # 已選字段列表
        self.selected_group = QGroupBox("已選字段")
        selected_layout = QVBoxLayout()
        self.selected_field_list = QListWidget()
        self.selected_field_list.itemDoubleClicked.connect(self.remove_selected_field)
        
        selected_buttons = QHBoxLayout()
        self.add_field_btn = QPushButton("添加 >>>")
        self.add_field_btn.clicked.connect(self.add_selected_fields)
        selected_buttons.addWidget(self.add_field_btn)
        
        self.remove_field_btn = QPushButton("<<< 移除")
        self.remove_field_btn.clicked.connect(self.remove_selected_fields)
        selected_buttons.addWidget(self.remove_field_btn)
        
        self.clear_field_btn = QPushButton("清空")
        self.clear_field_btn.clicked.connect(self.clear_selected_fields)
        selected_buttons.addWidget(self.clear_field_btn)
        
        selected_layout.addWidget(self.selected_field_list)
        selected_layout.addLayout(selected_buttons)
        self.selected_group.setLayout(selected_layout)
        
        field_layout.addLayout(type_layout)
        field_layout.addLayout(search_layout)
        field_layout.addWidget(QLabel("可用字段:"))
        field_layout.addLayout(field_list_buttons)  # 添加按鈕行
        field_layout.addWidget(self.field_list, 2)
        field_layout.addWidget(self.selected_group, 1)
        field_group.setLayout(field_layout)
        
        # 布局添加
        layout.addWidget(file_group)
        layout.addWidget(field_group, 1)
    
    def load_dataset_list(self):
        """載入datasets目錄中的CSV文件列表"""
        self.dataset_combo.clear()
        
        if not os.path.exists(DATASETS_DIR):
            os.makedirs(DATASETS_DIR)
        
        csv_files = []
        for file in os.listdir(DATASETS_DIR):
            if file.endswith("_fields_formatted.csv"):
                csv_files.append(file)
        
        # 按字母順序排序
        csv_files.sort()
        
        # 添加到下拉框
        for file in csv_files:
            self.dataset_combo.addItem(file)
            
        # 如果有文件，選擇第一個
        if self.dataset_combo.count() > 0:
            self.dataset_combo.setCurrentIndex(0)
        
    def on_dataset_selected(self, index):
        """當選擇數據集時觸發"""
        if index >= 0:
            file_name = self.dataset_combo.currentText()
            self.csv_file_path = os.path.join(DATASETS_DIR, file_name)
            self.load_fields_from_csv()
        
    def select_csv_file(self):
        """選擇CSV文件"""
        file_dialog = QFileDialog()
        file_path, _ = file_dialog.getOpenFileName(
            self, "選擇CSV文件", "", "CSV文件 (*.csv)"
        )
        
        if file_path:
            self.csv_file_path = file_path
            self.file_path_label.setText(file_path)
            self.load_fields_from_csv()
            
    def load_fields_from_csv(self):
        """從CSV文件加載字段"""
        self.field_list.clear()
        self.field_data = {}
        
        if not self.csv_file_path or not os.path.exists(self.csv_file_path):
            return
            
        try:
            df = pd.read_csv(self.csv_file_path)
            
            # 檢查必要列是否存在
            if 'Field' not in df.columns or 'Type' not in df.columns:
                QMessageBox.warning(self, "無效的CSV格式", 
                                  "CSV文件必須包含'Field'和'Type'列")
                return
                
            # 處理每一行
            for _, row in df.iterrows():
                field = row.get('Field', '')
                field_type = str(row.get('Type', '')).upper()
                
                # 只收集矩陣和向量類型字段
                if field and field_type in ['MATRIX', 'VECTOR']:
                    self.field_data[field] = {
                        'type': field_type,
                        'description': row.get('Description', ''),
                        'coverage': row.get('Coverage', '')
                    }
                    
            # 更新字段列表
            self.filter_fields_by_type()
            
        except Exception as e:
            QMessageBox.critical(self, "錯誤", f"讀取CSV文件時出錯: {str(e)}")
            
    def filter_fields_by_type(self):
        """根據所選類型過濾字段"""
        self.field_list.clear()
        type_index = self.type_combo.currentIndex()
        
        # 設置列表項的文本格式
        self.field_list.setFont(QFont("Consolas", 9))
        
        for field, data in self.field_data.items():
            field_type = data.get('type', '')
            
            # 根據選擇的類型過濾
            if (type_index == 0 and field_type == 'MATRIX') or \
               (type_index == 1 and field_type == 'VECTOR') or \
               (type_index == 2):  # 全部
                # 獲取覆蓋率信息
                coverage = data.get('coverage', '')
                coverage_str = str(coverage).strip()
                
                # 準備右側顯示的信息
                right_part = ""
                if coverage_str:
                    right_part = coverage_str
                if field_type:
                    type_short = "M" if field_type == "MATRIX" else "V"
                    if right_part:
                        right_part = f"{right_part} {type_short}"
                    else:
                        right_part = type_short
                
                # 創建顯示文本，使字段名靠左，覆蓋率和類型完全靠右
                total_width = 54  # 總顯示寬度
                # 先決定右側部分的長度
                right_width = len(right_part)
                # 計算應該填充多少空格
                field_max_width = total_width - right_width - 1  # 減1為中間空格
                
                # 如果字段名過長，則截斷
                if len(field) > field_max_width:
                    display_field = field[:field_max_width-3] + "..."
                else:
                    display_field = field
                
                # 創建顯示文本，使用格式化確保右對齊
                padding = total_width - len(display_field) - right_width
                display_text = f"{display_field}{' ' * padding}{right_part}"
                    
                item = QListWidgetItem(display_text)
                
                # 設置詳細提示信息
                description = data.get('description', '')
                tooltip = f"字段：{field}\n"
                tooltip += f"類型：{field_type}\n"
                tooltip += f"覆蓋率：{coverage_str}\n"
                if description:
                    tooltip += f"描述：{description}"
                item.setToolTip(tooltip)
                
                # 設置項目數據 - 用於後續處理
                item.setData(Qt.UserRole, field)  # 保存原始字段名
                
                # 設置顏色 - 根據覆蓋率
                try:
                    cov_value = float(str(coverage).replace('%', ''))
                    if cov_value > 75:
                        item.setBackground(QColor("#e8f5e9"))  # 淺綠色
                    elif cov_value > 50:
                        item.setBackground(QColor("#fff9c4"))  # 淺黃色
                    else:
                        item.setBackground(QColor("#ffebee"))  # 淺紅色
                except:
                    pass
                    
                self.field_list.addItem(item)
                
        # 應用搜索過濾
        self.filter_fields()
                
    def filter_fields(self):
        """根據搜索文本過濾字段"""
        search_text = self.search_input.text().lower()
        
        for i in range(self.field_list.count()):
            item = self.field_list.item(i)
            field = item.text().lower()
            desc = item.toolTip().lower()
            
            if search_text in field or search_text in desc:
                item.setHidden(False)
            else:
                item.setHidden(True)
                
    def on_field_double_click(self, item):
        """字段雙擊事件"""
        # 從項目數據中獲取原始字段名
        field_name = item.data(Qt.UserRole)
        self.add_field_to_selected(field_name)
        
    def add_selected_fields(self):
        """添加選中的字段到已選列表"""
        for item in self.field_list.selectedItems():
            field_name = item.data(Qt.UserRole)  # 使用原始字段名
            self.add_field_to_selected(field_name)
            
    def add_field_to_selected(self, field_name):
        """添加字段到已選列表"""
        # 檢查是否已經在已選列表中
        for i in range(self.selected_field_list.count()):
            item_data = self.selected_field_list.item(i).data(Qt.UserRole)
            if item_data == field_name:
                return
                
        # 創建新的列表項
        field_data = self.field_data.get(field_name, {})
        field_type = field_data.get('type', '')
        coverage = field_data.get('coverage', '')
        
        # 準備右側顯示的信息
        right_part = ""
        if coverage:
            right_part = coverage
        if field_type:
            type_short = "M" if field_type == "MATRIX" else "V"
            if right_part:
                right_part = f"{right_part} {type_short}"
            else:
                right_part = type_short
        
        # 創建顯示文本，使字段名靠左，覆蓋率和類型完全靠右
        total_width = 50  # 總顯示寬度
        # 先決定右側部分的長度
        right_width = len(right_part)
        # 計算應該填充多少空格
        field_max_width = total_width - right_width - 1  # 減1為中間空格
        
        # 如果字段名過長，則截斷
        if len(field_name) > field_max_width:
            display_field = field_name[:field_max_width-3] + "..."
        else:
            display_field = field_name
        
        # 創建顯示文本，使用格式化確保右對齊
        padding = total_width - len(display_field) - right_width
        display_text = f"{display_field}{' ' * padding}{right_part}"
            
        # 添加到已選列表
        item = QListWidgetItem(display_text)
        item.setData(Qt.UserRole, field_name)  # 保存原始字段名
        self.selected_field_list.addItem(item)
        
    def remove_selected_field(self, item):
        """從已選列表移除字段"""
        row = self.selected_field_list.row(item)
        self.selected_field_list.takeItem(row)
        
    def remove_selected_fields(self):
        """移除選中的已選字段"""
        selected_items = self.selected_field_list.selectedItems()
        for item in selected_items:
            row = self.selected_field_list.row(item)
            self.selected_field_list.takeItem(row)
            
    def clear_selected_fields(self):
        """清空已選字段列表"""
        self.selected_field_list.clear()
        
    def get_selected_fields(self):
        """獲取所有已選字段"""
        selected_fields = []
        for i in range(self.selected_field_list.count()):
            # 從項目數據中獲取原始字段名
            field_name = self.selected_field_list.item(i).data(Qt.UserRole)
            selected_fields.append(field_name)
        return selected_fields
        
    def toggle_select_all(self):
        """全選或取消全選字段列表中的所有可見項目"""
        all_selected = True
        
        # 檢查是否所有可見項目都已選中
        for i in range(self.field_list.count()):
            item = self.field_list.item(i)
            if not item.isHidden() and not item.isSelected():
                all_selected = False
                break
                
        # 切換選擇狀態
        for i in range(self.field_list.count()):
            item = self.field_list.item(i)
            if not item.isHidden():
                item.setSelected(not all_selected)
                
    def add_all_visible_fields(self):
        """添加所有可見字段到已選列表"""
        for i in range(self.field_list.count()):
            item = self.field_list.item(i)
            if not item.isHidden():
                field_name = item.data(Qt.UserRole)
                self.add_field_to_selected(field_name)

    # --- New Slot to receive codes from backtest_viewer ---
    @Slot(list)
    def add_fields_from_list(self, field_names):
        """從列表添加多個字段到已選列表 (Slot for external signals)"""
        if not isinstance(field_names, list):
            print(f"錯誤: add_fields_from_list 預期接收列表，但收到 {type(field_names)}")
            QMessageBox.warning(self, "匯入錯誤", f"預期接收列表，但收到 {type(field_names)}")
            return

        added_count = 0
        skipped_count = 0
        for field_name in field_names:
            if isinstance(field_name, str) and field_name.strip():
                clean_field_name = field_name.strip()
                # 檢查是否已存在於已選列表
                already_exists = False
                for i in range(self.selected_field_list.count()):
                    item_data = self.selected_field_list.item(i).data(Qt.UserRole)
                    if item_data == clean_field_name:
                        already_exists = True
                        break

                if not already_exists:
                    # 嘗試使用 add_field_to_selected (如果字段存在於當前加載的數據集)
                    # 否則，直接添加純文本
                    if clean_field_name in self.field_data:
                        self.add_field_to_selected(clean_field_name)
                        added_count += 1
                    else:
                        # 如果 Code 不在當前 FieldSelector 加載的數據集字段中，
                        # 仍然將其作為純文本添加到已選列表
                        item = QListWidgetItem(clean_field_name)
                        item.setData(Qt.UserRole, clean_field_name) # 保存原始名稱
                        # 可以考慮設置不同的背景色或提示
                        # item.setBackground(QColor("#f0f0f0")) # 例如灰色背景
                        item.setToolTip(f"匯入的 Code: {clean_field_name}\n(不在當前數據集字段列表中)")
                        self.selected_field_list.addItem(item)
                        added_count += 1
                else:
                    skipped_count += 1 # 如果已存在則跳過
            else:
                print(f"跳過無效的匯入項: {field_name}")
                skipped_count += 1

        status_message = f"匯入完成：成功添加 {added_count} 個 Code"
        if skipped_count > 0:
            status_message += f"，跳過 {skipped_count} 個重複或無效項。"
        print(status_message)
        # 可以考慮更新狀態欄或顯示一個短暫的消息框
class AlphaSyntaxHighlighter(QSyntaxHighlighter):
    """用於 Alpha 模板編輯器的語法高亮器"""

    def __init__(self, parent=None):
        super(AlphaSyntaxHighlighter, self).__init__(parent)

        self.highlighting_rules = []

        # 運算符/關鍵字格式 (藍色)
        keyword_format = QTextCharFormat()
        keyword_format.setForeground(QColor("blue"))
        keyword_format.setFontWeight(QFont.Bold)
        # 使用者提供的運算符列表 + 常用符號
        keywords = [
            "abs", "add", "densify", "divide", "inverse", "log", "max", "min",
            "multiply", "power", "reverse", "sign", "signed_power", "sqrt",
            "subtract", "and", "if_else", "is_nan", "not", "or",
            "days_from_last_change", "hum", "kth_element", "last_diff_value",
            "ts_argmax", "ts_argmin", "ts_av_diff", "ts_backfill", "ts_corr",
            "ts_count_nans", "ts_covariance", "ts_decay_linear", "ts_delay",
            "ts_delta", "ts_mean", "ts_product", "ts_quantile", "ts_rank",
            "ts_regression", "ts_scale", "ts_std_dev", "ts_step", "ts_sum",
            "ts_zscore", "normalize", "quantile", "rank", "scale", "winsorize",
            "zscore", "vec_avg", "vec_sum", "bucket", "trade_when",
            "group_backfill", "group_mean", "group_neutralize", "group_rank",
            "group_scale", "group_zscore",
            r'\+', r'\-', r'\*', r'\/', r'<', r'<=', r'==', r'>', r'>=', r'!='
        ]
        # 創建關鍵字的正則表達式，使用 \b 來匹配單詞邊界，除非是符號
        keyword_patterns = [r'\b' + keyword + r'\b' for keyword in keywords if keyword.isalnum()]
        symbol_patterns = [keyword for keyword in keywords if not keyword.isalnum()] # 處理符號
        # 將符號的正則表達式轉義，以防它們包含特殊字符
        escaped_symbol_patterns = [re.escape(p) for p in symbol_patterns]
        self.highlighting_rules.append((re.compile("|".join(keyword_patterns + escaped_symbol_patterns)), keyword_format))


        # 數字格式 (紅色)
        number_format = QTextCharFormat()
        number_format.setForeground(QColor("red"))
        # 匹配整數和浮點數 (包括科學記數法)
        self.highlighting_rules.append((re.compile(r'\b[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?\b'), number_format))

        # 括號/方括號/花括號格式 (預設顏色，加粗)
        bracket_format = QTextCharFormat()
        # bracket_format.setForeground(QColor("darkGray")) # 可以自訂顏色
        bracket_format.setFontWeight(QFont.Bold)
        self.highlighting_rules.append((re.compile(r'[()\[\]{}]'), bracket_format)) # 注意：這裡不包括花括號，因為下面有特殊處理

        # 佔位符 {field} 格式 (綠色)
        placeholder_format = QTextCharFormat()
        placeholder_format.setForeground(QColor("green"))
        # 精確匹配 {field}
        self.highlighting_rules.append((re.compile(r'\{field\}'), placeholder_format))

    def highlightBlock(self, text):
        """對文本塊應用高亮規則"""
        # 按優先級應用規則（例如，先匹配佔位符，再匹配關鍵字）
        # 這裡的順序很重要，因為後面的規則可能會覆蓋前面的規則
        # 1. 佔位符
        placeholder_pattern, placeholder_format = self.highlighting_rules[3]
        for match in placeholder_pattern.finditer(text):
            start, end = match.span()
            self.setFormat(start, end - start, placeholder_format)

        # 2. 關鍵字/運算符
        keyword_pattern, keyword_format = self.highlighting_rules[0]
        for match in keyword_pattern.finditer(text):
            start, end = match.span()
            # 檢查當前範圍是否已被佔位符高亮，如果是則跳過
            if self.format(start).foreground() != placeholder_format.foreground():
                 self.setFormat(start, end - start, keyword_format)

        # 3. 數字
        number_pattern, number_format = self.highlighting_rules[1]
        for match in number_pattern.finditer(text):
            start, end = match.span()
            # 檢查是否已被其他規則高亮
            current_format = self.format(start)
            if current_format.foreground() != placeholder_format.foreground() and \
               current_format.foreground() != keyword_format.foreground():
                self.setFormat(start, end - start, number_format)

        # 4. 括號/方括號 (不包括花括號)
        bracket_pattern, bracket_format = self.highlighting_rules[2]
        for match in bracket_pattern.finditer(text):
            start, end = match.span()
             # 檢查是否已被其他規則高亮
            current_format = self.format(start)
            if current_format.foreground() != placeholder_format.foreground() and \
               current_format.foreground() != keyword_format.foreground() and \
               current_format.foreground() != number_format.foreground():
                self.setFormat(start, end - start, bracket_format)


        # 可選：處理多行註釋或字符串等跨塊語法，此處暫不處理
        self.setCurrentBlockState(0)
        # QMessageBox.information(self, "匯入結果", status_message)

class CodeTemplateEditor(QWidget):
    """代碼模板編輯器"""
    
    def __init__(self, parent=None):
        super(CodeTemplateEditor, self).__init__(parent)
        
        # 確保模板目錄存在
        if not os.path.exists(TEMPLATES_DIR):
            os.makedirs(TEMPLATES_DIR)
            
        self.init_ui()
        self.load_templates()
        
    def init_ui(self):
        layout = QVBoxLayout(self)
        
        # 模板選擇和管理
        template_header = QHBoxLayout()
        
        template_header.addWidget(QLabel("模板:"))
        self.template_combo = QComboBox()
        self.template_combo.currentIndexChanged.connect(self.load_selected_template)
        template_header.addWidget(self.template_combo, 1)
        
        self.save_btn = QPushButton("保存")
        self.save_btn.clicked.connect(self.save_template)
        template_header.addWidget(self.save_btn)
        
        self.new_btn = QPushButton("新建")
        self.new_btn.clicked.connect(self.new_template)
        template_header.addWidget(self.new_btn)
        
        self.delete_btn = QPushButton("刪除")
        self.delete_btn.clicked.connect(self.delete_template)
        template_header.addWidget(self.delete_btn)
        
        # 模板編輯區域
        self.editor = QTextEdit()
# 應用語法高亮
        self.highlighter = AlphaSyntaxHighlighter(self.editor.document())
        self.editor.setFont(QFont("Consolas", 14))
        self.editor.setPlaceholderText("在此輸入代碼模板，使用{field}作為字段佔位符...")
        
        # 模板描述
        desc_layout = QHBoxLayout()
        desc_layout.addWidget(QLabel("描述:"))
        self.description_edit = QLineEdit()
        self.description_edit.setPlaceholderText("(可選) 描述此模板的用途")
        desc_layout.addWidget(self.description_edit)
        
        # 說明文本
        help_text = """
        <b>模板使用說明:</b>
        <ul>
          <li>使用 <code>{field}</code> 作為字段名稱佔位符</li>
          <li>支持 ts_rank, rank, zscore 等所有算子</li>
          <li>模板將保存到 templates/ 目錄中</li>
        </ul>
        """
        help_label = QLabel(help_text)
        help_label.setTextFormat(Qt.RichText)
        
        layout.addLayout(template_header)
        layout.addLayout(desc_layout)
        layout.addWidget(self.editor, 1)
        layout.addWidget(help_label)
        
    def load_templates(self):
        """載入預設和保存的模板"""
        self.template_combo.clear()
        
        # 預設模板
        default_templates = {
            "基本 ts_rank": {
                "code": "ts_rank({field}, 126)",
                "description": "計算126天的時序排名"
            },
            "基本 rank": {
                "code": "rank({field})",
                "description": "橫截面排名"
            },
            "基本 zscore": {
                "code": "zscore({field})",
                "description": "標準化得分"
            },
            "同行業比較": {
                "code": "group_rank({field}, industry)",
                "description": "行業內排名"
            },
            "同子行業比較": {
                "code": "group_rank({field}, subindustry)",
                "description": "子行業內排名"
            },
            "複雜動量": {
                "code": "ts_rank(ts_delta({field}, 5) / ts_delay({field}, 5), 21)",
                "description": "5天變化率的21天排名"
            }
        }
        
        # 讀取保存的模板
        custom_templates = {}
        try:
            if os.path.exists(TEMPLATES_DIR):
                for file_name in os.listdir(TEMPLATES_DIR):
                    if file_name.endswith('.json'):
                        file_path = os.path.join(TEMPLATES_DIR, file_name)
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                template_data = json.load(f)
                                if 'name' in template_data and 'code' in template_data:
                                    custom_templates[template_data['name']] = {
                                        'code': template_data['code'],
                                        'description': template_data.get('description', ''),
                                        'file_path': file_path
                                    }
                        except Exception as e:
                            print(f"讀取模板文件 {file_path} 時出錯: {str(e)}")
        except Exception as e:
            print(f"掃描模板目錄時出錯: {str(e)}")
        
        # 先添加預設模板
        for name, data in default_templates.items():
            self.template_combo.addItem(f"[預設] {name}", data)
            
        # 再添加自定義模板
        for name, data in custom_templates.items():
            self.template_combo.addItem(name, data)
            
        # 選擇第一個模板
        if self.template_combo.count() > 0:
            self.template_combo.setCurrentIndex(0)
            
    def load_selected_template(self):
        """載入選擇的模板到編輯器"""
        index = self.template_combo.currentIndex()
        if index >= 0:
            template_data = self.template_combo.itemData(index)
            self.editor.setText(template_data.get('code', ''))
            self.description_edit.setText(template_data.get('description', ''))
            
    def save_template(self):
        """保存當前模板"""
        code = self.editor.toPlainText().strip()
        description = self.description_edit.text().strip()
        
        if not code:
            QMessageBox.warning(self, "錯誤", "模板不能為空")
            return
            
        # 檢查是否是編輯預設模板
        current_index = self.template_combo.currentIndex()
        current_text = self.template_combo.itemText(current_index)
        
        if current_text.startswith("[預設]"):
            # 建立新的自定義模板而不是修改預設的
            name, ok = QInputDialog.getText(self, "保存模板", "請輸入模板名稱:")
            if not ok or not name:
                return
                
            self._save_template_to_file(name, code, description)
            
            # 重新加載模板列表
            self.load_templates()
            
            # 選擇新建的模板
            for i in range(self.template_combo.count()):
                if self.template_combo.itemText(i) == name:
                    self.template_combo.setCurrentIndex(i)
                    break
        else:
            # 更新現有自定義模板
            template_data = self.template_combo.itemData(current_index)
            file_path = template_data.get('file_path')
            
            if file_path:
                self._update_template_file(file_path, current_text, code, description)
                
                # 更新下拉框中的數據
                updated_data = template_data.copy()
                updated_data['code'] = code
                updated_data['description'] = description
                self.template_combo.setItemData(current_index, updated_data)
            else:
                # 如果找不到文件路徑，則當作新模板保存
                self._save_template_to_file(current_text, code, description)
            
        QMessageBox.information(self, "成功", "模板已保存")
            
    def new_template(self):
        """創建新模板"""
        name, ok = QInputDialog.getText(self, "新建模板", "請輸入模板名稱:")
        if ok and name:
            # 清空編輯器和描述
            self.editor.clear()
            self.description_edit.clear()
            
            # 添加新模板到下拉框
            self.template_combo.addItem(name, {'code': '', 'description': ''})
            self.template_combo.setCurrentIndex(self.template_combo.count() - 1)
            
            self.editor.setFocus()
            
    def delete_template(self):
        """刪除當前選中的模板"""
        current_index = self.template_combo.currentIndex()
        current_text = self.template_combo.itemText(current_index)
        
        # 不允許刪除預設模板
        if current_text.startswith("[預設]"):
            QMessageBox.warning(self, "無法刪除", "預設模板不能被刪除！")
            return
            
        reply = QMessageBox.question(
            self, "確認刪除", 
            f"確定要刪除模板 '{current_text}' 嗎？此操作不可恢復！",
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No
        )
        
        if reply != QMessageBox.Yes:
            return
            
        # 刪除模板文件
        template_data = self.template_combo.itemData(current_index)
        file_path = template_data.get('file_path')
        
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception as e:
                QMessageBox.critical(self, "刪除失敗", f"無法刪除模板文件: {str(e)}")
                return
                
        # 從下拉框中移除
        self.template_combo.removeItem(current_index)
        
        QMessageBox.information(self, "成功", f"模板 '{current_text}' 已被刪除")
            
    def _save_template_to_file(self, name, code, description=''):
        """將模板保存到文件"""
        # 生成唯一的文件名
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"{self._sanitize_filename(name)}_{timestamp}.json"
        file_path = os.path.join(TEMPLATES_DIR, file_name)
        
        # 創建模板數據
        template_data = {
            'name': name,
            'code': code,
            'description': description,
            'created_at': datetime.datetime.now().isoformat()
        }
        
        # 保存到文件
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(template_data, f, ensure_ascii=False, indent=2)
            return file_path
        except Exception as e:
            QMessageBox.critical(self, "保存失敗", f"無法保存模板文件: {str(e)}")
            return None
            
    def _update_template_file(self, file_path, name, code, description=''):
        """更新現有模板文件"""
        try:
            # 讀取原始數據
            with open(file_path, 'r', encoding='utf-8') as f:
                template_data = json.load(f)
                
            # 更新數據
            template_data['code'] = code
            template_data['description'] = description
            template_data['updated_at'] = datetime.datetime.now().isoformat()
            
            # 如果名稱改變，則更新名稱
            if template_data.get('name') != name:
                template_data['name'] = name
                
            # 保存回文件
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(template_data, f, ensure_ascii=False, indent=2)
                
            return True
        except Exception as e:
            QMessageBox.critical(self, "更新失敗", f"無法更新模板文件: {str(e)}")
            return False
    
    def _sanitize_filename(self, name):
        """處理文件名，去除不合法字符"""
        # 替換不合法字符
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            name = name.replace(char, '_')
            
        # 去除前後空格
        name = name.strip()
        
        # 如果名稱為空，則使用默認名稱
        if not name:
            name = "template"
            
        return name
            
    def get_current_template(self):
        """獲取當前模板代碼"""
        return self.editor.toPlainText().strip()

class StrategySettingsWidget(QWidget):
    """策略設置小部件"""
    
    def __init__(self, parent=None):
        super(StrategySettingsWidget, self).__init__(parent)
        
        self.init_ui()
        
    def init_ui(self):
        layout = QVBoxLayout(self)
        
        # 組織各種參數設置
        form_layout = QFormLayout()
        
        # 中性化設置
        self.neutralization_combo = QComboBox()
        self.neutralization_combo.addItems(["SUBINDUSTRY", "INDUSTRY", "SECTOR", "MARKET", "NONE"])
        form_layout.addRow("中性化 (Neutralization):", self.neutralization_combo)
        
        # 衰減設置
        self.decay_spin = QSpinBox()
        self.decay_spin.setRange(0, 252)
        self.decay_spin.setValue(0)
        form_layout.addRow("衰減 (Decay):", self.decay_spin)
        
        # 截斷設置
        self.truncation_spin = QDoubleSpinBox()
        self.truncation_spin.setRange(0, 0.5)
        self.truncation_spin.setSingleStep(0.01)
        self.truncation_spin.setValue(0.01)
        form_layout.addRow("截斷 (Truncation):", self.truncation_spin)
        
        # 延遲設置
        self.delay_spin = QSpinBox()
        self.delay_spin.setRange(1, 10)
        self.delay_spin.setValue(1)
        form_layout.addRow("延遲 (Delay):", self.delay_spin)
        
        # 宇宙設置
        self.universe_combo = QComboBox()
        self.universe_combo.addItems(["TOP3000", "TOP500", "TOP1000", "TOP200"])
        form_layout.addRow("宇宙 (Universe):", self.universe_combo)
        
        # 區域設置
        self.region_combo = QComboBox()
        self.region_combo.addItems(["USA", "GLOBAL", "JAPAN", "CHINA", "EUROPE"])
        form_layout.addRow("區域 (Region):", self.region_combo)
        
        # 巴氏殺菌設置
        self.pasteurization_combo = QComboBox()
        self.pasteurization_combo.addItems(["ON", "OFF"])
        form_layout.addRow("巴氏殺菌 (Pasteurization):", self.pasteurization_combo)
        
        # NaN處理設置
        self.nan_handling_combo = QComboBox()
        self.nan_handling_combo.addItems(["ON", "OFF"])
        form_layout.addRow("NaN處理 (NaN Handling):", self.nan_handling_combo)
        
        # 單位處理設置
        self.unit_handling_combo = QComboBox()
        self.unit_handling_combo.addItems(["VERIFY", "IGNORE"])
        form_layout.addRow("單位處理 (Unit Handling):", self.unit_handling_combo)
        
        # 添加表單佈局
        layout.addLayout(form_layout)
        layout.addStretch()
        
    def get_settings(self):
        """獲取當前設置"""
        return {
            'neutralization': self.neutralization_combo.currentText(),
            'decay': self.decay_spin.value(),
            'truncation': self.truncation_spin.value(),
            'delay': self.delay_spin.value(),
            'universe': self.universe_combo.currentText(),
            'region': self.region_combo.currentText(),
            'pasteurization': self.pasteurization_combo.currentText(),
            'nanHandling': self.nan_handling_combo.currentText(),
            'unitHandling': self.unit_handling_combo.currentText()
        }

class GeneratorMainWindow(QMainWindow):
    """策略生成器主窗口"""
    # Define the signal to emit the generated strategies
    strategies_ready_for_simulation = Signal(list)

    def __init__(self):
        super(GeneratorMainWindow, self).__init__()

        self.setWindowTitle("WorldQuant Brain 策略生成器")
        self.resize(1200, 800)

        # Remove initialization of the local simulation_table
        # self.simulation_table = None

        self.init_ui()
        
    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        main_layout = QVBoxLayout(central_widget)
        
        # 創建分割器，左側為字段選擇，右側為策略編輯
        splitter = QSplitter(Qt.Horizontal)
        
        # 左側：字段選擇器
        self.field_selector = FieldSelector()
        
        # 右側：策略編輯區
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        
        # 標籤頁：模板編輯和策略設置
        tabs = QTabWidget()
        
        # 模板編輯標籤頁
        self.template_editor = CodeTemplateEditor()
        tabs.addTab(self.template_editor, "代碼模板")
        
        # 策略設置標籤頁
        self.settings_widget = StrategySettingsWidget()
        tabs.addTab(self.settings_widget, "策略設置")

        # Remove the local simulation table tab
        # self.simulation_table = QTableWidget()
        # self.simulation_table.setEditTriggers(QAbstractItemView.NoEditTriggers) # 設置為不可編輯
        # self.simulation_table.setAlternatingRowColors(True)
        # tabs.addTab(self.simulation_table, "模擬表格")

        right_layout.addWidget(tabs) # Add the tabs widget directly
        
        # 預覽和生成區域
        generate_layout = QHBoxLayout()
        
        self.preview_btn = QPushButton("預覽策略")
        self.preview_btn.clicked.connect(self.preview_strategies)
        generate_layout.addWidget(self.preview_btn)
        
        self.generate_count_label = QLabel("生成 0 個策略")
        generate_layout.addWidget(self.generate_count_label)
        
        generate_layout.addStretch()

        # Keep the button, maybe rename it slightly
        self.import_to_simulator_btn = QPushButton("匯入到模擬")
        self.import_to_simulator_btn.clicked.connect(self.emit_strategies_for_simulation) # Connect to new emitting method
        generate_layout.addWidget(self.import_to_simulator_btn)

        self.generate_btn = QPushButton("生成策略文件")
        self.generate_btn.setStyleSheet("background-color: #4caf50; color: white; font-weight: bold; padding: 8px 16px;")
        self.generate_btn.clicked.connect(self.generate_strategies_file)
        generate_layout.addWidget(self.generate_btn)
        
        right_layout.addLayout(generate_layout)
        
        # 預覽區域
        preview_group = QGroupBox("策略預覽")
        preview_layout = QVBoxLayout()
        
        self.preview_text = QTextEdit()
        self.preview_text.setReadOnly(True)
        self.preview_text.setFont(QFont("Consolas", 10))
        
        preview_layout.addWidget(self.preview_text)
        preview_group.setLayout(preview_layout)
        
        right_layout.addWidget(preview_group)
        
        # 添加到分割器
        splitter.addWidget(self.field_selector)
        splitter.addWidget(right_panel)
        
        # 設置初始大小比例
        splitter.setSizes([400, 800])
        
        main_layout.addWidget(splitter)

        # Remove reference to local tabs_widget if only used for the removed table
        # self.tabs_widget = right_panel.findChild(QTabWidget)

    def emit_strategies_for_simulation(self):
        """生成策略列表並通過信號發送"""
        # 獲取字段數據
        selected_fields = self.field_selector.get_selected_fields()
        if not selected_fields:
            QMessageBox.warning(self, "警告", "未選擇任何字段，請先在左側選擇字段")
            return
            
        # 獲取模板代碼
        template_code = self.template_editor.get_current_template()
        if not template_code:
            QMessageBox.warning(self, "警告", "模板代碼為空")
            return
            
        # 獲取策略設置
        settings = self.settings_widget.get_settings()
        
        # 構建策略列表
        strategies = []
        for field in selected_fields:
            try:
                # 替換模板中的字段佔位符
                code = template_code.replace("{field}", field)
                strategy = settings.copy()
                strategy['code'] = code
                # Keep the generated strategy dictionary as is
                strategies.append(strategy)
            except Exception as e:
                print(f"為字段 {field} 生成策略時出錯: {str(e)}")

        if not strategies:
            QMessageBox.warning(self, "錯誤", "未能生成任何策略")
            return

        # Emit the signal with the list of strategy dictionaries
        self.strategies_ready_for_simulation.emit(strategies)


    def preview_strategies(self):
        """預覽將要生成的策略"""
        # 獲取字段數據
        selected_fields = self.field_selector.get_selected_fields()
        if not selected_fields:
            QMessageBox.warning(self, "警告", "未選擇任何字段，請先在左側選擇字段")
            return
            
        # 獲取模板代碼
        template_code = self.template_editor.get_current_template()
        if not template_code or '{' not in template_code:
            QMessageBox.warning(self, "警告", "模板代碼為空或缺少佔位符")
            return
            
        # 獲取策略設置
        settings = self.settings_widget.get_settings()
        
        # 預覽第一個策略
        if selected_fields:
            first_field = selected_fields[0]
            # 確保使用原始字段名，移除可能包含的格式信息
            code = template_code.replace("{field}", first_field)
            
            # 生成文件名
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            dataset_name = os.path.basename(self.field_selector.csv_file_path).split("_fields_formatted.csv")[0]
            file_name = f"alpha_{dataset_name}_{timestamp}.py"
            file_path = os.path.join(ALPHAS_DIR, file_name)
            
            preview = f"""# 將為 {len(selected_fields)} 個字段生成策略
# 輸出文件路徑: {file_path}
            
策略示例 (使用字段: {first_field}):
{{
    'neutralization': '{settings["neutralization"]}',
    'decay': {settings["decay"]},
    'truncation': {settings["truncation"]},
    'delay': {settings["delay"]},
    'universe': '{settings["universe"]}',
    'region': '{settings["region"]}',
    'pasteurization': '{settings["pasteurization"]}',
    'nanHandling': '{settings["nanHandling"]}',
    'unitHandling': '{settings["unitHandling"]}',
    'code': '''
    {code}
    '''
}}
"""
            self.preview_text.setText(preview)
            self.generate_count_label.setText(f"生成 {len(selected_fields)} 個策略")
            
    def generate_strategies_file(self):
        """生成策略文件"""
        # 獲取字段數據
        selected_fields = self.field_selector.get_selected_fields()
        if not selected_fields:
            QMessageBox.warning(self, "警告", "未選擇任何字段，請先在左側選擇字段")
            return
            
        # 獲取模板代碼
        template_code = self.template_editor.get_current_template()
        if not template_code:
            QMessageBox.warning(self, "警告", "模板代碼為空")
            return
            
        # 獲取策略設置
        settings = self.settings_widget.get_settings()
        
        # 構建策略列表
        strategies = []
        for field in selected_fields:
            try:
                # 替換模板中的字段佔位符，確保使用原始字段名
                code = template_code.replace("{field}", field)
                    
                # 創建策略字典
                strategy = settings.copy()
                strategy['code'] = code
                strategies.append(strategy)
            except Exception as e:
                print(f"為字段 {field} 生成策略時出錯: {str(e)}")
        
        if not strategies:
            QMessageBox.warning(self, "錯誤", "未能生成任何策略")
            return
        
        # 確保輸出目錄存在
        if not os.path.exists(ALPHAS_DIR):
            os.makedirs(ALPHAS_DIR)
            
        # 生成文件名
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        dataset_name = os.path.basename(self.field_selector.csv_file_path).split("_fields_formatted.csv")[0]
        file_name = f"alpha_{dataset_name}_{timestamp}.py"
        file_path = os.path.join(ALPHAS_DIR, file_name)
            
        # 將策略保存到Python文件
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("#!/usr/bin/env python\n")
                f.write("# -*- coding: utf-8 -*-\n\n")
                f.write("# 自動生成的策略列表\n")
                f.write(f"# 基於 {dataset_name} 數據集的 {len(strategies)} 個策略\n")
                f.write(f"# 生成時間: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                f.write("DATA = [\n")
                
                for i, strategy in enumerate(strategies):
                    f.write(f"    # 策略{i+1}: {strategy['code'].strip()}\n")
                    f.write("    {\n")
                    for key, value in strategy.items():
                        if isinstance(value, str):
                            if key == 'code':
                                f.write("        'code': '''\n")
                                f.write(f"        {value}\n")
                                f.write("        '''\n")
                            else:
                                f.write(f"        '{key}': '{value}',\n")
                        else:
                            f.write(f"        '{key}': {value},\n")
                    f.write("    },\n\n")
                
                f.write("]\n")
                
            QMessageBox.information(
                self, "成功", f"成功生成 {len(strategies)} 個策略並保存到:\n{file_path}"
            )
        except Exception as e:
            QMessageBox.critical(self, "錯誤", f"保存策略文件時出錯: {str(e)}")


def main():
    app = QApplication(sys.argv)
    
    # 設置應用程序樣式
    app.setStyle("Fusion")
    
    # 創建主窗口
    window = GeneratorMainWindow()
    window.show()
    
    sys.exit(app.exec())

if __name__ == "__main__":
    main()

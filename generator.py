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
import ast
from PySide6.QtWidgets import (QApplication, QMainWindow, QVBoxLayout, QHBoxLayout,
                             QWidget, QLabel, QComboBox, QLineEdit, QTextEdit,
                             QPushButton, QFileDialog, QMessageBox, QTabWidget,
                             QGroupBox, QFormLayout, QCheckBox, QSpinBox,
                             QDoubleSpinBox, QListWidget, QListWidgetItem,
                              QSplitter, QFrame, QTableWidget, QTableWidgetItem,
                              QHeaderView, QAbstractItemView, QInputDialog) # QInputDialog 已存在
from PySide6.QtCore import Qt, QDir, Signal, Slot # Keep Signal and Slot
from PySide6.QtGui import QFont, QColor

import re
from PySide6.QtGui import QSyntaxHighlighter, QTextCharFormat, QColor, QFont
# 匯入模擬參數預設值
from simulation import PARAM_COLUMNS, DEFAULT_VALUES
# 獲取腳本所在目錄的絕對路徑
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# 定義常量 - 使用絕對路徑
DATASETS_DIR = os.path.join(SCRIPT_DIR, "datasets")
TEMPLATES_DIR = os.path.join(SCRIPT_DIR, "templates")
ALPHAS_DIR = os.path.join(SCRIPT_DIR, "alphas")

class SelectedFieldsWidget(QWidget):
    """用於顯示和管理已選字段的小部件"""
    
    def __init__(self, parent=None):
        super(SelectedFieldsWidget, self).__init__(parent)
        # 確保輸出目錄存在
        if not os.path.exists(ALPHAS_DIR):
            os.makedirs(ALPHAS_DIR)
            
        self.init_ui()
        
    def init_ui(self):
        """初始化界面"""
        layout = QVBoxLayout(self)
        
        # 已選字段組
        group = QGroupBox("已選字段")
        group_layout = QVBoxLayout()
        
        # 字段列表
        self.field_list = QListWidget()
        self.field_list.setFont(QFont("Consolas", 14))
        self.field_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.field_list.itemDoubleClicked.connect(self.edit_field)
        group_layout.addWidget(self.field_list)
        
        # 按鈕區域
        buttons = QHBoxLayout()
        
        # 清空按鈕
        self.clear_btn = QPushButton("清空所有")
        self.clear_btn.setToolTip("清空所有已選字段")
        self.clear_btn.clicked.connect(self.clear_fields)
        buttons.addWidget(self.clear_btn)
        
        buttons.addStretch()
        
        # 移除按鈕
        self.remove_btn = QPushButton("移除選中")
        self.remove_btn.setToolTip("移除選中的字段")
        self.remove_btn.clicked.connect(self.remove_selected)
        buttons.addWidget(self.remove_btn)
        
        # 手動輸入按鈕
        self.custom_btn = QPushButton("手動輸入")
        self.custom_btn.setToolTip("手動輸入字段名稱")
        self.custom_btn.clicked.connect(self.add_custom_field)
        buttons.addWidget(self.custom_btn)
        
        group_layout.addLayout(buttons)
        group.setLayout(group_layout)
        layout.addWidget(group)
        
    def clear_fields(self):
        """清空所有字段"""
        self.field_list.clear()
        
    def get_selected_fields(self):
        """獲取所有已選字段"""
        fields = []
        for i in range(self.field_list.count()):
            fields.append(self.field_list.item(i).text())
        return fields
        
    def remove_selected(self):
        """移除選中的字段"""
        for item in self.field_list.selectedItems():
            self.field_list.takeItem(self.field_list.row(item))
            
    def add_custom_field(self):
        """手動輸入字段，支援逗號分隔或 Python 列表格式"""
        text, ok = QInputDialog.getText(
            self,
            "添加字段",
            "請輸入字段名稱 (多個字段可用逗號分隔，或輸入 Python 列表格式的字串):",
            QLineEdit.Normal,
            ""
        )

        if ok and text.strip():
            fields = []
            try:
                # 嘗試解析 Python 列表格式
                parsed_input = ast.literal_eval(text.strip())
                if isinstance(parsed_input, list):
                    # 確保列表中的元素都是字串
                    fields = [str(item).strip() for item in parsed_input if str(item).strip()]
                else:
                    # 如果解析結果不是列表，則按逗號分隔處理
                    QMessageBox.warning(self, "格式錯誤", "輸入的不是有效的 Python 列表，將嘗試按逗號分隔處理。")
                    fields = [f.strip() for f in text.split(',') if f.strip()]
            except (ValueError, SyntaxError):
                # 解析失敗，回退到逗號分隔
                fields = [f.strip() for f in text.split(',') if f.strip()]

            if fields:
                self.add_fields(fields)
            else:
                QMessageBox.warning(self, "輸入無效", "未能從輸入中提取有效的字段名稱。")
    @Slot(list)
    def add_fields_from_list(self, fields):
        """從列表添加多個字段 (Slot for external signals)"""
        if isinstance(fields, list):
            self.add_fields(fields)
        else:
            QMessageBox.warning(self, "匯入錯誤", "預期接收字段列表")
            
    def edit_field(self, item):
        """編輯選中的字段"""
        old_text = item.text()
        new_text, ok = QInputDialog.getText(
            self,
            "編輯字段",
            "修改字段名稱:",
            QLineEdit.Normal,
            old_text
        )
        
        if ok and new_text.strip() and new_text != old_text:
            # 檢查是否與其他字段重複
            exists = False
            for i in range(self.field_list.count()):
                if i != self.field_list.row(item) and self.field_list.item(i).text() == new_text:
                    exists = True
                    break
            
            if not exists:
                item.setText(new_text.strip())
                item.setToolTip(new_text.strip())
            else:
                QMessageBox.warning(self, "錯誤", "此字段名稱已存在")

    def add_fields(self, fields):
        """添加一個或多個字段到列表"""
        if not fields:
            return
            
        if isinstance(fields, str):
            fields = [fields]
            
        added_count = 0
        skipped_count = 0
        
        for field in fields:
            if isinstance(field, str) and field.strip():
                field = field.strip()
                # 檢查是否已存在
                exists = False
                for i in range(self.field_list.count()):
                    if self.field_list.item(i).text() == field:
                        exists = True
                        break
                        
                if not exists:
                    item = QListWidgetItem(field)
                    item.setToolTip(field)  # 設置提示文字
                    self.field_list.addItem(item)
                    added_count += 1
                else:
                    skipped_count += 1
                    
        if added_count > 0:
            status = f"已添加 {added_count} 個字段"
            if skipped_count > 0:
                status += f"，跳過 {skipped_count} 個重複字段"
            QMessageBox.information(self, "添加成功", status)
        elif skipped_count > 0:
            QMessageBox.information(self, "提示", f"所選的 {skipped_count} 個字段已存在")


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

        # 取得 simulation_widget.py 的預設值（去除 checkbox 的 None）
        sim_defaults = DEFAULT_VALUES[1:]

        # 組織各種參數設置
        form_layout = QFormLayout()

        # 中性化設置
        self.neutralization_combo = QComboBox()
        self.neutralization_combo.addItems(["SUBINDUSTRY", "INDUSTRY", "SECTOR", "MARKET", "NONE"])
        # simulation_widget.py 預設值為 sim_defaults[3]，但 simulation_widget.py 的 neutralization 是 index 3
        self.neutralization_combo.setCurrentText(str(sim_defaults[3]))
        form_layout.addRow("中性化 (Neutralization):", self.neutralization_combo)

        # 衰減設置
        self.decay_spin = QSpinBox()
        self.decay_spin.setRange(0, 252)
        self.decay_spin.setValue(int(sim_defaults[1]))
        form_layout.addRow("衰減 (Decay):", self.decay_spin)

        # 截斷設置
        self.truncation_spin = QDoubleSpinBox()
        self.truncation_spin.setRange(0, 0.5)
        self.truncation_spin.setSingleStep(0.01)
        self.truncation_spin.setValue(float(sim_defaults[5]))
        form_layout.addRow("截斷 (Truncation):", self.truncation_spin)

        # 延遲設置
        self.delay_spin = QSpinBox()
        self.delay_spin.setRange(1, 10)
        self.delay_spin.setValue(int(sim_defaults[2]))
        form_layout.addRow("延遲 (Delay):", self.delay_spin)

        # 宇宙設置
        self.universe_combo = QComboBox()
        self.universe_combo.addItems(["TOP3000", "TOP500", "TOP1000", "TOP200"])
        # simulation_widget.py 預設值為 sim_defaults[6]
        self.universe_combo.setCurrentText(str(sim_defaults[6]))
        form_layout.addRow("宇宙 (Universe):", self.universe_combo)

        # 區域設置
        self.region_combo = QComboBox()
        self.region_combo.addItems(["USA", "GLOBAL", "JAPAN", "CHINA", "EUROPE"])
        self.region_combo.setCurrentText(str(sim_defaults[4]))
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
        
        # 右側：策略編輯區
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        
        # 左側：已選字段列表
        self.selected_fields_widget = SelectedFieldsWidget()
        
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
        self.preview_text.setFont(QFont("Consolas", 14))
        
        preview_layout.addWidget(self.preview_text)
        preview_group.setLayout(preview_layout)
        
        right_layout.addWidget(preview_group)
        
        # 創建分割器
        splitter = QSplitter(Qt.Horizontal)
        splitter.addWidget(self.selected_fields_widget)
        splitter.addWidget(right_panel)
        
        # 設置初始大小比例 (調整比例以適應新的佈局)
        splitter.setSizes([300, 900])
        
        main_layout.addWidget(splitter)

        # Remove reference to local tabs_widget if only used for the removed table
        # self.tabs_widget = right_panel.findChild(QTabWidget)

    def emit_strategies_for_simulation(self):
        """生成策略列表並通過信號發送"""
        # 獲取字段數據
        selected_fields = self.selected_fields_widget.get_selected_fields()
        if not selected_fields:
            QMessageBox.warning(self, "警告", "未選擇任何字段")
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
        selected_fields = self.selected_fields_widget.get_selected_fields()
        if not selected_fields:
            QMessageBox.warning(self, "警告", "未選擇任何字段")
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
            dataset_name = "custom"  # 由於不再依賴 CSV 文件，使用固定名稱
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
        selected_fields = self.selected_fields_widget.get_selected_fields()
        if not selected_fields:
            QMessageBox.warning(self, "警告", "未選擇任何字段")
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
        dataset_name = "custom"  # 由於不再依賴 CSV 文件，使用固定名稱
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

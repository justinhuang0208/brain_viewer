#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
WorldQuant Brain Strategy Generator
GUI tool for designing strategy templates and generating strategy files
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
    """Widget to display and manage selected fields"""
    
    def __init__(self, parent=None):
        super(SelectedFieldsWidget, self).__init__(parent)
        # 確保輸出目錄存在
        if not os.path.exists(ALPHAS_DIR):
            os.makedirs(ALPHAS_DIR)
            
        self.init_ui()
        
    def init_ui(self):
        """Initialize UI"""
        layout = QVBoxLayout(self)
        
        # 已選字段組
        group = QGroupBox("Selected Fields")
        group_layout = QVBoxLayout()
        
        # 字段列表
        self.field_list = QListWidget()
        self.field_list.setFont(QFont("Consolas", 14))
        self.field_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.field_list.itemDoubleClicked.connect(self.edit_field)
        group_layout.addWidget(self.field_list)
        
        # 按鈕區域
        buttons = QHBoxLayout()
        
        # Clear button
        self.clear_btn = QPushButton("Clear All")
        self.clear_btn.setToolTip("Clear all selected fields")
        self.clear_btn.clicked.connect(self.clear_fields)
        buttons.addWidget(self.clear_btn)
        
        buttons.addStretch()
        
        # Remove button
        self.remove_btn = QPushButton("Remove Selected")
        self.remove_btn.setToolTip("Remove selected fields")
        self.remove_btn.clicked.connect(self.remove_selected)
        buttons.addWidget(self.remove_btn)
        
        # Manual input button
        self.custom_btn = QPushButton("Add Manually")
        self.custom_btn.setToolTip("Enter field names manually")
        self.custom_btn.clicked.connect(self.add_custom_field)
        buttons.addWidget(self.custom_btn)
        
        group_layout.addLayout(buttons)
        group.setLayout(group_layout)
        layout.addWidget(group)
        
    def clear_fields(self):
        """Clear all fields"""
        self.field_list.clear()
        
    def get_selected_fields(self):
        """Get all selected fields"""
        fields = []
        for i in range(self.field_list.count()):
            fields.append(self.field_list.item(i).text())
        return fields
        
    def remove_selected(self):
        """Remove selected fields"""
        for item in self.field_list.selectedItems():
            self.field_list.takeItem(self.field_list.row(item))
            
    def add_custom_field(self):
        """Manually input fields; support comma-separated or Python list format"""
        text, ok = QInputDialog.getMultiLineText(
            self,
            "Add Fields",
            "Enter field names (comma-separated or Python list string):",
            ""
        )

        if ok and text.strip():
            fields = []
            raw = text.strip()
            try:
                parsed_input = ast.literal_eval(raw)
                if isinstance(parsed_input, list):
                    fields = [str(item).strip() for item in parsed_input if str(item).strip()]
                else:
                    # 不是列表，回退到逗號分隔
                    fields = [f.strip() for f in raw.split(',') if f.strip()]
            except (ValueError, SyntaxError):
                # 解析失敗，回退到逗號分隔
                fields = [f.strip() for f in raw.split(',') if f.strip()]

            if fields:
                self.add_fields(fields)
            else:
                QMessageBox.warning(self, "Invalid Input", "Could not parse valid field names from input.")
    @Slot(list)
    def add_fields_from_list(self, fields):
        """Add multiple fields from list (Slot for external signals)"""
        if isinstance(fields, list):
            self.add_fields(fields)
        else:
            QMessageBox.warning(self, "Import Error", "Expected a list of fields")
            
    def edit_field(self, item):
        """Edit selected field"""
        old_text = item.text()
        new_text, ok = QInputDialog.getText(
            self,
            "Edit Field",
            "Modify field name:",
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
                QMessageBox.warning(self, "Error", "Field name already exists")

    def add_fields(self, fields):
        """Add one or more fields to the list"""
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
            status = f"Added {added_count} field(s)"
            if skipped_count > 0:
                status += f", skipped {skipped_count} duplicate(s)"
            QMessageBox.information(self, "Added", status)
        elif skipped_count > 0:
            QMessageBox.information(self, "Info", f"{skipped_count} selected field(s) already exist")


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
            r'\+', r'\-', r'\*', r'\/', r'<', r'<=', r'==', r'>', r'>=', r'!=', "vector_neut"
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
    """Code template editor"""
    
    def __init__(self, parent=None):
        super(CodeTemplateEditor, self).__init__(parent)
        
        # 確保模板目錄存在
        if not os.path.exists(TEMPLATES_DIR):
            os.makedirs(TEMPLATES_DIR)
            
        self.init_ui()
        self.load_templates()
        
    def init_ui(self):
        layout = QVBoxLayout(self)
        
        # Template selection and management
        template_header = QHBoxLayout()
        
        template_header.addWidget(QLabel("Template:"))
        self.template_combo = QComboBox()
        self.template_combo.currentIndexChanged.connect(self.load_selected_template)
        template_header.addWidget(self.template_combo, 1)
        
        self.save_btn = QPushButton("Save")
        self.save_btn.clicked.connect(self.save_template)
        template_header.addWidget(self.save_btn)
        
        self.new_btn = QPushButton("New")
        self.new_btn.clicked.connect(self.new_template)
        template_header.addWidget(self.new_btn)
        
        self.delete_btn = QPushButton("Delete")
        self.delete_btn.clicked.connect(self.delete_template)
        template_header.addWidget(self.delete_btn)
        
        # 模板編輯區域
        self.editor = QTextEdit()
# 應用語法高亮
        self.highlighter = AlphaSyntaxHighlighter(self.editor.document())
        self.editor.setFont(QFont("Consolas", 14))
        self.editor.setPlaceholderText("Enter code template here; use {field} as placeholder...")
        
        # Template description
        desc_layout = QHBoxLayout()
        desc_layout.addWidget(QLabel("Description:"))
        self.description_edit = QLineEdit()
        self.description_edit.setPlaceholderText("(Optional) Describe the purpose of this template")
        desc_layout.addWidget(self.description_edit)
        
        # Help text
        help_text = """
        <b>Template Usage:</b>
        <ul>
          <li>Use <code>{field}</code> as the field placeholder</li>
          <li>Supports all operators like ts_rank, rank, zscore, ...</li>
          <li>Templates are saved under the templates/ directory</li>
        </ul>
        """
        help_label = QLabel(help_text)
        help_label.setTextFormat(Qt.RichText)
        
        layout.addLayout(template_header)
        layout.addLayout(desc_layout)
        layout.addWidget(self.editor, 1)
        layout.addWidget(help_label)
        
    def load_templates(self):
        """Load default and saved templates"""
        self.template_combo.clear()
        
        # Default templates
        default_templates = {
            "Basic ts_rank": {
                "code": "ts_rank({field}, 126)",
                "description": "Time-series rank over 126 days"
            },
            "Basic rank": {
                "code": "rank({field})",
                "description": "Cross-sectional rank"
            },
            "Basic zscore": {
                "code": "zscore({field})",
                "description": "Standardized score"
            },
            "Industry comparison": {
                "code": "group_rank({field}, industry)",
                "description": "Rank within industry"
            },
            "Subindustry comparison": {
                "code": "group_rank({field}, subindustry)",
                "description": "Rank within subindustry"
            },
            "Complex momentum": {
                "code": "ts_rank(ts_delta({field}, 5) / ts_delay({field}, 5), 21)",
                "description": "21-day rank of 5-day rate of change"
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
                            print(f"Error reading template file {file_path}: {str(e)}")
        except Exception as e:
            print(f"Error scanning templates directory: {str(e)}")
        
        # 先添加預設模板
        for name, data in default_templates.items():
            self.template_combo.addItem(f"[Default] {name}", data)
            
        # 再添加自定義模板
        for name, data in custom_templates.items():
            self.template_combo.addItem(name, data)
            
        # 選擇第一個模板
        if self.template_combo.count() > 0:
            self.template_combo.setCurrentIndex(0)
            
    def load_selected_template(self):
        """Load selected template into editor"""
        index = self.template_combo.currentIndex()
        if index >= 0:
            template_data = self.template_combo.itemData(index)
            self.editor.setText(template_data.get('code', ''))
            self.description_edit.setText(template_data.get('description', ''))
            
    def save_template(self):
        """Save current template"""
        code = self.editor.toPlainText().strip()
        description = self.description_edit.text().strip()
        
        if not code:
            QMessageBox.warning(self, "Error", "Template cannot be empty")
            return
            
        # 檢查是否是編輯預設模板
        current_index = self.template_combo.currentIndex()
        current_text = self.template_combo.itemText(current_index)
        
        if current_text.startswith("[Default]"):
            # 建立新的自定義模板而不是修改預設的
            name, ok = QInputDialog.getText(self, "Save Template", "Enter template name:")
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
            
            QMessageBox.information(self, "Success", "Template saved")
            
    def new_template(self):
        """Create new template"""
        name, ok = QInputDialog.getText(self, "New Template", "Enter template name:")
        if ok and name:
            # 清空編輯器和描述
            self.editor.clear()
            self.description_edit.clear()
            
            # 添加新模板到下拉框
            self.template_combo.addItem(name, {'code': '', 'description': ''})
            self.template_combo.setCurrentIndex(self.template_combo.count() - 1)
            
            self.editor.setFocus()
            
    def delete_template(self):
        """Delete currently selected template"""
        current_index = self.template_combo.currentIndex()
        current_text = self.template_combo.itemText(current_index)
        
        # 不允許刪除預設模板
        if current_text.startswith("[Default]"):
            QMessageBox.warning(self, "Cannot Delete", "Default templates cannot be deleted!")
            return
            
        reply = QMessageBox.question(
            self, "Confirm Delete", 
            f"Delete template '{current_text}'? This cannot be undone!",
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
                QMessageBox.critical(self, "Delete Failed", f"Unable to delete template file: {str(e)}")
                return
                
        # 從下拉框中移除
        self.template_combo.removeItem(current_index)
        
        QMessageBox.information(self, "Success", f"Template '{current_text}' deleted")
            
    def _save_template_to_file(self, name, code, description=''):
        """Save template to file"""
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
            QMessageBox.critical(self, "Save Failed", f"Unable to save template file: {str(e)}")
            return None
            
    def _update_template_file(self, file_path, name, code, description=''):
        """Update existing template file"""
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
            QMessageBox.critical(self, "Update Failed", f"Unable to update template file: {str(e)}")
            return False
    
    def _sanitize_filename(self, name):
        """Sanitize file name by removing invalid characters"""
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
    """Strategy settings widget"""
    
    def __init__(self, parent=None):
        super(StrategySettingsWidget, self).__init__(parent)
        
        self.init_ui()
        
    def init_ui(self):
        layout = QVBoxLayout(self)

        # 取得 simulation_widget.py 的預設值（去除 checkbox 的 None）
        sim_defaults = DEFAULT_VALUES[1:]

        # 組織各種參數設置
        form_layout = QFormLayout()

        # Neutralization
        self.neutralization_combo = QComboBox()
        self.neutralization_combo.addItems(["SUBINDUSTRY", "INDUSTRY", "SECTOR", "MARKET", "NONE"])
        # simulation_widget.py 預設值為 sim_defaults[3]，但 simulation_widget.py 的 neutralization 是 index 3
        self.neutralization_combo.setCurrentText(str(sim_defaults[3]))
        form_layout.addRow("Neutralization:", self.neutralization_combo)

        # Decay
        self.decay_spin = QSpinBox()
        self.decay_spin.setRange(0, 252)
        self.decay_spin.setValue(int(sim_defaults[1]))
        form_layout.addRow("Decay:", self.decay_spin)

        # Truncation
        self.truncation_spin = QDoubleSpinBox()
        self.truncation_spin.setRange(0, 0.5)
        self.truncation_spin.setSingleStep(0.01)
        self.truncation_spin.setValue(float(sim_defaults[5]))
        form_layout.addRow("Truncation:", self.truncation_spin)

        # Delay
        self.delay_spin = QSpinBox()
        self.delay_spin.setRange(1, 10)
        self.delay_spin.setValue(int(sim_defaults[2]))
        form_layout.addRow("Delay:", self.delay_spin)

        # Universe
        self.universe_combo = QComboBox()
        self.universe_combo.addItems(["TOP3000", "TOP500", "TOP1000", "TOP200"])
        # simulation_widget.py 預設值為 sim_defaults[6]
        self.universe_combo.setCurrentText(str(sim_defaults[6]))
        form_layout.addRow("Universe:", self.universe_combo)

        # Region
        self.region_combo = QComboBox()
        self.region_combo.addItems(["USA", "GLOBAL", "JAPAN", "CHINA", "EUROPE"])
        self.region_combo.setCurrentText(str(sim_defaults[4]))
        form_layout.addRow("Region:", self.region_combo)

        # Pasteurization
        self.pasteurization_combo = QComboBox()
        self.pasteurization_combo.addItems(["ON", "OFF"])
        form_layout.addRow("Pasteurization:", self.pasteurization_combo)

        # NaN Handling
        self.nan_handling_combo = QComboBox()
        self.nan_handling_combo.addItems(["ON", "OFF"])
        form_layout.addRow("NaN Handling:", self.nan_handling_combo)

        # Unit Handling
        self.unit_handling_combo = QComboBox()
        self.unit_handling_combo.addItems(["VERIFY", "IGNORE"])
        form_layout.addRow("Unit Handling:", self.unit_handling_combo)

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
    """Strategy Generator main window"""
    # Define the signal to emit the generated strategies
    strategies_ready_for_simulation = Signal(list)

    def __init__(self):
        super(GeneratorMainWindow, self).__init__()

        self.setWindowTitle("WorldQuant Brain Strategy Generator")
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
        
        # Left: Selected fields list
        self.selected_fields_widget = SelectedFieldsWidget()
        
        # Tabs: template editor and strategy settings
        tabs = QTabWidget()
        
        # Template editor tab
        self.template_editor = CodeTemplateEditor()
        tabs.addTab(self.template_editor, "Code Template")
        
        # Strategy settings tab
        self.settings_widget = StrategySettingsWidget()
        tabs.addTab(self.settings_widget, "Strategy Settings")

        # Remove the local simulation table tab
        # self.simulation_table = QTableWidget()
        # self.simulation_table.setEditTriggers(QAbstractItemView.NoEditTriggers) # 設置為不可編輯
        # self.simulation_table.setAlternatingRowColors(True)
        # tabs.addTab(self.simulation_table, "模擬表格")

        right_layout.addWidget(tabs) # Add the tabs widget directly
        
        # Preview and generate area
        generate_layout = QHBoxLayout()
        
        self.preview_btn = QPushButton("Preview Strategies")
        self.preview_btn.clicked.connect(self.preview_strategies)
        generate_layout.addWidget(self.preview_btn)
        
        self.generate_count_label = QLabel("Generate 0 strategies")
        generate_layout.addWidget(self.generate_count_label)
        
        generate_layout.addStretch()

        # Keep the button, maybe rename it slightly
        self.import_to_simulator_btn = QPushButton("Import to Simulation")
        self.import_to_simulator_btn.clicked.connect(self.emit_strategies_for_simulation) # Connect to new emitting method
        generate_layout.addWidget(self.import_to_simulator_btn)

        self.generate_btn = QPushButton("Generate Strategy File")
        self.generate_btn.setStyleSheet("background-color: #4caf50; color: white; font-weight: bold; padding: 8px 16px;")
        self.generate_btn.clicked.connect(self.generate_strategies_file)
        generate_layout.addWidget(self.generate_btn)
        
        right_layout.addLayout(generate_layout)
        
        # Preview area
        preview_group = QGroupBox("Strategy Preview")
        preview_layout = QVBoxLayout()
        
        self.preview_text = QTextEdit()
        self.preview_text.setReadOnly(True)
        self.preview_text.setFont(QFont("Consolas", 14))
        
        preview_layout.addWidget(self.preview_text)
        preview_group.setLayout(preview_layout)
        
        right_layout.addWidget(preview_group)
        
        # Splitter
        splitter = QSplitter(Qt.Horizontal)
        splitter.addWidget(self.selected_fields_widget)
        splitter.addWidget(right_panel)
        
        # 設置初始大小比例 (調整比例以適應新的佈局)
        splitter.setSizes([300, 900])
        
        main_layout.addWidget(splitter)

        # Remove reference to local tabs_widget if only used for the removed table
        # self.tabs_widget = right_panel.findChild(QTabWidget)

    def emit_strategies_for_simulation(self):
        """Generate strategy list and emit via signal"""
        # Get fields
        selected_fields = self.selected_fields_widget.get_selected_fields()
        if not selected_fields:
            QMessageBox.warning(self, "Warning", "No fields selected")
            return
            
        # Get template code
        template_code = self.template_editor.get_current_template()
        if not template_code:
            QMessageBox.warning(self, "Warning", "Template code is empty")
            return
            
        # Get settings
        settings = self.settings_widget.get_settings()
        
        # Build strategies list
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
            QMessageBox.warning(self, "Error", "Failed to generate any strategy")
            return

        # Emit the signal with the list of strategy dictionaries
        self.strategies_ready_for_simulation.emit(strategies)


    def preview_strategies(self):
        """Preview strategies to be generated"""
        # Get fields
        selected_fields = self.selected_fields_widget.get_selected_fields()
        if not selected_fields:
            QMessageBox.warning(self, "Warning", "No fields selected")
            return
            
        # Get template code
        template_code = self.template_editor.get_current_template()
        if not template_code or '{' not in template_code:
            QMessageBox.warning(self, "Warning", "Template code is empty or missing placeholder")
            return
            
        # Get settings
        settings = self.settings_widget.get_settings()
        
        # Preview first strategy
        if selected_fields:
            first_field = selected_fields[0]
            # 確保使用原始字段名，移除可能包含的格式信息
            code = template_code.replace("{field}", first_field)
            
            # 生成文件名
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            dataset_name = "custom"  # Fixed name (no longer based on CSV)
            file_name = f"alpha_{dataset_name}_{timestamp}.py"
            file_path = os.path.join(ALPHAS_DIR, file_name)
            
            preview = f"""# Will generate strategies for {len(selected_fields)} fields
# Output file: {file_path}

Strategy example (field: {first_field}):
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
            self.generate_count_label.setText(f"Generate {len(selected_fields)} strategies")
            
    def generate_strategies_file(self):
        """Generate strategy file"""
        # Get fields
        selected_fields = self.selected_fields_widget.get_selected_fields()
        if not selected_fields:
            QMessageBox.warning(self, "Warning", "No fields selected")
            return
            
        # Get template code
        template_code = self.template_editor.get_current_template()
        if not template_code:
            QMessageBox.warning(self, "Warning", "Template code is empty")
            return
            
        # Get settings
        settings = self.settings_widget.get_settings()
        
        # Build strategies list
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
            QMessageBox.warning(self, "Error", "Failed to generate any strategy")
            return
        
        # Ensure output directory exists
        if not os.path.exists(ALPHAS_DIR):
            os.makedirs(ALPHAS_DIR)
            
        # Generate file name
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        dataset_name = "custom"  # Fixed name (no longer CSV-based)
        file_name = f"alpha_{dataset_name}_{timestamp}.py"
        file_path = os.path.join(ALPHAS_DIR, file_name)
            
        # Write strategies to Python file
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("#!/usr/bin/env python\n")
                f.write("# -*- coding: utf-8 -*-\n\n")
                f.write("# Auto-generated strategies\n")
                f.write(f"# {len(strategies)} strategies based on {dataset_name}\n")
                f.write(f"# Generated at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                f.write("DATA = [\n")
                
                for i, strategy in enumerate(strategies):
                    f.write(f"    # Strategy {i+1}: {strategy['code'].strip()}\n")
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
                self, "Success", f"Generated {len(strategies)} strategies and saved to:\n{file_path}"
            )
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error saving strategy file: {str(e)}")


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

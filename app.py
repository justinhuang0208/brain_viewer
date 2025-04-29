#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
WorldQuant Brain 工具箱
整合數據集瀏覽、回測結果分析和策略生成功能
"""

import sys
import os
import pandas as pd
from PySide6.QtWidgets import (QApplication, QMainWindow, QTabWidget, QSplitter,
                              QVBoxLayout, QHBoxLayout, QWidget, QLabel, 
                              QStatusBar, QMessageBox, QFrame)
from PySide6.QtCore import Qt, QDir, QSize
from PySide6.QtGui import QIcon, QFont, QPalette, QColor

# 獲取腳本所在目錄的絕對路徑
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# 定義資源目錄常量 - 使用絕對路徑
DATASETS_DIR = os.path.join(SCRIPT_DIR, "datasets")
DATA_DIR = "data"
TEMPLATES_DIR = os.path.join(SCRIPT_DIR, "templates")
ALPHAS_DIR = os.path.join(SCRIPT_DIR, "alphas")

# 資源檢查函數
def check_resources():
    resources = {
        DATASETS_DIR: "數據集目錄",
        DATA_DIR: "回測結果目錄",
        TEMPLATES_DIR: "模板目錄",
        ALPHAS_DIR: "策略輸出目錄"
    }
    
    missing = []
    for path, desc in resources.items():
        if not os.path.exists(path):
            try:
                os.makedirs(path)
                print(f"已創建 {desc} ({path})")
            except:
                missing.append(f"{desc} ({path})")
    
    if missing:
        return False, f"找不到必要的資源: {', '.join(missing)}"
    return True, "資源檢查通過"

# 重構DatasetViewer為Widget
class DatasetViewerWidget(QWidget):
    def __init__(self, parent=None):
        super(DatasetViewerWidget, self).__init__(parent)
        
        # 導入原始數據集查看器主窗口類
        from dataset_viewer import MainWindow as DsMainWindow
        
        # 創建布局
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # 創建數據集查看器實例
        self.viewer = DsMainWindow()
        
        # 提取中央窗口部分
        central_widget = self.viewer.centralWidget()
        
        # 將中央窗口添加到此Widget的布局中
        layout.addWidget(central_widget)
        
        # 保存狀態欄的引用
        self.status_bar = self.viewer.statusBar()
            
    def get_status_message(self):
        """獲取當前狀態欄消息"""
        return self.status_bar.currentMessage()

# 重構BacktestViewer為Widget
class BacktestViewerWidget(QWidget):
    def __init__(self, parent=None):
        super(BacktestViewerWidget, self).__init__(parent)
        
        # 導入原始回測查看器主窗口類
        from backtest_viewer import MainWindow as BtMainWindow
        
        # 創建布局
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # 創建回測查看器實例，並傳遞 DATA_DIR
        self.viewer = BtMainWindow(data_path=DATA_DIR)
        
        # 提取中央窗口部分
        central_widget = self.viewer.centralWidget()
        
        # 將中央窗口添加到此Widget的布局中
        layout.addWidget(central_widget)
        
        # 保存狀態欄的引用
        self.status_bar = self.viewer.statusBar()
            
    def get_status_message(self):
        """獲取當前狀態欄消息"""
        return self.status_bar.currentMessage()

# 重構StrategyGenerator為Widget
class StrategyGeneratorWidget(QWidget):
    def __init__(self, parent=None):
        super(StrategyGeneratorWidget, self).__init__(parent)
        
        # 導入原始策略生成器主窗口類
        from generator import GeneratorMainWindow
        
        # 創建布局
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # 創建策略生成器實例
        self.generator = GeneratorMainWindow()
        
        # 提取中央窗口部分
        central_widget = self.generator.centralWidget()
        
        # 將中央窗口添加到此Widget的布局中
        layout.addWidget(central_widget)
        
    def get_status_message(self):
        """獲取當前狀態欄消息"""
        return "策略生成模式"

from simulation_widget import SimulationWidget

# 主應用程序窗口
class MainWindow(QMainWindow):
    def __init__(self):
        super(MainWindow, self).__init__()
        self.setWindowTitle("WorldQuant Brain 工具箱")
        self.setMinimumSize(1280, 900)
        
        # 創建中央窗口和主布局
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)  # 減少邊距
        
        # 創建標籤頁
        self.tab_widget = QTabWidget()
        self.tab_widget.setTabPosition(QTabWidget.North)
        self.tab_widget.setStyleSheet("""
            QTabWidget::pane { 
                border: 1px solid #cccccc; 
                background: white; 
            }
            QTabBar::tab {
                background: #f0f0f0;
                border: 1px solid #cccccc;
                padding: 8px 15px;
                margin-right: 2px;
            }
            QTabBar::tab:selected {
                background: white;
                border-bottom-color: white;
            }
        """)
        
        # 創建數據集查看器、回測結果查看器和策略生成器和模擬器
        self.dataset_viewer = DatasetViewerWidget()
        self.backtest_viewer = BacktestViewerWidget()
        self.strategy_generator = StrategyGeneratorWidget()
        self.simulation_widget = SimulationWidget()
        
        # 添加標籤頁
        self.tab_widget.addTab(self.dataset_viewer, "數據集")
        self.tab_widget.addTab(self.backtest_viewer, "回測結果")
        self.tab_widget.addTab(self.strategy_generator, "策略生成")
        self.tab_widget.addTab(self.simulation_widget, "模擬")
        
        # 連接標籤切換信號
        self.tab_widget.currentChanged.connect(self.on_tab_changed)
        
        # 將標籤頁添加到主布局
        main_layout.addWidget(self.tab_widget)
        
        # 創建狀態欄
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        
        # 顯示初始狀態
        self.status_bar.showMessage("應用程序已準備就緒 - 數據集模式")

        # 連接回測查看器的匯入信號到處理槽 (用於模擬)
        self.backtest_viewer.viewer.import_data_requested.connect(self.handle_import_request)
        # 連接回測查看器的匯入 Code 信號到生成器的槽
        self.backtest_viewer.viewer.import_code_requested.connect(self.strategy_generator.generator.field_selector.add_fields_from_list)
        # 新增：連接策略生成器的匯出信號到模擬器的匯入槽
        self.strategy_generator.generator.strategies_ready_for_simulation.connect(self.simulation_widget.load_strategies_from_generator)

    def on_tab_changed(self, index):
        """處理標籤頁切換事件"""
        if index == 0:
            # 切換到數據集標籤
            status_msg = self.dataset_viewer.get_status_message()
            if status_msg:
                self.status_bar.showMessage(f"數據集模式 - {status_msg}")
            else:
                self.status_bar.showMessage("數據集模式")
        elif index == 1:
            # 切換到回測結果標籤
            status_msg = self.backtest_viewer.get_status_message()
            if status_msg:
                self.status_bar.showMessage(f"回測結果模式 - {status_msg}")
            else:
                self.status_bar.showMessage("回測結果模式")
        elif index == 2:
            # 切換到策略生成標籤
            self.status_bar.showMessage("策略生成模式")
        elif index == 3:
            self.status_bar.showMessage("模擬模式")

    def handle_import_request(self, df: pd.DataFrame):
        """處理從回測結果匯入數據的請求"""
        try:
            self.simulation_widget.load_parameters_from_dataframe(df)
            self.status_bar.showMessage(f"已成功將 {len(df)} 筆數據匯入模擬參數")
            # 切換到模擬標籤頁
            self.tab_widget.setCurrentIndex(3)
        except Exception as e:
            QMessageBox.critical(self, "匯入錯誤", f"將數據匯入模擬時發生錯誤:\n{str(e)}")
            self.status_bar.showMessage(f"匯入模擬參數失敗: {str(e)}")

# 應用程序入口點
if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    # 設置應用程序樣式
    app.setStyle("Fusion")
    
    # 設置固定淺色調色盤
    palette = QPalette()
    palette.setColor(QPalette.Window, QColor(240, 240, 240))
    palette.setColor(QPalette.WindowText, QColor(0, 0, 0))
    palette.setColor(QPalette.Base, QColor(255, 255, 255))
    palette.setColor(QPalette.AlternateBase, QColor(245, 245, 245))
    palette.setColor(QPalette.Text, QColor(0, 0, 0))
    palette.setColor(QPalette.Button, QColor(240, 240, 240))
    palette.setColor(QPalette.ButtonText, QColor(0, 0, 0))
    palette.setColor(QPalette.Link, QColor(0, 120, 230))
    palette.setColor(QPalette.Highlight, QColor(42, 130, 218))
    palette.setColor(QPalette.HighlightedText, QColor(255, 255, 255))
    
    # 強制使用淺色調色盤
    app.setPalette(palette)
    
    # 檢查必要的資源
    success, message = check_resources()
    if not success:
        QMessageBox.warning(None, "資源缺失", message + "\n部分功能可能無法正常使用。")
    
    # 創建並顯示主窗口
    window = MainWindow()
    window.show()
    
    sys.exit(app.exec())

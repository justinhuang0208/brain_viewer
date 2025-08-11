#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
WorldQuant Brain Toolbox
Integrates dataset browser, backtest analysis, strategy generator, and simulation
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

# Define resource directories - use absolute paths
DATASETS_DIR = os.path.join(SCRIPT_DIR, "datasets")
DATA_DIR = "data"
TEMPLATES_DIR = os.path.join(SCRIPT_DIR, "templates")
ALPHAS_DIR = os.path.join(SCRIPT_DIR, "alphas")

# Resource checking function
def check_resources():
    resources = {
        DATASETS_DIR: "Datasets directory",
        DATA_DIR: "Backtest data directory",
        TEMPLATES_DIR: "Templates directory",
        ALPHAS_DIR: "Alphas output directory"
    }
    
    missing = []
    for path, desc in resources.items():
        if not os.path.exists(path):
            try:
                os.makedirs(path)
                print(f"Created {desc} ({path})")
            except:
                missing.append(f"{desc} ({path})")
    
    if missing:
        return False, f"Required resources not found: {', '.join(missing)}"
    return True, "Resources OK"

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
        """Get current status bar message"""
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
        """Get current status bar message"""
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
        """Get current status bar message"""
        return "Strategy Generator Mode"

from simulation import SimulationWidget

# Main application window
class MainWindow(QMainWindow):
    def __init__(self):
        super(MainWindow, self).__init__()
        self.setWindowTitle("WorldQuant Brain Toolbox")
        self.setMinimumSize(1280, 900)
        
        # 創建中央窗口和主布局
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)  # 減少邊距
        
        # Create tabs
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
        
        # Create widgets for datasets, backtests, generator, and simulation
        self.dataset_viewer = DatasetViewerWidget()
        self.backtest_viewer = BacktestViewerWidget()
        self.strategy_generator = StrategyGeneratorWidget()
        self.simulation_widget = SimulationWidget()
        
        # Add tabs
        self.tab_widget.addTab(self.dataset_viewer, "Datasets")
        self.tab_widget.addTab(self.backtest_viewer, "Backtests")
        self.tab_widget.addTab(self.strategy_generator, "Strategy Generator")
        self.tab_widget.addTab(self.simulation_widget, "Simulation")
        
        # 連接標籤切換信號
        self.tab_widget.currentChanged.connect(self.on_tab_changed)
        
        # 將標籤頁添加到主布局
        main_layout.addWidget(self.tab_widget)
        
        # Create status bar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        
        # Initial status
        self.status_bar.showMessage("App ready - Datasets Mode")

        # 連接回測查看器的匯入信號到處理槽 (用於模擬)
        self.backtest_viewer.viewer.import_data_requested.connect(self.handle_import_request)
        # 連接回測查看器的匯入 Code 信號到生成器的槽
        self.backtest_viewer.viewer.import_code_requested.connect(self.strategy_generator.generator.selected_fields_widget.add_fields)
        # 新增：連接策略生成器的匯出信號到模擬器的匯入槽
        self.strategy_generator.generator.strategies_ready_for_simulation.connect(self.simulation_widget.load_strategies_from_generator)
        # 新增：連接數據集查看器的匯出信號到生成器的匯入槽
        self.dataset_viewer.viewer.fields_selected_for_generator.connect(self.strategy_generator.generator.selected_fields_widget.add_fields_from_list)

    def on_tab_changed(self, index):
        """Handle tab change"""
        if index == 0:
            status_msg = self.dataset_viewer.get_status_message()
            if status_msg:
                self.status_bar.showMessage(f"Datasets Mode - {status_msg}")
            else:
                self.status_bar.showMessage("Datasets Mode")
        elif index == 1:
            status_msg = self.backtest_viewer.get_status_message()
            if status_msg:
                self.status_bar.showMessage(f"Backtests Mode - {status_msg}")
            else:
                self.status_bar.showMessage("Backtests Mode")
        elif index == 2:
            self.status_bar.showMessage("Strategy Generator Mode")
        elif index == 3:
            self.status_bar.showMessage("Simulation Mode")

    def handle_import_request(self, df: pd.DataFrame):
        """Handle request to import data from backtests into simulation"""
        try:
            self.simulation_widget.load_parameters_from_dataframe(df)
            self.status_bar.showMessage(f"Successfully imported {len(df)} rows into simulation parameters")
            # 切換到模擬標籤頁
            self.tab_widget.setCurrentIndex(3)
        except Exception as e:
            QMessageBox.critical(self, "Import Error", f"Error importing data into simulation:\n{str(e)}")
            self.status_bar.showMessage(f"Failed to import simulation parameters: {str(e)}")

# 應用程序入口點
if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    # Application style
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
    
    # Force light palette
    app.setPalette(palette)
    
    # Check required resources
    success, message = check_resources()
    if not success:
        QMessageBox.warning(None, "Missing Resources", message + "\nSome features may not work properly.")
    
    # Create and show main window
    window = MainWindow()
    window.show()
    
    sys.exit(app.exec())

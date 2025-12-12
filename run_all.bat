@echo off
title DFS Launcher
echo ==================================================
echo      Distributed File System - One-Click Run
echo ==================================================

echo [1/3] Ensuring ports are clear...
python cleanup_ports.py

echo [2/3] Starting Cluster (Master + Nodes)...
start cmd /k python start_cluster.py

echo Waiting for systems to initialize...
timeout /t 3 /nobreak >nul

echo [3/3] Launching Client GUI...
python client_app.py

echo.
echo Application Closed.
pause

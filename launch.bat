@echo off
echo Starting SailStream Wizard...
echo.
set FYNE_RENDER=software
set CGO_ENABLED=0
cd /d "C:\Users\hawka\SailStream\platforms\pc"
go run wizarzd.go
pause
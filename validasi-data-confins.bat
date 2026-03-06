@echo off
python "%~dp0validasi-data-coreaccount.py"
python "%~dp0validasi-data-customer.py"
python "%~dp0validasi-data-customerpersonal.py"
python "%~dp0validasi-data-custcorporate.py"
python "%~dp0validasi-data-custcorpmanagement.py"
pause
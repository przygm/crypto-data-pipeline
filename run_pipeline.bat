@echo off
cd /d C:\programs\crypto-data-pipeline

echo. >> log.txt
echo ========================================== >> log.txt
echo START PIPELINE: %date% %time% >> log.txt
echo ========================================== >> log.txt

echo [1/3] Running Python Ingestion... >> log.txt
python ingest_crypto.py >> log.txt 2>&1

echo ------------------------------------------ >> log.txt
echo [2.0/3] Cleaning dbt artifacts... >> log.txt
:: DODAJ TO: Czyści folder target i stare wersje modeli z pamięci
call dbt clean >> log.txt 2>&1

echo [2.1/3] Running dbt models... >> log.txt
:: Używamy 'call', żeby skrypt kontynuował pracę po wykonaniu komendy dbt
call dbt run >> log.txt 2>&1

echo ------------------------------------------ >> log.txt
echo [3/3] Running dbt tests... >> log.txt
call dbt test >> log.txt 2>&1

echo ========================================== >> log.txt
echo END PIPELINE: %date% %time% >> log.txt
echo ************************************************************ >> log.txt
echo. >> log.txt
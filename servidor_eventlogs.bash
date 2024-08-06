#!/bin/bash
cd /eventlogs
while :
do
	kill pgrep -f "python3 /eventlogs/nome_do_seu_script_principal.py"
	echo "Iniciando eventlogs"
	python3 /eventlogs/nome_do_seu_script_principal.py >> /eventlogs/log_do_eventlogs.log
	sleep 5
done
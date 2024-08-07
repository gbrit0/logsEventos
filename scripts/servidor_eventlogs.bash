#!/bin/bash
cd /eventlogs
while :
do
	# kill pgrep -f "python3 /eventlogs/processarSolicitacoesDeLogs.py"
	echo "Iniciando eventlogs"
	python3 /eventlogs/processarSolicitacoesDeLogs.py >> /eventlogs/log_do_eventlogs.log
	sleep 1800
done
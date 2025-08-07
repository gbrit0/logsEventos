#!/bin/bash
cd /eventlogs
while :
do
	# kill pgrep -f "python3 /eventlogs/processarSolicitacoesDeLogs.py"
	printf "\n$(date) Iniciando eventlogs\n"
	source /eventlogs/venv/bin/activate
	python3 /eventlogs/processarSolicitacoesDeLogs.py >> /eventlogs/log_do_eventlogs.log
	printf "$(date) Fim da execução\n"
	sleep 1800
done
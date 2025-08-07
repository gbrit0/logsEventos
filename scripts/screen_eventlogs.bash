#!/bin/bash
screen -XS eventlogs quit
screen -dmS eventlogs 
screen -S eventlogs -p 0 -X stuff 'bash /eventlogs/scripts/servidor_eventlogs.bash
'
#!/bin/sh
# Entrypoint for the main trading app service
exec python main.py --mode ${EXECUTION__MODE:-paper}

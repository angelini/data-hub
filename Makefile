.PHONY: reset generate engine web

reset:
	psql -U postgres -d dh -a -1 -v ON_ERROR_STOP=1 -f init.sql

generate: reset
	python core/data.py

engine:
	ipython -i core/engine.py

web: export FLASK_APP=web
web: export FLASK_ENV=development
web:
	flask run

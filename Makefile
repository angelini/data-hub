.PHONY: check-venv
.PHONY: install-uikit install-datatables install-python-deps install
.PHONY: reset generate engine web

UIKIT_VERSION = "3.3.7"
DATATABLES_VERSION = "1.10.20"

check-venv:
ifndef VIRTUAL_ENV
	$(error virtualenv is not activated)
endif

install-uikit:
	curl -s -L -o uikit.tar.gz "https://github.com/uikit/uikit/archive/v${UIKIT_VERSION}.tar.gz"
	tar xzf uikit.tar.gz
	mkdir -p web/static/uikit
	mv "uikit-${UIKIT_VERSION}/dist/css/uikit.css" web/static/uikit/
	mv "uikit-${UIKIT_VERSION}/dist/js/uikit.js" web/static/uikit/
	mv "uikit-${UIKIT_VERSION}/dist/js/uikit-icons.js" web/static/uikit/
	rm -r "uikit-${UIKIT_VERSION}"
	rm uikit.tar.gz

install-datatables:
	curl -s -L -o datatables.tar.gz "https://github.com/DataTables/DataTables/archive/${DATATABLES_VERSION}.tar.gz"
	tar xzf datatables.tar.gz
	mkdir -p web/static/datatables/
	mv "DataTables-${DATATABLES_VERSION}/media/css/dataTables.uikit.css" web/static/datatables/
	mv "DataTables-${DATATABLES_VERSION}/media/js/jquery.js" web/static/datatables/
	mv "DataTables-${DATATABLES_VERSION}/media/js/jquery.dataTables.js" web/static/datatables/
	mv "DataTables-${DATATABLES_VERSION}/media/js/dataTables.uikit.js" web/static/datatables/
	rm -r "DataTables-${DATATABLES_VERSION}"
	rm datatables.tar.gz

install-python-deps: check-venv
	pip install -r requirements.txt

install: install-uikit install-datatables install-python-deps

reset:
	psql -h localhost -U postgres -d dh -a -1 -v ON_ERROR_STOP=1 -f init.sql

generate: reset
	python core/data.py

engine:
	ipython -i core/engine.py

web: export FLASK_APP=web
web: export FLASK_ENV=development
web:
	flask run

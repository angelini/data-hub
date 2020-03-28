.PHONY: check-venv
.PHONY: install-uikit install-datatables install-d3 install-dagre install-python-deps install
.PHONY: reset example engine web

UIKIT_VERSION = "3.3.7"
DATATABLES_VERSION = "1.10.20"
D3_VERSION = "5.15.0"
DAGRE_VERSION = "0.5.0"

check-venv:
ifndef VIRTUAL_ENV
	$(error virtualenv is not activated)
endif

check-web:
ifneq ($(shell curl -s -I "http://localhost:5000/" | head -n 1), HTTP/1.0 302 FOUND)
	$(error server is not running)
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

install-d3:
	curl -s -L -o d3.zip "https://github.com/d3/d3/releases/download/v${D3_VERSION}/d3.zip"
	unzip d3.zip -d "d3-${D3_VERSION}"
	mkdir -p web/static/d3/
	mv "d3-${D3_VERSION}/d3.js" web/static/d3/
	rm -r "d3-${D3_VERSION}"
	rm d3.zip

install-dagre:
	curl -s -L -o dagre.tar.gz "https://github.com/dagrejs/dagre-d3/archive/v${DAGRE_VERSION}.tar.gz"
	tar xzf dagre.tar.gz
	mkdir -p web/static/dagre/
	mv "dagre-d3-${DAGRE_VERSION}/dist/dagre-d3.js" web/static/dagre/
	rm -r "dagre-d3-${DAGRE_VERSION}"
	rm dagre.tar.gz

install-python-deps: check-venv
	pip install -r requirements.txt

install: install-uikit install-datatables install-d3 install-dagre install-python-deps

reset: export PYTHONPATH=.
reset:
	psql -h localhost -U postgres -d dh -a -1 -v ON_ERROR_STOP=1 -f init.sql
	python core/reset.py

example: export PYTHONPATH=.
example: check-web reset
	python examples/first.py

engine:
	ipython -i core/engine.py

web: export FLASK_APP=web
web: export FLASK_ENV=development
web:
	flask run

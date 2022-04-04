# ----------------------------------
#          INSTALL & TEST
# ----------------------------------
install_requirements:
	@pip install -r requirements.txt

check_code:
	@flake8 *.py

black:
	@black *.py

test:
	@coverage run -m pytest tests/*.py
	@coverage report -m --omit=$(VIRTUAL_ENV)/lib/python*

ftest:
	@Write me

clean:
	@rm -f */version.txt
	@rm -f .coverage
	@rm -fr */*/__pycache__ */__pycache__ */*.pyc __pycache__
	@rm -fr build dist
	@rm -fr *.dist-info
	@rm -fr *.egg-info */*.egg-info
	@rm -fr */.ipynb_checkpoints */*.ipynb_checkpoints

install:
	@pip install . -U

all: clean install test black check_code

uninstal:
	@python setup.py install --record files.txt
	@cat files.txt | xargs rm -rf
	@rm -f files.txt

count_lines:
	@find ./ -name '*.py' -exec  wc -l {} \; | sort -n| awk \
        '{printf "%4s %s\n", $$1, $$2}{s+=$$0}END{print s}'
	@echo ''
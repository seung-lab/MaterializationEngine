FROM gcr.io/neuromancer-seung-import/pychunkedgraph:graph-tool_dracopy

ENV UWSGI_INI /app/uwsgi.ini

COPY dev_requirements.txt /app/.
RUN python -m pip install --upgrade pip
RUN pip install -r dev_requirements.txt
COPY . /app
RUN chmod +x /entrypoint.sh
WORKDIR /app
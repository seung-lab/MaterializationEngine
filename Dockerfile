FROM gcr.io/neuromancer-seung-import/pychunkedgraph:graph-tool_dracopy

ENV UWSGI_INI ./uwsgi.ini

COPY requirements.txt /app/.
RUN pip install -r requirements.txt
COPY . /app
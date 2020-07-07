FROM gcr.io/neuromancer-seung-import/pychunkedgraph:vpinky-prod.1.5

ENV UWSGI_INI ./uwsgi.ini

COPY requirements.txt /app/.
RUN pip install -r requirements.txt
COPY . /app
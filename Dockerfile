FROM gcr.io/neuromancer-seung-import/pychunkedgraph:vpinky-prod.1.5

COPY requirements.txt /app
RUN pip install --no-cache-dir --process-dependency-links -r requirements.txt
COPY . /app
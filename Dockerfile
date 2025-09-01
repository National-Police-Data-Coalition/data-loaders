FROM python:3.14.0b3-slim-bookworm

# Install dependencies
RUN pip3 install --upgrade pip
COPY requirements/ requirements/
RUN pip3 install -r requirements/requirements.txt

CMD ["./run_loader.sh"]
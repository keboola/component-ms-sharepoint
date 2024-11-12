FROM python:3.9-slim
ENV PYTHONIOENCODING utf-8

# install gcc to be able to build packages - e.g. required by regex, dateparser, also required for pandas
RUN apt-get update && apt-get install -y build-essential

RUN pip install flake8

RUN pip install -r /code/requirements.txt

COPY . /code/

WORKDIR /code/


CMD ["python", "-u", "/code/src/component.py"]

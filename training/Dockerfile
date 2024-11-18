FROM python:3.12

ENV VIRTUAL_ENV="/opt/venv"
RUN python -m venv ${VIRTUAL_ENV}
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"
ENV PUTHONBUFFERED True

WORKDIR /home
COPY requirements.txt .
RUN python -m pip install pip --upgrade
RUN python -m pip install -r requirements.txt
COPY training_ros.py .
COPY key1.json .

CMD [ "python","/home/training_ros.py" ]
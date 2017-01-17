FROM python:3.5-slim

ENV LUIGI_HOME /etc/luigi

RUN apt-get update

RUN apt-get install -y libblas-dev liblapack-dev build-essential gfortran git \
                       curl mdbtools  openjdk-7-jdk postgresql-client \
                       postgis unzip wget


ADD requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

RUN mkdir  $LUIGI_HOME

## Variables para el usuario
ARG WORKER_USER
ARG WORKER_UID

ARG GITHUB_REPO
ARG GITHUB_USER

## Creamos al usuario que ejecuta el worker en el grupo `users`
RUN useradd -m -s /bin/bash -N -u $WORKER_UID $WORKER_USER

ADD .boto /home/$WORKER_USER/.boto
ADD .boto /etc/boto.cfg

RUN chown $WORKER_USER:users /home/$WORKER_USER/.boto

RUN mkdir /home/$WORKER_USER/.ssh

## Agregamos la llave para que pueda descargar github
ADD id_rsa /home/$WORKER_USER/.ssh/id_rsa

RUN touch /home/$WORKER_USER/.ssh/known_hosts

RUN ssh-keyscan -T 60 github.com >> /home/$WORKER_USER/.ssh/known_hosts

## Ajustamos los permisos
RUN chown -R $WORKER_USER:users -R /home/$WORKER_USER

USER $WORKER_USER

WORKDIR /home/$WORKER_USER

## Setting pyenv
RUN git clone git://github.com/yyuu/pyenv.git .pyenv
ENV PYENV_ROOT /home/$WORKER_USER/.pyenv
ENV PATH $PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH


RUN echo git@github.com:$GITHUB_USER/$GITHUB_REPO.git

RUN git clone git@github.com:$GITHUB_USER/$GITHUB_REPO.git

WORKDIR /home/$WORKER_USER/$GITHUB_REPO

## Install the correct version of python
RUN $PYENV_ROOT/bin/pyenv install $(cat .python-version)

## Install requirements
RUN make prepare


CMD [ "/bin/sh",  "-c", "while true; do echo hello world; sleep 1; done"]

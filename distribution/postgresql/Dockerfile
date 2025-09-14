FROM postgres:17.5
## RUN mkdir /opt/postgresql/data
## RUN mkdir /opt/postgresql/sql
## COPY src/main/resources/sql_scripts/* /opt/postgresql/sql
## UN psql -f /sql/create_user.sql
## RUN psql -U user -W 123 -f create_db.sql
## RUN psql -U user -W 123 -d school_ats -f create_tables.sql

##sudo -u postgres initdb -Uroot --pwprompt --encoding=UTF8 --auth=password -D /var/local/pgsql/data
##RUN psql -d postgres -Upostgres --command="CREATE DATABASE seleznyov ENCODING 'UTF-8' LC_COLLATE 'ru_RU.UTF-8' LC_CTYPE 'ru_RU.UTF-8'"
##RUN psql -d postgres -Upostgres --command="CREATE USER root WITH SUPERUSER ENCRYPTED PASSWORD 'rootagain'"

RUN sed -i '/ru_RU.UTF-8/s/^# //g' /etc/locale.gen && \
    locale-gen
ENV LANG=ru_RU.utf8
ENV LANGUAGE=ru_RU:ru
ENV LC_ALL=ru_RU.UTF-8
##ENV POSTGRES_USER=seleznyoviyu
##ENV POSTGRES_PASSWORD='<jufncndj&Ghjwdtnfybt$16051979'
##ENV POSTGRES_DB=kstories

COPY init.sql /docker-entrypoint-initdb.d/

##ENV POSTGRES_USER seleznyov
##ENV POSTGRES_PASSWORD seleznyov
##ENV POSTGRES_DB seleznyov
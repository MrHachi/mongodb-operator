FROM mongo:8.3.7

COPY ./mongod.conf /etc/mongod.conf
COPY ./scripts/bootstrap-rs.js /bootstrap/bootstrap-rs.js
COPY ./scripts/bootstrap-admin.js /bootstrap/bootstrap-admin.js

COPY ./init.sh /
RUN chmod 555 /init.sh

CMD [ "/init.sh" ]

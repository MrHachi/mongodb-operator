#!/bin/sh
set -eu

cp /etc/kf/keyfile /etc/keyfile
chown mongodb:mongodb /etc/keyfile
chmod 400 /etc/keyfile

mongod --config /etc/mongod.conf &
pid=$!

# Wait until MongoDB is accepting connections
until mongosh --quiet --eval 'db.adminCommand({ ping: 1 })' >/dev/null 2>&1
do
    sleep 1
done

# Only the first StatefulSet pod initializes the replica set
case "$HOSTNAME" in
    *-0)
        # Only initiates RS mode if rs.status() fails
        mongosh /bootstrap/bootstrap-rs.js

        # Create the admin user if it doesn't exist
        mongosh /bootstrap/bootstrap-admin.js
        ;;
esac

# Wait
wait "$pid"

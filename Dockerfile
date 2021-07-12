FROM statsd/statsd:latest
WORKDIR /usr/src/app
# Copy the backend source code instead of an installation since there are no dependencies
COPY ./kafka-backend.js ./backends/kafka-backend.js
# Start statsd
CMD [ "node", "stats.js", "config.js" ]

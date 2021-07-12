/*
 * Flush stats to kafka (http://kafka.apache.org/).
 *
 * Currently, only publishing metrics via the kafka rest proxy is supported.
 * A nice enhancement would be to allow publishing via the kafka binary protocol as well.
 *
 * To enable this backend, include 'kafka-backend' in the backends
 * configuration array:
 *
 *   backends: ['kafka-backend']
 *
 * This backend supports the following config options:
 *
 *   restProxyUrl: comma-separate list of broker nodes
 *
 */

let util = require('util'),
    http = require('http'),
    https = require('https'),
    url = require('url');

let debug;
let restProxyUrl;
let kafkaTopic;

// prefix configuration
let globalPrefix;
let prefixCounter;
let prefixTimer;
let prefixGauge;
let prefixSet;
let prefixStats;

// set up namespaces
let legacyNamespace = true;
let globalNamespace = [];
let counterNamespace = [];
let timerNamespace = [];
let gaugesNamespace = [];
let setsNamespace = [];

let kafkaStats = {};

function metric(path, val, timestamp, type) {
    let pathParts = path.split(";")
    // Metric name
    let metric = pathParts.find(p => p.indexOf("=") === -1);
    let thisMetric = this;
    // Tags
    pathParts
      .filter(p => p.indexOf('=') !== -1)
      .map(p => p.split("=", 2))
      .filter(ts => ts.length == 2)
      .forEach(ts => thisMetric[ts[0]] = ts[1]);
    this.metric = metric != null ? metric : "undefined";
    this.value = val;
    this.timestamp = timestamp;
    this.type = type;
}

let post_stats = function kafka_publish_stats(metricsArray) {
    let last_flush = kafkaStats.last_flush || 0;
    let last_exception = kafkaStats.last_exception || 0;
    let flush_time = kafkaStats.flush_time || 0;
    let flush_length = kafkaStats.flush_length || 0;

    if (restProxyUrl) {
        try {
            let starttime = Date.now();
            let ts = Math.round(new Date().getTime() / 1000);
            let namespace = globalNamespace.concat(prefixStats).join('.');

            metricsArray.push(new metric(namespace + '.kafkaStats.last_exception', last_exception, ts, "gauge"));
            metricsArray.push(new metric(namespace + '.kafkaStats.last_flush', last_flush, ts, "gauge"));
            metricsArray.push(new metric(namespace + '.kafkaStats.flush_time', flush_time, ts, "timer"));
            metricsArray.push(new metric(namespace + '.kafkaStats.flush_length', flush_length, ts, "timer"));

            let kafkaMetricsObject = {
                'records': metricsArray.map(m => ({ 'value' :  m }))
            };
            let data = JSON.stringify(kafkaMetricsObject);

            let options = url.parse(restProxyUrl.concat('/').concat(kafkaTopic));
            options.method = 'POST';
            options.headers = {
                'Content-Length': data.length,
                'Content-Type': 'application/vnd.kafka.json.v2+json',
                'Accept': 'application/json'
            };

            let req;

            if (options.protocol === 'https:') {
                req = https.request(options, function (res) {
                    res.setEncoding('utf8');
                });
            } else {
                req = http.request(options, function (res) {
                    res.setEncoding('utf8');
                    res.on('data', function (payload) {
                        for (let offset in payload.offsets) {
                            if (offset.error) {
                                console.log('Error: ' + offset.error + ' Code: ' + offset.error_code);
                            }
                        }
                    });
                });
            }

            req.on('error', function (e) {
                console.log('problem with request: ' + e.message);
                kafkaStats.last_exception = Math.round(new Date().getTime() / 1000);
            });

            req.on('close', function () {
                kafkaStats.flush_time = (Date.now() - starttime);
                kafkaStats.flush_length = data.length;
                kafkaStats.last_flush = Math.round(new Date().getTime() / 1000);
            });

            req.write(data);
            req.end();

        } catch (e) {
            if (debug) {
                util.log(e);
            }
            kafkaStats.last_exception = Math.round(new Date().getTime() / 1000);
        }
    }
};

let flush_stats = function kafka_flush(ts, metrics) {
    let starttime = Date.now();
    let metricsArray = [];
    let numStats = 0;
    let key;
    let timer_data_key;
    let counters = metrics.counters;
    let gauges = metrics.gauges;
    let sets = metrics.sets;
    let counter_rates = metrics.counter_rates;
    let timer_data = metrics.timer_data;
    let statsd_metrics = metrics.statsd_metrics;

    for (key in counters) {
        let namespace = counterNamespace.concat(key);
        let value = counters[key];
        let valuePerSecond = counter_rates[key]; // pre-calculated "per second" rate

        if (legacyNamespace === true) {
            metricsArray.push(new metric(namespace.join("."), valuePerSecond, ts, "gauge"));
            metricsArray.push(new metric('stats_counts.' + key, value, ts, "count"));
        } else {
            metricsArray.push(new metric(namespace.concat('rate').join("."), valuePerSecond, ts, "gauge"));
            metricsArray.push(new metric(namespace.concat('count').join("."), value, ts, "count"));
        }
    }

    for (key in timer_data) {
        let namespace = timerNamespace.concat(key);
        let the_key = namespace.join('.');
        for (timer_data_key in timer_data[key]) {
            if (typeof (timer_data[key][timer_data_key]) === 'number') {
                metricsArray.push(new metric(the_key + '.' + timer_data_key, timer_data[key][timer_data_key], ts, "timer"));
            } else {
                for (let timer_data_sub_key in timer_data[key][timer_data_key]) {
                    let mpath = the_key + '.' + timer_data_key + '.' + timer_data_sub_key;
                    let mval = timer_data[key][timer_data_key][timer_data_sub_key];
                    if (debug) {
                        util.log(mval.toString());
                    }
                    metricsArray.push(new metric(mpath, mval, ts, "timer"));
                }
            }
        }
    }

    for (key in gauges) {
        let namespace = gaugesNamespace.concat(key);
        metricsArray.push(new metric(namespace.join("."), gauges[key], ts, "gauge"));
    }

    for (key in sets) {
        let namespace = setsNamespace.concat(key);
        metricsArray.push(new metric(namespace.join(".") + '.count', sets[key].values().length, ts, "set"));
    }

    //these go into gauges object
    let namespace = globalNamespace.concat(prefixStats);
    if (legacyNamespace === true) {
        metricsArray.push(new metric(prefixStats + '.numStats', numStats, ts, "count"));
        metricsArray.push(new metric('stats.' + prefixStats + '.kafkaStats.calculationtime', (Date.now() - starttime), ts, "timer"));
        for (key in statsd_metrics) {
            metricsArray.push(new metric('stats.' + prefixStats + '.' + key, statsd_metrics[key], ts, "statsd"));
        }
    } else {
        metricsArray.push(new metric(namespace.join(".") + '.numStats', numStats, ts, "count"));
        metricsArray.push(new metric(namespace.join(".") + '.kafkaStats.calculationtime', (Date.now() - starttime), ts, "timer"));
        for (key in statsd_metrics) {
            let the_key = namespace.concat(key);
            metricsArray.push(new metric(the_key.join("."), statsd_metrics[key], ts, "statsd"));
        }
    }

    post_stats(metricsArray);
};

let backend_status = function kafka_status(writeCb) {
    for (let stat in kafkaStats) {
        writeCb(null, 'kafka', stat, kafkaStats[stat]);
    }
};

exports.init = function kafka_init(startup_time, config, events) {
    debug = config.debug;
    prefixStats = config.prefixStats;
    restProxyUrl = config.restProxyUrl;
    kafkaTopic = config.kafkaTopic;
    config.kafka = config.kafka || {};
    globalPrefix = config.kafka.globalPrefix;
    prefixCounter = config.kafka.prefixCounter;
    prefixTimer = config.kafka.prefixTimer;
    prefixGauge = config.kafka.prefixGauge;
    prefixSet = config.kafka.prefixSet;
    legacyNamespace = config.kafka.legacyNamespace;

    // set defaults for prefixes
    globalPrefix = globalPrefix !== undefined ? globalPrefix : 'stats';
    prefixCounter = prefixCounter !== undefined ? prefixCounter : 'counters';
    prefixTimer = prefixTimer !== undefined ? prefixTimer : 'timers';
    prefixGauge = prefixGauge !== undefined ? prefixGauge : 'gauges';
    prefixSet = prefixSet !== undefined ? prefixSet : 'sets';
    prefixStats = prefixStats !== undefined ? prefixStats : 'statsd';
    legacyNamespace = legacyNamespace !== undefined ? legacyNamespace : true;

    if (legacyNamespace === false) {
        if (globalPrefix !== '') {
            globalNamespace.push(globalPrefix);
            counterNamespace.push(globalPrefix);
            timerNamespace.push(globalPrefix);
            gaugesNamespace.push(globalPrefix);
            setsNamespace.push(globalPrefix);
        }

        if (prefixCounter !== '') {
            counterNamespace.push(prefixCounter);
        }
        if (prefixTimer !== '') {
            timerNamespace.push(prefixTimer);
        }
        if (prefixGauge !== '') {
            gaugesNamespace.push(prefixGauge);
        }
        if (prefixSet !== '') {
            setsNamespace.push(prefixSet);
        }
    } else {
        globalNamespace = ['stats'];
        counterNamespace = ['stats'];
        timerNamespace = ['stats', 'timers'];
        gaugesNamespace = ['stats', 'gauges'];
        setsNamespace = ['stats', 'sets'];
    }

    kafkaStats.last_flush = startup_time;
    kafkaStats.last_exception = startup_time;
    kafkaStats.flush_time = 0;
    kafkaStats.flush_length = 0;

    events.on('flush', flush_stats);
    events.on('status', backend_status);

    return true;
};

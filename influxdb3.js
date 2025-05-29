const _ = require('lodash');
const { InfluxDBClient, Point } = require('@influxdata/influxdb3-client');

module.exports = function (RED) {
    function InfluxDB3Config(n) {
        RED.nodes.createNode(this, n);
        this.name = n.name;
        this.host = n.host;
        this.port = n.port;
        this.database = n.database;
        this.timeout = n.timeout || 10;

        const clientOptions = {
            host: `${this.host}:${this.port}`,
            token: this.credentials.token,
            database: this.database,
            timeout: Math.floor(this.timeout * 1000)
        };

        try {
            this.client = new InfluxDBClient(clientOptions);
        } catch (err) {
            this.error("Failed to create InfluxDB 3 client: " + err.message);
        }

        this.on("close", () => {
            if (this.client) this.client.close();
        });
    }

    RED.nodes.registerType("influxdb3-config", InfluxDB3Config, {
        credentials: {
            token: { type: "password" }
        }
    });

    function addFieldToPoint(point, name, value) {
        if (typeof value === 'number') {
            Number.isInteger(value) ? point.intField(name, value) : point.floatField(name, value);
        } else if (typeof value === 'boolean') {
            point.booleanField(name, value);
        } else if (typeof value === 'string') {
            const intMatch = value.match(/^-?\d+i$/);
            if (intMatch) {
                point.intField(name, parseInt(value.slice(0, -1)));
            } else {
                point.stringField(name, value);
            }
        } else {
            point.stringField(name, String(value));
        }
    }

    function InfluxDB3Out(n) {
        RED.nodes.createNode(this, n);
        this.name = n.name;
        this.measurement = n.measurement;
        this.influxdb = RED.nodes.getNode(n.influxdb);

        if (!this.influxdb?.client) {
            this.error("Missing InfluxDB 3 client");
            return;
        }

        this.on("input", async (msg, send, done) => {
            try {
                const measurement = msg.measurement || this.measurement;
                const db = msg.database || this.influxdb.database;

                if (!measurement) {
                    return done("Missing measurement");
                }

                const buildPoint = (data, tags = {}) => {
                    const point = new Point(measurement);
                    for (const [k, v] of Object.entries(data)) {
                        if (k === 'time') {
                            point.timestamp(v);
                        } else {
                            addFieldToPoint(point, k, v);
                        }
                    }
                    for (const [k, v] of Object.entries(tags)) {
                        point.tag(k, String(v));
                    }
                    return point;
                };

                const payload = msg.payload;
                const points = [];

                if (Array.isArray(payload)) {
                    if (Array.isArray(payload[0])) {
                        payload.forEach(([fields, tags]) => points.push(buildPoint(fields, tags)));
                    } else {
                        points.push(buildPoint(payload[0], payload[1]));
                    }
                } else if (_.isPlainObject(payload)) {
                    points.push(buildPoint(payload));
                } else {
                    const point = new Point(measurement);
                    addFieldToPoint(point, 'value', payload);
                    points.push(point);
                }

                for (const pt of points) {
                    await this.influxdb.client.write(pt.toLineProtocol(), db);
                }

                done();
            } catch (err) {
                msg.influx_error = { errorMessage: err.message };
                done(err);
            }
        });
    }

    RED.nodes.registerType("influxdb3 out", InfluxDB3Out);

    function InfluxDB3Batch(n) {
        RED.nodes.createNode(this, n);
        this.name = n.name;
        this.influxdb = RED.nodes.getNode(n.influxdb);

        if (!this.influxdb?.client) {
            this.error("Missing InfluxDB 3 client");
            return;
        }

        this.on("input", async (msg, send, done) => {
            try {
                const db = msg.database || this.influxdb.database;
                const payload = msg.payload;

                if (!Array.isArray(payload)) {
                    return done(new Error("Payload must be an array"));
                }

                for (const item of payload) {
                    if (!item.measurement || !item.fields) continue;
                    const point = new Point(item.measurement);
                    for (const [k, v] of Object.entries(item.fields)) {
                        addFieldToPoint(point, k, v);
                    }
                    if (item.tags) {
                        for (const [k, v] of Object.entries(item.tags)) {
                            point.tag(k, String(v));
                        }
                    }
                    if (item.timestamp) {
                        point.timestamp(item.timestamp);
                    }
                    await this.influxdb.client.write(point.toLineProtocol(), db);
                }

                done();
            } catch (err) {
                msg.influx_error = { errorMessage: err.message };
                done(err);
            }
        });
    }

    RED.nodes.registerType("influxdb3 batch", InfluxDB3Batch);

    function InfluxDB3In(n) {
        RED.nodes.createNode(this, n);
        this.name = n.name;
        this.query = n.query;
        this.queryType = n.queryType || 'sql';
        this.influxdb = RED.nodes.getNode(n.influxdb);

        if (!this.influxdb?.client) {
            this.error("Missing InfluxDB 3 client");
            return;
        }

        this.on("input", async (msg, send, done) => {
            try {
                const db = msg.database || this.influxdb.database;
                const query = msg.query || this.query;
                const type = msg.queryType || this.queryType;

                if (!query) return done("Missing query");

                const options = { type: type === 'influxql' ? 'influxql' : 'sql' };
                const result = await this.influxdb.client.query(query, db, options);
                const rows = [];
                for await (const r of result) {
                    rows.push(r);
                }

                msg.payload = rows;
                send(msg);
                done();
            } catch (err) {
                msg.influx_error = { errorMessage: err.message };
                done(err);
            }
        });
    }

    RED.nodes.registerType("influxdb3 in", InfluxDB3In);
};
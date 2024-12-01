import { createClient, type InsertResult } from "@clickhouse/client";
import { DateTime } from "luxon";
import fs from "node:fs";
import readline from "node:readline";
const schemaVersion = 3;

function _client() {
	return createClient({
		url: "http://localhost:8123",
		username: "default",
		password: "password",
	});
}

export async function runMigrations() {
	for (const migration of [
		`DROP TABLE IF EXISTS logs_${schemaVersion}`,
		`
CREATE TABLE IF NOT EXISTS logs_${schemaVersion}
(
  _tenantId LowCardinality(String) NOT NULL CODEC(ZSTD(1)),
  _timestamp DateTime64(3) NOT NULL CODEC(DoubleDelta, LZ4),
  _now DateTime64(3) NOT NULL CODEC(ZSTD(1)),
  _ttl DateTime NOT NULL,
  _traceId String CODEC(ZSTD(1)),
  _spanId String CODEC(ZSTD(1)),
  _parentSpanId String CODEC(ZSTD(1)),
  _startTime DateTime64(3) CODEC(DoubleDelta, LZ4),
  _endTime DateTime64(3) CODEC(DoubleDelta, LZ4),
  _duration UInt32 Codec(T64, LZ4),
  _source String NOT NULL CODEC(ZSTD(1)),
  \`string.names\` Array(String) CODEC(ZSTD(2)),
  \`string.values\` Array(String) CODEC(ZSTD(2)),
  \`number.names\` Array(String) CODEC(ZSTD(2)),
  \`number.values\` Array(Float64) CODEC(ZSTD(2)),
  \`boolean.names\` Array(String) CODEC(ZSTD(2)),
  \`boolean.values\` Array(UInt8) CODEC(ZSTD(2)),
  INDEX idx_tenant_bloom _tenantId TYPE bloom_filter GRANULARITY 4
) ENGINE = MergeTree()
PARTITION BY toDate(_timestamp)
ORDER BY (_tenantId, _traceId, _timestamp)
TTL _ttl DELETE`,
		`CREATE TABLE IF NOT EXISTS logs_json (
    data JSON(),
	_tenantId LowCardinality(String) NOT NULL CODEC(ZSTD(1)),
    _timestamp DateTime64(3) NOT NULL CODEC(DoubleDelta, LZ4),
	_now DateTime64(3) NOT NULL CODEC(ZSTD(1)),
  _ttl DateTime NOT NULL,
  INDEX idx_tenant_bloom _tenantId TYPE bloom_filter GRANULARITY 4
) ENGINE = MergeTree()
PARTITION BY toDate(_timestamp)
ORDER BY (_tenantId, _timestamp)`,
	]) {
		try {
			await _client().command({
				query: migration,
				clickhouse_settings: {
					allow_experimental_json_type: 1,
				},
			});
		} catch (e) {
			console.error(migration, e);
			throw new Error(`Migration failed: ${e}`);
		}
	}
}

export function parseRows(rows: any[]) {
	const processedRows = rows.map((row) => {
		const paths: {
			string: Record<string, string>;
			number: Record<string, number>;
			boolean: Record<string, number>;
		} = {
			string: {},
			number: {},
			boolean: {},
		};

		function processObject(obj: any, currentPath = "") {
			for (const [key, value] of Object.entries(obj)) {
				const newPath = currentPath ? `${currentPath}.${key}` : key;

				if (Array.isArray(value)) {
					continue;
				}

				if (typeof value === "object" && value !== null) {
					processObject(value, newPath);
				} else {
					const type = typeof value;
					if (type === "string" || type === "number" || type === "boolean") {
						paths[type][newPath] = value as any;
					}
				}
			}
		}

		processObject(row);
		return paths;
	});

	return processedRows;
}

const writeRowsToClickhouse = async (
	userId: string,
	rows: ReturnType<typeof parseRows>,
): Promise<InsertResult> => {
	// Write to ClickHouse
	const now = new Date();
	const ttl = new Date(now.getTime() + 1 * 24 * 60 * 60 * 1000); // 1 day TTL

	const clickhouseRows = rows.map((row) => {
		// Find trace/span related fields if they exist
		const traceId = row.string.traceId || row.string.trace_id;
		const spanId = row.string.spanId || row.string.span_id;
		const parentSpanId = row.string.parentSpanId || row.string.parent_span_id;
		// TOOD: handle more cases
		const startTime = row.string.startTime || row.string.start_time;
		const endTime = row.string.endTime || row.string.end_time;
		const duration = row.number.duration;

		const timestamp = row.string.timestamp || row.string.ts || row.string.dt;
		const parsedTimestamp = timestamp
			? DateTime.fromISO(timestamp).toJSDate()
			: null;
		return {
			_tenantId: userId,
			_timestamp: parsedTimestamp || now,
			_now: now,
			_ttl: ttl,
			_traceId: traceId || null,
			_spanId: spanId || null,
			_parentSpanId: parentSpanId || null,
			_startTime: startTime ? new Date(startTime) : null,
			_endTime: endTime ? new Date(endTime) : null,
			_duration: duration || null,
			_source: [
				"json",
				"logs",
				"metrics",
				"traces",
				"events",
				"telemetry",
				"monitoring",
				"analytics",
				"system",
				"app",
			][Math.floor(Math.random() * 10)],
			"string.names": Object.keys(row.string),
			"string.values": Object.values(row.string),
			"number.names": Object.keys(row.number),
			"number.values": Object.values(row.number),
			"boolean.names": Object.keys(row.boolean),
			"boolean.values": Object.values(row.boolean),
		};
	});

	return await _client().insert({
		table: `logs_${schemaVersion}`,
		values: clickhouseRows,
		format: "JSONEachRow",
		clickhouse_settings: {
			date_time_input_format: "best_effort",
			date_time_output_format: "iso",
		},
	});
};

await runMigrations();

const writeJsonRowsToClickhouse = async (
	userId: string,
	rows: any[],
): Promise<InsertResult> => {
	// Write to ClickHouse
	const now = new Date();
	const ttl = new Date(now.getTime() + 1 * 24 * 60 * 60 * 1000); // 1 day TTL

	const clickhouseRows = rows.map((row) => {
		// Find trace/span related fields if they exist
		const traceId = row.traceId || row.trace_id;
		const spanId = row.spanId || row.span_id;
		const parentSpanId = row.parentSpanId || row.parent_span_id;
		// TOOD: handle more cases
		const startTime = row.startTime || row.start_time;
		const endTime = row.endTime || row.end_time;
		const duration = row.duration;

		const timestamp = row.timestamp || row.ts || row.dt;
		const parsedTimestamp = timestamp
			? DateTime.fromISO(timestamp).toJSDate()
			: null;
		return {
			_tenantId: userId,
			_timestamp: parsedTimestamp || now,
			_now: now,
			_ttl: ttl,
			data: row,
		};
	});

	return await _client().insert({
		table: "logs_json",
		values: clickhouseRows,
		format: "JSONEachRow",
		clickhouse_settings: {
			date_time_input_format: "best_effort",
			date_time_output_format: "iso",
		},
	});
};

const processLogFile = async (filePath: string) => {
	const fileStream = fs.createReadStream(filePath);
	const rl = readline.createInterface({
		input: fileStream,
		crlfDelay: Number.POSITIVE_INFINITY,
	});

	let lines = [];
	for await (const line of rl) {
		lines.push(JSON.parse(line));
		if (lines.length <= 10000) {
			continue;
		}
		const rows = parseRows(lines);
		await writeRowsToClickhouse("123", rows);
		// await writeJsonRowsToClickhouse("456", lines);
		lines = []; // Reset array for next batch
	}
};

const logFiles = fs
	.readdirSync("logs")
	.filter((file) => file.endsWith(".json"));
for (const logFile of logFiles) {
	console.log(`Processing ${logFile}`);
	await processLogFile(`logs/${logFile}`);
}

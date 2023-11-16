import _ from 'lodash'
import axios from 'axios'
import prometheus from 'parse-prometheus-text-format'
import {setTimeout} from 'node:timers/promises'
import {readFile} from 'fs/promises'

import {Client} from '@containers-js/containerd'
import {Client as Elastic} from '@elastic/elasticsearch'

const hostname = await (readFile('/etc/k8s-hostname', 'utf8').then(h => h.trim()).catch(e => "unknown"))
const elasticClient = new Elastic({
	node: {
		url: new URL(process.env.ELASTIC_URL)
	},
	auth: {
		username: process.env.ELASTIC_USER,
		password: process.env.ELASTIC_PASS
	},
	tls: {
		rejectUnauthorized: false,
//		ca: ''
	}
})
const containerClient = new Client(process.env.CONTAINERD_SOCKET, 'k8s.io')

function parseMetric(metrics, name, id) {
	let v = Number(metrics[name][id]?.value || 0)
	return v > Number.MAX_SAFE_INTEGER ? 0 : v
}

async function getMetrics() {
	const timestamp = new Date()
	const res = await containerClient.containers.list({})
	const tasks = (await containerClient.tasks.list({})).tasks.map(t => t.id)

	const { data } = await axios.get(process.env.METRICS_URL)
	const p = prometheus(data)
	let metrics = _.mapValues(
		_.keyBy(p.filter(x => x.name.startsWith('container_')), 'name'),
		x => _.keyBy(x.metrics, 'labels.container_id')
	)

	return res.containers.map(y => {
		let spec = JSON.parse(y.spec.value)
		let metadata = JSON.parse((y.extensions['io.cri-containerd.container.metadata'] || y.extensions['io.cri-containerd.sandbox.metadata']).value)

		let memUsage = parseMetric(metrics, 'container_memory_usage_bytes', y.id);
		let memLimit = parseMetric(metrics, 'container_memory_usage_limit_bytes', y.id);
		return {
			"@timestamp": timestamp,
			"cpu": {
				"throttled_time": parseMetric(metrics, 'container_cpu_throttled_usec_microseconds', y.id) * 1000,
				"usage_system": parseMetric(metrics, 'container_cpu_system_usec_microseconds', y.id) * 1000,
				"usage_user": parseMetric(metrics, 'container_cpu_user_usec_microseconds', y.id) * 1000,
				"usage_total": parseMetric(metrics, 'container_cpu_usage_usec_microseconds', y.id) * 1000
			},
			"mem": {
				"active_anon": parseMetric(metrics, 'container_memory_active_anon_bytes', y.id),
				"inactive_anon": parseMetric(metrics, 'container_memory_inactive_anon_bytes', y.id),
				"active_file": parseMetric(metrics, 'container_memory_active_file_bytes', y.id),
				"inactive_file": parseMetric(metrics, 'container_memory_inactive_file_bytes', y.id),
				"limit": memLimit,
				"usage": memUsage,
				"usage_pct": memUsage / memLimit,
				"pgfault": parseMetric(metrics, 'container_memory_pgfault_bytes', y.id),
				"pgmajfault": parseMetric(metrics, 'container_memory_pgmajfault_bytes', y.id),
				"unevictable": parseMetric(metrics, 'container_memory_unevictable_bytes', y.id)
			},
			"io": {
				"read": parseMetric(metrics, 'container_io_rios_total', y.id),
				"read_bytes": parseMetric(metrics, 'container_io_rbytes_bytes', y.id),
				"write": parseMetric(metrics, 'container_io_wios_total', y.id),
				"write_bytes": parseMetric(metrics, 'container_io_wbytes_bytes', y.id)
			},
			"proc": {
				"current": parseMetric(metrics, 'container_pids_current', y.id),
				"limit": parseMetric(metrics, 'container_pids_limit', y.id)
			},
			"measurement_name": "containerd",
			"tag": {
				"host": hostname,
				"container_id": y.id,
				"container_image": y.image,
				"container_name": metadata.Metadata.Name,
				"state": tasks.includes(y.id) ? 'Running' : 'Exited',
				"io.kubernetes.container.name": spec.annotations['io.kubernetes.cri.container-name'],
				"io.kubernetes.container.type": spec.annotations['io.kubernetes.cri.container-type'],
				"io.kubernetes.pod.name": y.labels['io.kubernetes.pod.name'],
				"io.kubernetes.pod.namespace": y.labels['io.kubernetes.pod.namespace']
			}
		}
	})
}

async function pushMetrics(dataset) {
	const today = new Date()
	const indexName = "docker-" + today.getFullYear() + "." + String(today.getMonth() + 1).padStart(2, '0') + "." + String(today.getDate()).padStart(2, '0')
	const operations = dataset.flatMap(doc => [{ index: { _index: indexName } }, doc])
	const bulkResponse = await elasticClient.bulk({ refresh: true, operations })

	if (bulkResponse.errors) {
		const erroredDocuments = []

		bulkResponse.items.forEach((action, i) => {
			const operation = Object.keys(action)[0]
			if (action[operation].error) {
				erroredDocuments.push({
					status: action[operation].status,
					error: action[operation].error,
					operation: operations[i * 2],
					document: operations[i * 2 + 1]
				})
			}
		})
		console.log(erroredDocuments)
	}
	console.log(`Pushed ${dataset.length} metrics`)
}

let run = async () => {
	console.log("Started monitoring containerd")
	let dataset = []
	let c = 0

	while (true) {
		await setTimeout(process.env.METRIC_INTERVAL * 1000)
		dataset.push(...(await getMetrics()))

		if (++c > 10 || dataset.length > 2000) {
			await pushMetrics(dataset)
			dataset = []
			c = 0
		}
	}
}

await run()

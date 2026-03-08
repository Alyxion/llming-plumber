/**
 * Plumber UI — zero-build entry point.
 *
 * Uses Vue (global) + Quasar (UMD) loaded via <script> tags,
 * and Drawflow / Pinia via importmap ESM modules.
 * All dependencies vendored locally — no npm, no CDN.
 */

import { createRouter, createWebHistory } from 'vue-router'
import { createPinia } from 'pinia'
import { getAllThemes, getTheme, applyTheme } from './themes/index.js'

// Drawflow loaded via <script> tag (UMD → window.Drawflow is the constructor)
const DrawflowLib = window.Drawflow

const { createApp, ref, reactive, computed, onMounted, watch, h, defineComponent, markRaw, nextTick, toRaw } = Vue

const IMAGINARY_OWNER = 'plumber-dev-user'

// ---------- Session ----------

const userHandle = ref('')
async function fetchSession() {
  try {
    const data = await api('GET', '/me')
    userHandle.value = data.handle
  } catch {}
}
fetchSession()

// ---------- API helper ----------

async function api(method, path, body) {
  const opts = { method, headers: { 'Content-Type': 'application/json' } }
  if (body) opts.body = JSON.stringify(body)
  const resp = await fetch(`/api${path}`, opts)
  if (!resp.ok) {
    const text = await resp.text().catch(() => '')
    throw new Error(`API ${method} ${path}: ${resp.status} ${text}`)
  }
  if (resp.status === 204) return null
  return resp.json()
}

// ---------- Block icon mapping ----------

const BLOCK_ICONS = {
  http_request: 'http', rss_reader: 'rss_feed', split: 'call_split',
  collect: 'call_merge', range: 'format_list_numbered', wait: 'hourglass_empty',
  log: 'terminal', text_template: 'text_fields', filter: 'filter_alt',
  sort: 'sort', merge: 'merge', aggregate: 'functions',
  excel_builder: 'table_chart', pdf_builder: 'picture_as_pdf',
  weather: 'wb_sunny', tagesschau: 'newspaper', llm_chat: 'smart_toy',
  static_data: 'data_object', csv_parser: 'table_rows',
  llm_summarizer: 'summarize', llm_translator: 'translate',
  llm_sentiment: 'sentiment_satisfied', llm_classifier: 'category',
  llm_rewriter: 'edit_note', llm_entity_extractor: 'manage_search',
  llm_data_extractor: 'data_exploration', llm_question_answerer: 'quiz',
  json_transformer: 'data_object', jsonpath: 'account_tree',
  xml_parser: 'code', html_extractor: 'web', regex_extractor: 'text_snippet',
  hash_generator: 'tag', base64_codec: 'lock', deduplicator: 'filter_none',
  split_text: 'content_cut', column_mapper: 'view_column',
  timer_trigger: 'timer', datetime_formatter: 'schedule', safe_eval: 'calculate',
  pdf_reader: 'picture_as_pdf', excel_reader: 'table_chart',
  word_reader: 'description', word_writer: 'description',
  news_api: 'newspaper', dwd_weather: 'cloud',
  nina: 'warning', autobahn: 'directions_car', feiertage: 'celebration',
  pegel_online: 'water', azure_blob_write: 'cloud_upload',
  azure_blob_read: 'cloud_download', azure_blob_list: 'folder',
  azure_blob_delete: 'delete', azure_blob_trigger: 'bolt',
  manual_trigger: 'play_circle', read_cache: 'saved_search', store_cache: 'archive',
  variable_store: 'inventory_2', set_variables: 'tune',
  // MongoDB
  mongo_find: 'search', mongo_find_one: 'find_in_page', mongo_insert: 'add_circle',
  mongo_update: 'edit', mongo_delete: 'delete_sweep', mongo_aggregate: 'analytics',
  mongo_count: 'tag', mongo_watch: 'visibility',
  // Redis
  redis_get: 'download', redis_set: 'upload', redis_delete: 'remove_circle',
  redis_list_push: 'playlist_add', redis_list_pop: 'playlist_remove',
  redis_list_range: 'view_list', redis_publish: 'campaign',
  redis_subscribe: 'notifications', redis_hash_get: 'dataset',
  redis_hash_set: 'dataset_linked', redis_keys: 'vpn_key', redis_incr: 'exposure_plus_1',
  // Archive
  zip_create: 'folder_zip', zip_extract: 'unarchive', zip_list: 'list_alt',
  // Files
  file_list: 'folder_open', file_read: 'file_open', file_write: 'save',
  file_collector: 'create_new_folder', file_move: 'drive_file_move', file_delete: 'delete_forever',
}
function blockIcon(bt) { return BLOCK_ICONS[bt] || 'extension' }

// ---------- Field classification ----------

// Fields that are infrastructure — hidden by default, never pipeable
const INFRA_PATTERNS = ['api_key', 'api_base', 'api_url', 'base_url', 'endpoint', 'connection_string', 'account_url']
const SECRET_FIELDS = new Set(['api_key', 'connection_string', 'token', 'secret', 'password'])

function isInfraField(key, prop) {
  if (prop?.secret) return true
  if (prop?.group === 'advanced') return true
  if (INFRA_PATTERNS.some(p => key.includes(p))) return true
  if (SECRET_FIELDS.has(key)) return true
  return false
}

function isMultilineField(key, prop, val) {
  if (prop?.widget === 'textarea') return true
  if (prop?.widget === 'code') return true
  if (typeof val === 'object' && val !== null) return true
  if (typeof val === 'string' && val.length > 100) return true
  return false
}

// ---------- Primary field per block type (opened on double-click) ----------

const PRIMARY_FIELD = {
  text_template: 'template',
  llm_chat: 'system_prompt',
  llm_summarizer: 'text',
  llm_rewriter: 'text',
  llm_translator: 'text',
  llm_classifier: 'text',
  llm_sentiment: 'text',
  llm_entity_extractor: 'text',
  llm_data_extractor: 'text',
  llm_question_answerer: 'context',
  filter: 'expression',
  safe_eval: 'expression',
  static_data: 'content',
  json_transformer: 'expression',
  jsonpath: 'expression',
  regex_extractor: 'pattern',
  log: 'message',
  http_request: 'url',
  html_extractor: 'selector',
}

// ---------- UID helper ----------

let _uid = 0
function uid() { return `b${++_uid}_${Date.now().toString(36)}` }

// ---------- Preset pipelines ----------

const PRESETS = [
  {
    name: 'Weather Report',
    description: 'Fetch weather → format → summarize with LLM → log',
    blocks: [
      { uid: 'w1', block_type: 'weather', label: 'Get Weather', config: { city: 'Berlin,DE', units: 'metric' }, position: { x: 80, y: 150 } },
      { uid: 'w2', block_type: 'text_template', label: 'Format Report', config: { template: 'Weather in {city_name}: {description}, {temp}°C (feels like {feels_like}°C), humidity {humidity}%, wind {wind_speed} m/s' }, position: { x: 380, y: 150 } },
      { uid: 'w3', block_type: 'llm_summarizer', label: 'Summarize', config: {}, position: { x: 680, y: 150 } },
      { uid: 'w4', block_type: 'log', label: 'Log Result', config: {}, position: { x: 980, y: 150 } },
    ],
    pipes: [
      { uid: 'p1', source_block_uid: 'w1', source_fitting_uid: 'output', target_block_uid: 'w2', target_fitting_uid: 'input' },
      { uid: 'p2', source_block_uid: 'w2', source_fitting_uid: 'output', target_block_uid: 'w3', target_fitting_uid: 'input', field_mapping: { text: 'rendered' } },
      { uid: 'p3', source_block_uid: 'w3', source_fitting_uid: 'output', target_block_uid: 'w4', target_fitting_uid: 'input', field_mapping: { message: 'summary' } },
    ],
  },
  {
    name: 'News Analysis',
    description: 'RSS → split → sentiment → filter positive → collect → excel',
    blocks: [
      { uid: 'n1', block_type: 'rss_reader', label: 'RSS Feed', config: { url: 'https://feeds.bbci.co.uk/news/rss.xml' }, position: { x: 80, y: 100 } },
      { uid: 'n2', block_type: 'split', label: 'Split Articles', config: { field: 'items' }, position: { x: 350, y: 100 } },
      { uid: 'n3', block_type: 'llm_sentiment', label: 'Sentiment', config: {}, position: { x: 620, y: 100 } },
      { uid: 'n4', block_type: 'filter', label: 'Positive Only', config: { expression: 'sentiment == "positive"' }, position: { x: 620, y: 300 } },
      { uid: 'n5', block_type: 'collect', label: 'Collect', config: {}, position: { x: 890, y: 200 } },
      { uid: 'n6', block_type: 'excel_builder', label: 'Excel Export', config: { filename: 'positive_news.xlsx' }, position: { x: 1160, y: 200 } },
    ],
    pipes: [
      { uid: 'p1', source_block_uid: 'n1', source_fitting_uid: 'output', target_block_uid: 'n2', target_fitting_uid: 'input' },
      { uid: 'p2', source_block_uid: 'n2', source_fitting_uid: 'output', target_block_uid: 'n3', target_fitting_uid: 'input' },
      { uid: 'p3', source_block_uid: 'n3', source_fitting_uid: 'output', target_block_uid: 'n4', target_fitting_uid: 'input' },
      { uid: 'p4', source_block_uid: 'n4', source_fitting_uid: 'output', target_block_uid: 'n5', target_fitting_uid: 'input' },
      { uid: 'p5', source_block_uid: 'n5', source_fitting_uid: 'output', target_block_uid: 'n6', target_fitting_uid: 'input' },
    ],
  },
  {
    name: 'Data Transform Chain',
    description: 'Static JSON → JSONPath → filter → sort → deduplicate → log',
    blocks: [
      { uid: 'd1', block_type: 'static_data', label: 'Sample Data', config: { content: JSON.stringify([{ name: 'Alice', age: 30, city: 'Berlin' }, { name: 'Bob', age: 25, city: 'Munich' }, { name: 'Alice', age: 30, city: 'Berlin' }, { name: 'Carol', age: 35, city: 'Hamburg' }]), mime_type: 'application/json' }, position: { x: 80, y: 150 } },
      { uid: 'd2', block_type: 'jsonpath', label: 'Extract', config: { expression: '$[*]' }, position: { x: 350, y: 150 } },
      { uid: 'd3', block_type: 'filter', label: 'Age > 25', config: { expression: 'age > 25' }, position: { x: 350, y: 350 } },
      { uid: 'd4', block_type: 'deduplicator', label: 'Deduplicate', config: { key: 'name' }, position: { x: 620, y: 250 } },
      { uid: 'd5', block_type: 'sort', label: 'Sort by Name', config: { key: 'name', descending: false }, position: { x: 890, y: 250 } },
      { uid: 'd6', block_type: 'log', label: 'Output', config: {}, position: { x: 1160, y: 250 } },
    ],
    pipes: [
      { uid: 'p1', source_block_uid: 'd1', source_fitting_uid: 'output', target_block_uid: 'd2', target_fitting_uid: 'input' },
      { uid: 'p2', source_block_uid: 'd2', source_fitting_uid: 'output', target_block_uid: 'd3', target_fitting_uid: 'input' },
      { uid: 'p3', source_block_uid: 'd3', source_fitting_uid: 'output', target_block_uid: 'd4', target_fitting_uid: 'input' },
      { uid: 'p4', source_block_uid: 'd4', source_fitting_uid: 'output', target_block_uid: 'd5', target_fitting_uid: 'input' },
      { uid: 'p5', source_block_uid: 'd5', source_fitting_uid: 'output', target_block_uid: 'd6', target_fitting_uid: 'input' },
    ],
  },
  {
    name: 'Multi-Source Merge',
    description: 'Weather + Tagesschau + Feiertage → merge → AI briefing → log',
    blocks: [
      { uid: 'm1', block_type: 'weather', label: 'Weather Berlin', config: { city: 'Berlin,DE' }, position: { x: 80, y: 60 } },
      { uid: 'm2', block_type: 'tagesschau', label: 'Tagesschau', config: {}, position: { x: 80, y: 240 } },
      { uid: 'm3', block_type: 'feiertage', label: 'Feiertage', config: { year: 2026, state: 'BE' }, position: { x: 80, y: 420 } },
      { uid: 'm4', block_type: 'merge', label: 'Merge All', config: {}, position: { x: 450, y: 240 } },
      { uid: 'm5', block_type: 'llm_chat', label: 'AI Briefing', config: { system_prompt: 'Create a morning briefing from the provided data.' }, position: { x: 750, y: 240 } },
      { uid: 'm6', block_type: 'log', label: 'Output', config: {}, position: { x: 1050, y: 240 } },
    ],
    pipes: [
      { uid: 'p1', source_block_uid: 'm1', source_fitting_uid: 'output', target_block_uid: 'm4', target_fitting_uid: 'input' },
      { uid: 'p2', source_block_uid: 'm2', source_fitting_uid: 'output', target_block_uid: 'm4', target_fitting_uid: 'input' },
      { uid: 'p3', source_block_uid: 'm3', source_fitting_uid: 'output', target_block_uid: 'm4', target_fitting_uid: 'input' },
      { uid: 'p4', source_block_uid: 'm4', source_fitting_uid: 'output', target_block_uid: 'm5', target_fitting_uid: 'input' },
      { uid: 'p5', source_block_uid: 'm5', source_fitting_uid: 'output', target_block_uid: 'm6', target_fitting_uid: 'input' },
    ],
  },
  {
    name: 'Document Processing',
    description: 'HTTP → PDF extract → LLM entity extraction → JSON → Excel',
    blocks: [
      { uid: 'dp1', block_type: 'http_request', label: 'Fetch PDF', config: { url: 'https://example.com/report.pdf', method: 'GET' }, position: { x: 80, y: 180 } },
      { uid: 'dp2', block_type: 'pdf_extractor', label: 'Extract Text', config: {}, position: { x: 350, y: 180 } },
      { uid: 'dp3', block_type: 'llm_entity_extractor', label: 'Extract Entities', config: { entity_types: ['person', 'organization', 'date'] }, position: { x: 620, y: 180 } },
      { uid: 'dp4', block_type: 'json_transformer', label: 'Reshape', config: { expression: '{ entities: items, count: length(items) }' }, position: { x: 890, y: 180 } },
      { uid: 'dp5', block_type: 'excel_builder', label: 'Excel Report', config: { filename: 'entities.xlsx' }, position: { x: 1160, y: 180 } },
    ],
    pipes: [
      { uid: 'p1', source_block_uid: 'dp1', source_fitting_uid: 'output', target_block_uid: 'dp2', target_fitting_uid: 'input' },
      { uid: 'p2', source_block_uid: 'dp2', source_fitting_uid: 'output', target_block_uid: 'dp3', target_fitting_uid: 'input' },
      { uid: 'p3', source_block_uid: 'dp3', source_fitting_uid: 'output', target_block_uid: 'dp4', target_fitting_uid: 'input' },
      { uid: 'p4', source_block_uid: 'dp4', source_fitting_uid: 'output', target_block_uid: 'dp5', target_fitting_uid: 'input' },
    ],
  },
]

// ---------- Drawflow helpers ----------

function _fmKey(source, target) { return `${source}→${target}` }

function getBlockCatalogEntry(catalogEntries, blockType) {
  return catalogEntries.find(c => c.block_type === blockType) || null
}

function getBlockFittings(catalogEntries, blockType) {
  const entry = getBlockCatalogEntry(catalogEntries, blockType)
  return {
    input: entry?.input_fittings?.length
      ? entry.input_fittings
      : [{ uid: 'input', label: 'Input', color: '', description: '' }],
    output: entry?.output_fittings?.length
      ? entry.output_fittings
      : [{ uid: 'output', label: 'Output', color: '', description: '' }],
  }
}

function fittingUidToPort(fittings, fittingUid, prefix) {
  const idx = fittings.findIndex(f => f.uid === fittingUid)
  return idx >= 0 ? `${prefix}_${idx + 1}` : `${prefix}_1`
}

function portToFittingUid(fittings, portClass) {
  const match = portClass.match(/(\d+)/)
  const idx = match ? parseInt(match[1]) - 1 : 0
  return fittings[idx]?.uid || (portClass.startsWith('input') ? 'input' : 'output')
}

function nodeHtml(blockType, label, status, disabled) {
  const icon = blockIcon(blockType)
  const sc = { running: 'var(--p-node-running)', completed: 'var(--p-node-completed)', failed: 'var(--p-node-failed)', idle: 'var(--p-node-idle)' }
  const cls = `block-node block-node--${status || 'idle'}${disabled ? ' block-node--disabled' : ''}`
  return `<div class="${cls}">
    <div class="block-node__status" style="background:${sc[status] || sc.idle}"></div>
    <div class="block-node__content">
      <span class="material-icons block-node__icon">${icon}</span>
      <div>
        <div class="block-node__label">${label}</div>
        <div class="block-node__type">${blockType}</div>
      </div>
    </div>
  </div>`
}

function _buildPortTooltip(fitting, schema) {
  const fields = schema?.properties ? Object.entries(schema.properties) : []
  const titleColor = fitting.color ? ` style="color:${fitting.color}"` : ''
  let fieldsHtml = ''
  if (fields.length > 0) {
    fieldsHtml = '<div class="port-tooltip__fields">' +
      fields.map(([name, prop]) =>
        `<div class="port-tooltip__field"><span class="port-tooltip__field-name">${name}</span><span class="port-tooltip__field-type">${prop.type || ''}${prop.description ? ' — ' + prop.description : ''}</span></div>`
      ).join('') + '</div>'
  }
  return '<div class="port-tooltip">' +
    `<div class="port-tooltip__title"${titleColor}>${fitting.label || fitting.uid}</div>` +
    (fitting.description ? `<div class="port-tooltip__desc">${fitting.description}</div>` : '') +
    fieldsHtml + '</div>'
}

function applyPortColors(_editor, dfId, fittings, catalogEntry) {
  const nodeEl = document.getElementById(`node-${dfId}`)
  if (!nodeEl) return
  const outSchema = catalogEntry?.output_schema
  const inSchema = catalogEntry?.input_schema

  fittings.output.forEach((f, i) => {
    const portEl = nodeEl.querySelector(`.output.output_${i + 1}`)
    if (!portEl) return
    if (f.color) { portEl.style.background = f.color; portEl.style.borderColor = f.color }
    // Tooltip
    if (!portEl.querySelector('.port-tooltip')) {
      portEl.insertAdjacentHTML('beforeend', _buildPortTooltip(f, outSchema))
    }
  })
  fittings.input.forEach((f, i) => {
    const portEl = nodeEl.querySelector(`.input.input_${i + 1}`)
    if (!portEl) return
    if (f.color) { portEl.style.background = f.color; portEl.style.borderColor = f.color }
    // Tooltip
    if (!portEl.querySelector('.port-tooltip')) {
      portEl.insertAdjacentHTML('beforeend', _buildPortTooltip(f, inSchema))
    }
  })
  // Port labels removed — fitting names shown in tooltip on hover instead
}

function applyEdgeColor(editor, srcDfId, tgtDfId, outputClass, inputClass, catalog, nodes, dfIdToUid) {
  const sourceUid = dfIdToUid[srcDfId]
  if (!sourceUid) return
  const srcNode = nodes.find(n => n.id === sourceUid)
  if (!srcNode) return
  const fittings = getBlockFittings(catalog, srcNode.data.blockType).output
  const portIdx = parseInt(outputClass.split('_')[1]) - 1
  const fitting = fittings[portIdx]
  const color = fitting?.color || ''
  if (!color) return
  setTimeout(() => {
    const svgs = editor.container.querySelectorAll(
      `.connection.node_out_node-${srcDfId}.node_in_node-${tgtDfId}.${outputClass}.${inputClass}`
    )
    svgs.forEach(svg => {
      const path = svg.querySelector('.main-path')
      if (path) path.style.stroke = color
    })
  }, 50)
}

function flashEdge(editor, uidToDfId, sourceUid, targetUid, flashType) {
  const srcDfId = uidToDfId[sourceUid]
  const tgtDfId = uidToDfId[targetUid]
  if (srcDfId == null || tgtDfId == null) return
  const svgs = editor.container.querySelectorAll(
    `.connection.node_out_node-${srcDfId}.node_in_node-${tgtDfId}`
  )
  svgs.forEach(svg => {
    svg.classList.remove('edge-flash--running', 'edge-flash--completed', 'edge-flash--failed')
    if (flashType) svg.classList.add(`edge-flash--${flashType}`)
  })
}

// ---------- Pages ----------

const PipelineListPage = defineComponent({
  name: 'PipelineListPage',
  setup() {
    const pipelines = ref([])
    const loading = ref(true)
    const searchQuery = ref('')
    const catalogOpen = ref(false)
    const catalog = ref([])
    const catalogLoading = ref(false)
    const busy = reactive({})
    const expandedPipeline = ref(null) // pipeline id showing run history
    const pipelineRuns = ref([])
    const runsLoading = ref(false)
    const expandedRun = ref(null)
    const expandedRunDetail = ref(null)
    const expandedBlock = ref(null)
    let refreshTimer = null

    async function load() {
      try { pipelines.value = await api('GET', '/pipelines') } catch {}
      loading.value = false
    }
    async function deletePipeline(id, e) {
      e.stopPropagation()
      try {
        await api('DELETE', `/pipelines/${id}`)
        pipelines.value = pipelines.value.filter(p => (p.id || p._id) !== id)
      } catch {}
    }
    async function loadCatalog() {
      catalogLoading.value = true
      try { catalog.value = await api('GET', '/demo-pipelines/catalog') } catch {}
      catalogLoading.value = false
    }
    async function toggleCatalog() {
      catalogOpen.value = !catalogOpen.value
      if (catalogOpen.value && catalog.value.length === 0) await loadCatalog()
    }
    async function addSample(key) {
      busy[key] = true
      try { await api('POST', `/demo-pipelines/add/${key}`); await Promise.all([load(), loadCatalog()]) } catch {}
      busy[key] = false
    }
    async function removeSample(key) {
      busy[key] = true
      try { await api('DELETE', `/demo-pipelines/remove/${key}`); await Promise.all([load(), loadCatalog()]) } catch {}
      busy[key] = false
    }
    function isSample(p) { return (p.tags || []).some(t => t.startsWith('_sample:')) }
    function hasTimerBlock(p) {
      return (p.blocks || []).some(b => b.block_type === 'timer_trigger')
    }
    async function togglePipelineEnabled(p, e) {
      e.stopPropagation()
      const pid = p.id || p._id
      const newEnabled = !(p.enabled !== false) // toggle — default is true
      try {
        // Update the pipeline enabled flag
        const full = await api('GET', `/pipelines/${pid}`)
        full.enabled = newEnabled
        await api('PUT', `/pipelines/${pid}`, full)
        // Also toggle the schedule if it has one
        if (hasTimerBlock(p)) {
          const ss = await api('GET', '/schedules')
          const existing = ss.find(s => s.pipeline_id === pid)
          if (existing && newEnabled && !existing.enabled) {
            await api('PUT', `/schedules/${existing.id}`, { ...existing, enabled: true })
          } else if (existing && !newEnabled && existing.enabled) {
            await api('PUT', `/schedules/${existing.id}`, { ...existing, enabled: false })
          }
        }
      } catch {}
      await load()
    }
    function timeAgo(dateStr) {
      if (!dateStr || dateStr === 'None' || dateStr === '') return ''
      const d = new Date(dateStr)
      if (isNaN(d)) return ''
      const s = Math.floor((Date.now() - d.getTime()) / 1000)
      if (s < 0) return 'just now'
      if (s < 5) return 'just now'
      if (s < 60) return `${s}s ago`
      if (s < 3600) return `${Math.floor(s / 60)}m ago`
      if (s < 86400) return `${Math.floor(s / 3600)}h ago`
      return `${Math.floor(s / 86400)}d ago`
    }
    async function toggleRunHistory(pid, e) {
      e.stopPropagation()
      if (expandedPipeline.value === pid) {
        expandedPipeline.value = null; pipelineRuns.value = []; expandedRun.value = null; expandedRunDetail.value = null
        return
      }
      expandedPipeline.value = pid; expandedRun.value = null; expandedRunDetail.value = null; expandedBlock.value = null
      runsLoading.value = true
      try { pipelineRuns.value = await api('GET', `/runs?pipeline_id=${pid}&limit=100`) } catch { pipelineRuns.value = [] }
      runsLoading.value = false
    }
    async function toggleRunDetail(runId, e) {
      e.stopPropagation()
      if (expandedRun.value === runId) { expandedRun.value = null; expandedRunDetail.value = null; expandedBlock.value = null; return }
      expandedRun.value = runId; expandedBlock.value = null
      try { expandedRunDetail.value = await api('GET', `/runs/${runId}`) } catch { expandedRunDetail.value = null }
    }
    function fmtRunTime(iso) {
      if (!iso) return ''
      return new Date(iso).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' })
    }
    function fmtRunDur(r) {
      if (!r.started_at || !r.finished_at) return ''
      const ms = new Date(r.finished_at) - new Date(r.started_at)
      return ms < 1000 ? `${ms}ms` : `${(ms / 1000).toFixed(1)}s`
    }
    function fmtDur(ms) {
      if (ms == null) return ''
      return ms < 1000 ? `${Math.round(ms)}ms` : `${(ms / 1000).toFixed(1)}s`
    }
    function onKeydown(e) {
      if (e.key === 'Escape' && catalogOpen.value) catalogOpen.value = false
      if (e.key === 'Escape' && expandedPipeline.value) {
        expandedPipeline.value = null; pipelineRuns.value = []; expandedRun.value = null; expandedRunDetail.value = null
      }
    }
    onMounted(() => {
      load()
      window.addEventListener('keydown', onKeydown)
      refreshTimer = setInterval(() => { if (!loading.value) load() }, 5000)
    })
    Vue.onUnmounted(() => {
      window.removeEventListener('keydown', onKeydown)
      if (refreshTimer) clearInterval(refreshTimer)
    })

    function catalogByCategory() {
      const groups = {}
      for (const s of catalog.value) { if (!groups[s.category]) groups[s.category] = []; groups[s.category].push(s) }
      return groups
    }

    // Compute filtered pipelines based on search
    function filteredPipelines() {
      const q = searchQuery.value.toLowerCase().trim()
      if (!q) return pipelines.value
      return pipelines.value.filter(p => p.name.toLowerCase().includes(q))
    }

    // Extract interesting stats from a block log's output_summary
    function extractStats(log, statKeys) {
      if (!log || !log.length) return []
      const stats = []
      const seen = new Set()
      for (const entry of log) {
        const os = entry.output_summary || {}
        for (const [k, v] of Object.entries(os)) {
          if (seen.has(k)) continue
          // Auto-detect interesting numeric/count fields
          const isInteresting = statKeys.length
            ? statKeys.includes(k)
            : /count|size|total|rows|items|files|bytes|records|length|pages/.test(k)
          if (isInteresting && v != null && v !== '' && v !== 0) {
            seen.add(k)
            const label = k.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase())
            stats.push({ key: k, label, value: typeof v === 'number' && v > 1024 ? `${(v / 1024).toFixed(1)}K` : String(v) })
          }
        }
      }
      return stats.slice(0, 4) // max 4 stats shown
    }

    // Render a single pipeline row
    function renderPipelineRow(p) {
      const pid = p.id || p._id
      const sample = isSample(p)
      const lr = p.latest_run
      const isRunning = lr && (lr.status === 'running' || lr.status === 'queued')
      const scheduleEnabled = !!(p.schedule && p.schedule.enabled)
      const pipelineEnabled = p.enabled !== false
      const isExpanded = expandedPipeline.value === pid
      const display = p.display || {}
      const accentColor = display.color || null
      const blockLog = lr?.log || []
      const statKeys = display.stat_keys || []
      const stats = extractStats(blockLog, statKeys)

      // Accent style
      const accentStyle = accentColor ? `border-left: 3px solid ${accentColor};` : ''

      // Time display
      const timeDisplay = isRunning
        ? (lr.started_at ? timeAgo(lr.started_at) : 'queued')
        : lr?.finished_at ? timeAgo(lr.finished_at) : ''

      const durDisplay = lr?.duration_ms != null ? fmtDur(lr.duration_ms) : ''

      return h('div', { key: pid, class: 'pl-row' + (isExpanded ? ' pl-row--expanded' : '') + (sample ? ' pl-row--sample' : '') + (!pipelineEnabled ? ' pl-row--disabled' : ''), style: accentStyle }, [
        // Main row: toggle | content | right
        h('div', { class: 'pl-row__main' }, [

          // Left column: enabled toggle (all pipelines)
          h('div', { class: 'pl-row__toggle-col' }, [
            h('button', {
              class: 'pl-toggle' + (pipelineEnabled ? ' pl-toggle--on' : ''),
              title: pipelineEnabled ? 'Disable pipeline' : 'Enable pipeline',
              onClick: (e) => togglePipelineEnabled(p, e),
            }, [
              h('div', { class: 'pl-toggle__track' }, [
                h('div', { class: 'pl-toggle__thumb' }),
              ]),
            ]),
          ]),

          // Clickable content area
          h('div', { class: 'pl-row__content', onClick: () => router.push(`/editor/${pid}`) }, [
            // Left: name + meta
            h('div', { class: 'pl-row__left' }, [
              h('div', { class: 'pl-row__info' }, [
                h('div', { class: 'pl-row__name' }, [
                  display.icon ? h('span', { class: 'material-icons', style: 'font-size:16px; margin-right:6px; vertical-align:middle; color:' + (accentColor || 'var(--p-text-secondary)') }, display.icon) : null,
                  isRunning ? h('span', { class: 'material-icons pl-row__running-icon' }, 'play_circle') : null,
                  p.name,
                  sample ? h('span', { class: 'sample-badge', style: 'margin-left:8px' }, 'sample') : null,
                ]),
                h('div', { class: 'pl-row__meta' }, [
                  `${(p.blocks || []).length} blocks · v${p.version || 1}`,
                  scheduleEnabled ? h('span', { class: 'pl-row__schedule', style: accentColor ? `background:${accentColor}` : '' }, [
                    p.schedule.interval_seconds ? `${p.schedule.interval_seconds}s` : p.schedule.cron_expression || 'sched',
                  ]) : null,
                ]),
              ]),
            ]),

            // Center: block execution dots
            blockLog.length > 0 ? h('div', { class: 'pl-row__dots' },
              blockLog.map((entry, i) => {
                const st = entry.status || 'pending'
                return h('div', {
                  key: i,
                  class: 'pl-dot pl-dot--' + st,
                  title: `${entry.label || entry.block_type}: ${st}${entry.duration_ms ? ' (' + fmtDur(entry.duration_ms) + ')' : ''}${entry.error ? '\n' + entry.error : ''}`,
                  style: accentColor && st === 'completed' ? `background:${accentColor}` : '',
                })
              })
            ) : h('div', { class: 'pl-row__dots' }),

            // Right: stats + timing
            h('div', { class: 'pl-row__right' }, [
              stats.length > 0 ? h('div', { class: 'pl-row__stats' },
                stats.map(s => h('span', { key: s.key, class: 'pl-stat', title: s.key }, [
                  h('span', { class: 'pl-stat__val' }, s.value),
                  h('span', { class: 'pl-stat__label' }, s.label),
                ]))
              ) : null,
              h('div', { class: 'pl-row__timing' }, [
                durDisplay ? h('span', { class: 'pl-row__dur' }, durDisplay) : null,
                timeDisplay ? h('span', { class: 'pl-row__time pl-row__time--' + (lr?.status || 'idle') }, timeDisplay) : null,
                lr?.error ? h('span', { class: 'pl-row__error-hint', title: lr.error }, [
                  h('span', { class: 'material-icons', style: 'font-size:13px' }, 'warning'),
                ]) : null,
              ]),
            ]),
          ]),

          // Action buttons (far right)
          h('div', { class: 'pl-row__actions' }, [
            h('button', { class: 'icon-btn', title: 'Run history', onClick: (e) => toggleRunHistory(pid, e) }, [
              h('span', { class: 'material-icons', style: 'font-size:16px; color:' + (isExpanded ? 'var(--p-primary)' : 'var(--p-text-secondary)') }, 'history'),
            ]),
            h('button', { class: 'icon-btn', title: 'Delete', onClick: (e) => deletePipeline(pid, e) }, [
              h('span', { class: 'material-icons', style: 'font-size:16px; color:var(--p-node-failed)' }, 'delete'),
            ]),
          ]),
        ]),

        // Expanded run history (inline below the row)
        isExpanded ? h('div', { class: 'pl-row__history' }, [
          h('div', { class: 'run-history-panel__header' }, [
            h('h3', { style: 'margin:0; font-size:13px; color:var(--p-text)' }, ['Run History']),
            h('button', { class: 'icon-btn', onClick: (e) => { e.stopPropagation(); expandedPipeline.value = null } }, [
              h('span', { class: 'material-icons', style: 'font-size:16px' }, 'close'),
            ]),
          ]),
          runsLoading.value
            ? h('div', { style: 'padding:12px; color:var(--p-text-secondary); font-size:12px' }, 'Loading...')
            : pipelineRuns.value.length === 0
              ? h('div', { style: 'padding:12px; color:var(--p-text-secondary); font-size:12px' }, 'No runs yet')
              : h('div', { class: 'run-history-list' }, pipelineRuns.value.map(r => {
                  const isOpen = expandedRun.value === r.id
                  const rd = expandedRunDetail.value
                  const statusColors = { completed: 'var(--p-node-completed)', failed: 'var(--p-node-failed)', running: 'var(--p-node-running)' }
                  return h('div', { key: r.id, class: 'run-history-item' + (isOpen ? ' run-history-item--open' : '') }, [
                    h('div', { class: 'run-history-item__row', onClick: (e) => toggleRunDetail(r.id, e) }, [
                      h('span', { class: 'material-icons', style: 'font-size:14px; color:' + (statusColors[r.status] || 'var(--p-text-secondary)') },
                        r.status === 'completed' ? 'check_circle' : r.status === 'failed' ? 'error' : r.status === 'running' ? 'play_circle' : 'circle'),
                      h('span', { class: `status-badge status-badge--${r.status}`, style: 'font-size:10px; padding:1px 5px' }, r.status),
                      h('span', { style: 'font-size:11px; color:var(--p-text-secondary)' }, fmtRunTime(r.created_at)),
                      fmtRunDur(r) ? h('span', { style: 'font-size:11px; color:var(--p-text-secondary)' }, fmtRunDur(r)) : null,
                      (r.log && r.log.length) ? h('span', { style: 'font-size:10px; color:var(--p-text-secondary); flex:1; overflow:hidden; text-overflow:ellipsis; white-space:nowrap' },
                        r.log.map(e => `${e.label || e.block_type}${e.error ? ' x' : ' ok'}`).join(' > ')
                      ) : h('span', { style: 'flex:1' }),
                      r.error ? h('span', { style: 'font-size:10px; color:var(--p-node-failed); max-width:200px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap' }, r.error) : null,
                      h('span', { class: 'material-icons', style: 'font-size:14px; color:var(--p-text-secondary)' }, isOpen ? 'expand_less' : 'expand_more'),
                    ]),
                    isOpen && rd ? h('div', { class: 'run-history-item__detail' }, [
                      rd.error ? h('div', { class: 'run-detail__error', style: 'margin:0 0 8px' }, [
                        h('span', { class: 'material-icons', style: 'font-size:14px' }, 'error'), rd.error,
                      ]) : null,
                      h('div', { class: 'run-detail__blocks' },
                        ((rd.log && rd.log.length) ? rd.log : Object.keys(rd.block_states || {}).map(bid => ({ uid: bid, ...rd.block_states[bid] }))).map(entry => {
                          const bid = entry.uid
                          const isBlockOpen = expandedBlock.value === `${r.id}:${bid}`
                          const st = entry.status || 'pending'
                          const statusIcon = st === 'completed' ? 'check_circle' : st === 'failed' ? 'error' : 'pending'
                          return h('div', { class: 'run-block', key: bid }, [
                            h('div', {
                              class: 'run-block__header',
                              onClick: (e) => { e.stopPropagation(); expandedBlock.value = isBlockOpen ? null : `${r.id}:${bid}` },
                              style: 'cursor:pointer',
                            }, [
                              h('span', { class: 'material-icons', style: `font-size:14px; color:${statusColors[st] || 'var(--p-text-secondary)'}` }, statusIcon),
                              entry.block_type ? h('code', { style: 'color:var(--p-text-secondary); font-size:9px; margin-right:4px' }, entry.block_type) : null,
                              h('span', { style: 'font-size:12px' }, entry.label || bid),
                              h('span', { style: 'flex:1' }),
                              entry.parcel_count > 1 ? h('span', { style: 'font-size:9px; color:var(--p-text-secondary); margin-right:6px' }, `${entry.parcel_count} parcels`) : null,
                              entry.duration_ms != null ? h('span', { class: 'run-block__dur', style: 'font-size:10px' }, fmtDur(entry.duration_ms)) : null,
                            ]),
                            isBlockOpen ? h('div', { class: 'run-block__detail' }, [
                              entry.error ? h('div', { class: 'run-block__error' }, entry.error) : null,
                              (entry.output_summary && Object.keys(entry.output_summary).length) ? h('pre', { class: 'run-block__json', style: 'font-size:10px; max-height:200px' }, JSON.stringify(entry.output_summary, null, 2)) : null,
                            ]) : null,
                          ])
                        }),
                      ),
                    ]) : (isOpen ? h('div', { style: 'padding:8px 12px; font-size:11px; color:var(--p-text-secondary)' }, 'Loading...') : null),
                  ])
                })),
        ]) : null,
      ])
    }

    return () => h('div', { class: 'pipeline-list' }, [
      // Header with search
      h('div', { class: 'pipeline-list__header' }, [
        h('h2', null, 'Pipelines'),
        h('div', { class: 'pl-search' }, [
          h('span', { class: 'material-icons pl-search__icon' }, 'search'),
          h('input', {
            class: 'pl-search__input',
            type: 'text',
            placeholder: 'Search pipelines...',
            value: searchQuery.value,
            onInput: (e) => { searchQuery.value = e.target.value },
          }),
          searchQuery.value ? h('button', { class: 'icon-btn pl-search__clear', onClick: () => { searchQuery.value = '' } }, [
            h('span', { class: 'material-icons', style: 'font-size:14px' }, 'close'),
          ]) : null,
        ]),
        h('div', { style: 'flex:1' }),
        h('button', {
          class: 'toolbar-btn' + (catalogOpen.value ? ' toolbar-btn--active' : ''),
          onClick: toggleCatalog,
        }, [
          h('span', { class: 'material-icons', style: 'font-size:16px' }, 'apps'), ' Samples',
        ]),
        h('button', { class: 'toolbar-btn toolbar-btn--primary', onClick: () => router.push('/editor') }, [
          h('span', { class: 'material-icons', style: 'font-size:16px' }, 'add'), ' New Pipeline',
        ]),
      ]),

      // Sample catalog drawer
      catalogOpen.value && h('div', { class: 'catalog-drawer' }, [
        catalogLoading.value
          ? h('div', { style: 'padding:16px; color:var(--p-text-secondary); font-size:13px' }, 'Loading catalog...')
          : Object.entries(catalogByCategory()).map(([cat, items]) =>
              h('div', { key: cat, class: 'catalog-group' }, [
                h('div', { class: 'catalog-group__title' }, cat),
                ...items.map(s => h('div', { key: s.key, class: 'catalog-item' + (s.installed ? ' catalog-item--installed' : '') }, [
                  h('div', { class: 'catalog-item__info' }, [
                    h('span', { class: 'material-icons catalog-item__icon' }, s.icon),
                    h('div', { style: 'flex:1; min-width:0' }, [
                      h('div', { class: 'catalog-item__title' }, [s.title, s.has_schedule ? h('span', { class: 'trigger-badge', style: 'margin-left:6px' }, 'scheduled') : null]),
                      h('div', { class: 'catalog-item__desc' }, s.description),
                    ]),
                    s.installed
                      ? h('button', { class: 'toolbar-btn toolbar-btn--ghost', style: 'padding:3px 8px; font-size:11px', onClick: () => removeSample(s.key), disabled: busy[s.key] }, busy[s.key] ? '...' : 'Remove')
                      : h('button', { class: 'toolbar-btn toolbar-btn--primary', style: 'padding:3px 8px; font-size:11px', onClick: () => addSample(s.key), disabled: busy[s.key] }, busy[s.key] ? '...' : 'Add'),
                  ]),
                ])),
              ]),
            ),
      ]),

      loading.value ? h('div', { class: 'empty-state' }, 'Loading...')
        : pipelines.value.length === 0
          ? h('div', { class: 'empty-state' }, [
              h('span', { class: 'material-icons' }, 'account_tree'),
              h('div', { style: 'font-size:16px; margin-top:8px' }, 'No pipelines yet'),
              h('div', { style: 'margin-top:4px' }, 'Create a pipeline or add samples from the catalog'),
            ])
          : h('div', { class: 'pl-list' }, filteredPipelines().map(p => renderPipelineRow(p))),
    ])
  },
})

const PipelineEditorPage = defineComponent({
  name: 'PipelineEditorPage',
  setup() {
    const catalog = ref([])
    const search = ref('')
    const pipelineName = ref('New Pipeline')
    const pipelineId = ref(null)
    const selectedNodeId = ref(null)
    const saving = ref(false)
    const presetMenuOpen = ref(false)
    const statusMsg = ref('')
    const showInfra = ref(false)

    // Canvas state
    const nodes = ref([])
    const edges = ref([])
    const edgeFieldMappings = reactive({})

    // Drawflow state
    let editor = null
    const dfIdToUid = {}  // Drawflow int ID → our block UID string
    const uidToDfId = {}  // our block UID string → Drawflow int ID
    let _programmaticChange = false  // suppress Drawflow events during programmatic changes
    let _unselectTimer = null

    // --- Undo/Redo history ---
    const MAX_HISTORY = 100
    const undoStack = []
    const redoStack = []
    let _skipSnapshot = false

    function snapshot() {
      return JSON.stringify({ nodes: nodes.value, edges: edges.value, edgeFieldMappings: { ...edgeFieldMappings } })
    }
    function pushUndo() {
      if (_skipSnapshot) return
      const s = snapshot()
      if (undoStack.length > 0 && undoStack[undoStack.length - 1] === s) return
      undoStack.push(s)
      if (undoStack.length > MAX_HISTORY) undoStack.shift()
      redoStack.length = 0
    }
    function restoreSnapshot(s) {
      _skipSnapshot = true
      const data = JSON.parse(s)
      // Restore field mappings
      Object.keys(edgeFieldMappings).forEach(k => delete edgeFieldMappings[k])
      if (data.edgeFieldMappings) Object.assign(edgeFieldMappings, data.edgeFieldMappings)
      // Rebuild Drawflow canvas from snapshot blocks/edges
      const blocks = data.nodes.map(n => ({
        uid: n.id, block_type: n.data.blockType, label: n.data.label,
        config: n.data.config || {}, position: n.position || { x: 0, y: 0 },
      }))
      const pipes = data.edges.map(e => ({
        uid: e.id, source_block_uid: e.source, target_block_uid: e.target,
        source_fitting_uid: e.sourceHandle || 'output', target_fitting_uid: e.targetHandle || 'input',
      }))
      if (editor) loadBlocksIntoDrawflow(blocks, pipes)
      else { nodes.value = data.nodes; edges.value = data.edges }
      nextTick(() => { _skipSnapshot = false })
    }
    function undo() {
      if (undoStack.length < 2) return
      redoStack.push(undoStack.pop())
      restoreSnapshot(undoStack[undoStack.length - 1])
    }
    function redo() {
      if (redoStack.length === 0) return
      const s = redoStack.pop()
      undoStack.push(s)
      restoreSnapshot(s)
    }

    // Watch for changes and auto-snapshot (debounced)
    let _snapTimer = null
    watch([nodes, edges], () => {
      if (_skipSnapshot) return
      clearTimeout(_snapTimer)
      _snapTimer = setTimeout(pushUndo, 300)
    }, { deep: true })

    // Keyboard shortcuts
    function _handleKeyboard(e) {
      const mod = e.metaKey || e.ctrlKey
      if (mod && e.key === 'z' && !e.shiftKey) { e.preventDefault(); undo() }
      if (mod && e.key === 'z' && e.shiftKey) { e.preventDefault(); redo() }
      if (mod && e.key === 'y') { e.preventDefault(); redo() }
      // Delete selected edge or node
      if ((e.key === 'Delete' || e.key === 'Backspace') && !e.target.closest('input, textarea, [contenteditable]')) {
        if (selectedEdgeId.value) { e.preventDefault(); deleteEdge(selectedEdgeId.value) }
        else if (selectedNodeId.value) { e.preventDefault(); deleteNode(selectedNodeId.value) }
      }
    }
    onMounted(() => document.addEventListener('keydown', _handleKeyboard))

    // Run state
    const running = ref(false)
    const runLog = ref([])
    const runOutput = ref(null)
    // (edge flash is handled via Drawflow DOM classes, no reactive needed)
    const consoleTab = ref('log')
    const hasSchedule = ref(false) // true if backend schedule is active
    let pipelineEvtSource = null // EventSource for pipeline-level events
    const consoleHeight = ref(parseInt(localStorage.getItem('plumber-console-h')) || 180)

    // LLM defaults (fetched from API)
    const llmDefaults = ref({ tiers: {}, providers: [], models: {} })
    // Global template variables
    const globalVars = ref([])
    // Variable autocomplete state
    const varAutocomplete = reactive({ visible: false, items: [], x: 0, y: 0, inputEl: null, selected: 0, prefix: '' })

    // Modal editor for multiline fields
    const textModal = ref(null) // { nodeId, key, title, value }

    const groupedCatalog = computed(() => {
      const map = new Map()
      const q = search.value.toLowerCase()
      for (const entry of catalog.value) {
        if (q && !entry.block_type.includes(q) && !entry.description.toLowerCase().includes(q)) continue
        const cat = (entry.categories && entry.categories[0]) || 'other'
        if (!map.has(cat)) map.set(cat, [])
        map.get(cat).push(entry)
      }
      return map
    })

    const selectedNode = computed(() => {
      if (!selectedNodeId.value) return null
      return nodes.value.find(n => n.id === selectedNodeId.value) || null
    })

    function showStatus(msg, dur = 2000) {
      statusMsg.value = msg
      setTimeout(() => { if (statusMsg.value === msg) statusMsg.value = '' }, dur)
    }

    function nodesToBlocks(nodeList) {
      return nodeList.map(n => ({
        uid: n.id, block_type: n.data.blockType, label: n.data.label,
        config: n.data.config || {}, position: n.position || { x: 0, y: 0 },
        disabled: !!n.data.disabled,
      }))
    }
    function edgesToPipes(edgeList, fmMap) {
      return edgeList.map(e => {
        const fm = fmMap[_fmKey(e.source, e.target)]
        return {
          uid: e.id, source_block_uid: e.source, source_fitting_uid: e.sourceHandle || 'output',
          target_block_uid: e.target, target_fitting_uid: e.targetHandle || 'input',
          ...(fm ? { field_mapping: fm } : {}),
        }
      })
    }

    function addBlock(blockType, label, config, x, y) {
      const blockUid = uid()
      const fittings = getBlockFittings(catalog.value, blockType)
      const effectiveLabel = label || blockType.replace(/_/g, ' ')
      const html = nodeHtml(blockType, effectiveLabel, 'idle')

      const dfId = editor.addNode(blockType, fittings.input.length, fittings.output.length, x, y, blockType, {}, html)
      dfIdToUid[dfId] = blockUid
      uidToDfId[blockUid] = dfId

      nodes.value = [...nodes.value, {
        id: blockUid, position: { x, y },
        data: { label: effectiveLabel, blockType, config: config || {}, status: 'idle' },
      }]

      const catEntry = catalog.value.find(c => c.block_type === blockType)
      nextTick(() => applyPortColors(editor, dfId, fittings, catEntry))
      return blockUid
    }

    function deleteNode(nodeId) {
      const dfId = uidToDfId[nodeId]
      if (dfId != null) {
        _programmaticChange = true
        editor.removeNodeId(`node-${dfId}`)
        _programmaticChange = false
        delete dfIdToUid[dfId]
        delete uidToDfId[nodeId]
      }
      nodes.value = nodes.value.filter(n => n.id !== nodeId)
      edges.value = edges.value.filter(e => e.source !== nodeId && e.target !== nodeId)
      if (selectedNodeId.value === nodeId) selectedNodeId.value = null
    }

    function toggleBlockDisabled(nodeId) {
      const node = nodes.value.find(n => n.id === nodeId)
      if (!node) return
      const nowDisabled = !node.data.disabled
      node.data = { ...node.data, disabled: nowDisabled }
      const dfId = uidToDfId[nodeId]
      if (dfId != null) {
        const el = document.querySelector(`#node-${dfId}`)
        if (el) el.classList.toggle('df-disabled', nowDisabled)
        const html = nodeHtml(node.data.blockType, node.data.label, node.data.status, nowDisabled)
        const contentEl = el?.querySelector('.drawflow_content_node')
        if (contentEl) contentEl.innerHTML = html
      }
    }

    function inferFieldMapping(sourceNodeId, targetNodeId) {
      const sourceNode = nodes.value.find(n => n.id === sourceNodeId)
      const targetNode = nodes.value.find(n => n.id === targetNodeId)
      if (!sourceNode || !targetNode) return null

      const srcEntry = catalog.value.find(c => c.block_type === sourceNode.data.blockType)
      const tgtEntry = catalog.value.find(c => c.block_type === targetNode.data.blockType)
      if (!srcEntry?.output_schema?.properties || !tgtEntry?.input_schema?.properties) return null

      const srcFields = Object.keys(srcEntry.output_schema.properties)
      const tgtFields = Object.keys(tgtEntry.input_schema.properties)
        .filter(k => !isInfraField(k, tgtEntry.input_schema.properties[k]))
      // Required fields on the target that have no default
      const tgtRequired = new Set(tgtEntry.input_schema.required || [])

      const mapping = {}
      for (const tgtField of tgtFields) {
        if (srcFields.includes(tgtField)) continue // same name — no mapping needed
        if (!tgtRequired.has(tgtField)) continue // optional — skip
        // Find a plausible source field by type match (prefer string→string)
        const tgtProp = tgtEntry.input_schema.properties[tgtField]
        const candidates = srcFields.filter(sf => {
          const sp = srcEntry.output_schema.properties[sf]
          return sp.type === tgtProp.type || tgtProp.type === 'string'
        })
        if (candidates.length === 1) {
          mapping[tgtField] = candidates[0]
        } else if (candidates.length > 1) {
          // Heuristic: prefer the "main" output field (first string field)
          const mainField = candidates.find(c => srcEntry.output_schema.properties[c].type === 'string') || candidates[0]
          mapping[tgtField] = mainField
        }
      }
      return Object.keys(mapping).length > 0 ? mapping : null
    }

    // onConnect handled by Drawflow connectionCreated event (see onMounted)

    const selectedEdgeId = ref(null)
    const ctxMenu = ref(null) // { x, y, nodeId }

    function deleteEdge(edgeId) {
      const edge = edges.value.find(e => e.id === edgeId)
      if (edge) {
        const srcDfId = uidToDfId[edge.source]
        const tgtDfId = uidToDfId[edge.target]
        if (srcDfId != null && tgtDfId != null) {
          const srcNode = nodes.value.find(n => n.id === edge.source)
          const tgtNode = nodes.value.find(n => n.id === edge.target)
          const srcFittings = getBlockFittings(catalog.value, srcNode?.data.blockType).output
          const tgtFittings = getBlockFittings(catalog.value, tgtNode?.data.blockType).input
          const outputClass = fittingUidToPort(srcFittings, edge.sourceHandle, 'output')
          const inputClass = fittingUidToPort(tgtFittings, edge.targetHandle, 'input')
          _programmaticChange = true
          editor.removeSingleConnection(srcDfId, tgtDfId, outputClass, inputClass)
          _programmaticChange = false
        }
        const key = _fmKey(edge.source, edge.target)
        delete edgeFieldMappings[key]
      }
      edges.value = edges.value.filter(e => e.id !== edgeId)
      selectedEdgeId.value = null
      pushUndo()
    }

    function onNodeDoubleClick(node) {
      const primaryKey = PRIMARY_FIELD[node.data.blockType]
      if (!primaryKey) return
      const catalogEntry = catalog.value.find(c => c.block_type === node.data.blockType)
      const prop = catalogEntry?.input_schema?.properties?.[primaryKey]
      if (!prop) return
      const val = node.data.config[primaryKey]
      textModal.value = {
        nodeId: node.id, key: primaryKey, title: prop.title || primaryKey,
        value: typeof val === 'object' ? JSON.stringify(val, null, 2) : (val ?? prop.default ?? ''),
        upstreamFields: getUpstreamFields(node.id),
      }
    }

    function onDrop(event) {
      event.preventDefault()
      const data = event.dataTransfer.getData('application/plumber-block')
      if (!data) return
      const entry = JSON.parse(data)
      // Convert screen coordinates to Drawflow canvas coordinates
      const containerRect = editor.container.getBoundingClientRect()
      const x = (event.clientX - containerRect.left - editor.canvas_x) / editor.zoom
      const y = (event.clientY - containerRect.top - editor.canvas_y) / editor.zoom
      addBlock(entry.block_type, entry.block_type.replace(/_/g, ' '), {}, x, y)
    }
    function onDragOver(event) { event.preventDefault(); event.dataTransfer.dropEffect = 'move' }

    function loadPreset(preset) {
      pipelineName.value = preset.name
      pipelineId.value = null
      Object.keys(edgeFieldMappings).forEach(k => delete edgeFieldMappings[k])
      loadBlocksIntoDrawflow(preset.blocks, preset.pipes)
      presetMenuOpen.value = false
      selectedNodeId.value = null
      undoStack.length = 0; redoStack.length = 0
      nextTick(pushUndo)
      showStatus(`Loaded preset: ${preset.name}`)
    }

    // Load blocks and pipes into Drawflow + our refs
    function loadBlocksIntoDrawflow(blocks, pipes) {
      _programmaticChange = true
      editor.clear()
      Object.keys(dfIdToUid).forEach(k => delete dfIdToUid[k])
      Object.keys(uidToDfId).forEach(k => delete uidToDfId[k])

      for (const b of blocks) {
        const fittings = getBlockFittings(catalog.value, b.block_type)
        const html = nodeHtml(b.block_type, b.label, 'idle', b.disabled)
        const dfId = editor.addNode(
          b.block_type, fittings.input.length, fittings.output.length,
          b.position?.x || 0, b.position?.y || 0, b.block_type, {}, html
        )
        dfIdToUid[dfId] = b.uid
        uidToDfId[b.uid] = dfId
        if (b.disabled) {
          const el = document.querySelector(`#node-${dfId}`)
          if (el) el.classList.add('df-disabled')
        }
      }

      for (const pipe of pipes) {
        const srcDfId = uidToDfId[pipe.source_block_uid]
        const tgtDfId = uidToDfId[pipe.target_block_uid]
        if (srcDfId == null || tgtDfId == null) continue
        const srcBlock = blocks.find(b => b.uid === pipe.source_block_uid)
        const tgtBlock = blocks.find(b => b.uid === pipe.target_block_uid)
        const srcFittings = getBlockFittings(catalog.value, srcBlock?.block_type).output
        const tgtFittings = getBlockFittings(catalog.value, tgtBlock?.block_type).input
        const outputClass = fittingUidToPort(srcFittings, pipe.source_fitting_uid, 'output')
        const inputClass = fittingUidToPort(tgtFittings, pipe.target_fitting_uid, 'input')
        editor.addConnection(srcDfId, tgtDfId, outputClass, inputClass)
      }
      _programmaticChange = false

      nodes.value = blocks.map(b => ({
        id: b.uid, position: { x: b.position?.x || 0, y: b.position?.y || 0 },
        data: { label: b.label, blockType: b.block_type, config: b.config || {}, status: 'idle', disabled: !!b.disabled },
      }))

      Object.keys(edgeFieldMappings).forEach(k => delete edgeFieldMappings[k])
      edges.value = pipes.map(p => {
        if (p.field_mapping) edgeFieldMappings[_fmKey(p.source_block_uid, p.target_block_uid)] = p.field_mapping
        return {
          id: p.uid, source: p.source_block_uid, target: p.target_block_uid,
          sourceHandle: p.source_fitting_uid, targetHandle: p.target_fitting_uid,
        }
      })

      // Apply port colors and edge colors after DOM renders
      nextTick(() => {
        for (const b of blocks) {
          const dfId = uidToDfId[b.uid]
          if (dfId != null) {
            const catEntry = catalog.value.find(c => c.block_type === b.block_type)
            applyPortColors(editor, dfId, getBlockFittings(catalog.value, b.block_type), catEntry)
          }
        }
        for (const pipe of pipes) {
          const srcDfId = uidToDfId[pipe.source_block_uid]
          const tgtDfId = uidToDfId[pipe.target_block_uid]
          if (srcDfId != null && tgtDfId != null) {
            const srcBlock = blocks.find(b => b.uid === pipe.source_block_uid)
            const srcFittings = getBlockFittings(catalog.value, srcBlock?.block_type).output
            const outputClass = fittingUidToPort(srcFittings, pipe.source_fitting_uid, 'output')
            const tgtBlock = blocks.find(b => b.uid === pipe.target_block_uid)
            const tgtFittings = getBlockFittings(catalog.value, tgtBlock?.block_type).input
            const inputClass = fittingUidToPort(tgtFittings, pipe.target_fitting_uid, 'input')
            applyEdgeColor(editor, srcDfId, tgtDfId, outputClass, inputClass, catalog.value, nodes.value, dfIdToUid)
          }
        }
      })
    }

    async function savePipeline() {
      saving.value = true
      // Read current positions from Drawflow
      const exported = editor.export()
      const dfData = exported.drawflow.Home.data
      for (const [dfIdStr, dfNode] of Object.entries(dfData)) {
        const blockUid = dfIdToUid[parseInt(dfIdStr)]
        const node = nodes.value.find(n => n.id === blockUid)
        if (node) node.position = { x: Math.round(dfNode.pos_x), y: Math.round(dfNode.pos_y) }
      }

      const payload = {
        name: pipelineName.value,
        blocks: nodesToBlocks(nodes.value),
        pipes: edgesToPipes(edges.value, edgeFieldMappings),
        owner_id: IMAGINARY_OWNER, owner_type: 'user', tags: [],
      }
      try {
        if (pipelineId.value) {
          payload.id = pipelineId.value
          const result = await api('PUT', `/pipelines/${pipelineId.value}`, payload)
          showStatus(`Saved v${result.version}`)
        } else {
          const result = await api('POST', '/pipelines', payload)
          pipelineId.value = result.id || result._id
          window.history.replaceState(null, '', `/editor/${pipelineId.value}`)
          showStatus('Created pipeline')
        }
        const timer = nodes.value.find(n => n.data.blockType === 'timer_trigger')
        const cfg = timer?.data?.config || {}
        hasSchedule.value = !!(parseInt(cfg.interval_seconds) > 0 || (cfg.cron_expression || '').trim())
      } catch (err) { showStatus(`Error: ${err.message}`) }
      saving.value = false
    }

    async function loadPipeline(id) {
      try {
        const p = await api('GET', `/pipelines/${id}`)
        pipelineName.value = p.name
        pipelineId.value = p.id || p._id
        loadBlocksIntoDrawflow(p.blocks || [], p.pipes || [])
        undoStack.length = 0; redoStack.length = 0
        nextTick(pushUndo)
        const timer = (p.blocks || []).find(b => b.block_type === 'timer_trigger')
        const cfg = timer?.config || {}
        hasSchedule.value = !!(parseInt(cfg.interval_seconds) > 0 || (cfg.cron_expression || '').trim())
        subscribePipelineEvents(pipelineId.value)
      } catch (err) { showStatus(`Failed to load: ${err.message}`) }
    }

    function subscribePipelineEvents(pid) {
      if (pipelineEvtSource) { pipelineEvtSource.close(); pipelineEvtSource = null }
      pipelineEvtSource = new EventSource(`/api/pipelines/${pid}/events`)
      for (const evtType of ['start', 'block_start', 'block_done', 'error', 'done']) {
        pipelineEvtSource.addEventListener(evtType, (e) => {
          try {
            const data = JSON.parse(e.data)
            // For 'start' events from scheduler, reset UI if not already running
            if (evtType === 'start' && !running.value) {
              running.value = true; runLog.value = []; runOutput.value = null
              consoleTab.value = 'log'
              resetAllNodeStatuses()
            }
            handleRunEvent(evtType, data)
            if (evtType === 'done') running.value = false
          } catch {}
        })
      }
      pipelineEvtSource.onerror = () => {
        // Reconnect after a brief delay
        pipelineEvtSource.close()
        pipelineEvtSource = null
        setTimeout(() => { if (pipelineId.value === pid) subscribePipelineEvents(pid) }, 3000)
      }
    }

    function setNodeStatus(nodeId, status) {
      const node = nodes.value.find(n => n.id === nodeId)
      if (node) node.data = { ...node.data, status }
      // Update Drawflow DOM
      const dfId = uidToDfId[nodeId]
      if (dfId != null) {
        const el = document.getElementById(`node-${dfId}`)
        if (el) {
          const sc = { running: 'var(--p-node-running)', completed: 'var(--p-node-completed)', failed: 'var(--p-node-failed)', idle: 'var(--p-node-idle)' }
          const statusEl = el.querySelector('.block-node__status')
          if (statusEl) statusEl.style.background = sc[status] || sc.idle
          // Apply status class on the drawflow-node wrapper for border styling
          el.classList.remove('df-status--running', 'df-status--completed', 'df-status--failed')
          if (status !== 'idle') el.classList.add(`df-status--${status}`)
        }
      }
    }
    function resetAllNodeStatuses() {
      nodes.value = nodes.value.map(n => ({ ...n, data: { ...n.data, status: 'idle', errorFields: [] } }))
      // Reset all Drawflow node DOMs
      for (const dfIdStr of Object.keys(dfIdToUid)) {
        const el = document.getElementById(`node-${dfIdStr}`)
        if (el) {
          const statusEl = el.querySelector('.block-node__status')
          if (statusEl) statusEl.style.background = 'var(--p-node-idle)'
          el.classList.remove('df-status--running', 'df-status--completed', 'df-status--failed')
        }
      }
    }

    async function runPipeline() {
      if (nodes.value.length === 0) { showStatus('No blocks to run'); return }

      // 1. Save pipeline first (ensure backend has latest version)
      let pid = pipelineId.value
      try {
        const payload = { name: pipelineName.value, blocks: nodesToBlocks(nodes.value), pipes: edgesToPipes(edges.value, edgeFieldMappings) }
        if (pid) {
          await api('PUT', `/pipelines/${pid}`, payload)
        } else {
          const created = await api('POST', '/pipelines', payload)
          pid = created.id
          pipelineId.value = pid
          router.replace({ name: 'editor', params: { id: pid } })
        }
      } catch (err) {
        showStatus(`Save failed: ${err.message}`); return
      }

      // 2. Trigger run via backend
      running.value = true; runLog.value = []; runOutput.value = null
      consoleTab.value = 'log'
      resetAllNodeStatuses()

      let runId
      try {
        const result = await api('POST', `/pipelines/${pid}/run`)
        runId = result.run_id
      } catch (err) {
        runLog.value.push({ type: 'error', message: `Trigger failed: ${err.message}`, ts: new Date().toISOString() })
        running.value = false; return
      }

      // 3. Events arrive via pipeline-level SSE (already subscribed in loadPipeline).
      //    If no pipeline subscription exists, fall back to run-level SSE.
      if (!pipelineEvtSource) {
        try {
          const evtSource = new EventSource(`/api/runs/${runId}/events`)
          evtSource.addEventListener('connected', () => {})
          for (const evtType of ['start', 'block_start', 'block_done', 'error', 'done']) {
            evtSource.addEventListener(evtType, (e) => {
              try { handleRunEvent(evtType, JSON.parse(e.data)) } catch {}
              if (evtType === 'done') { evtSource.close(); running.value = false }
            })
          }
          evtSource.onerror = () => {
            evtSource.close()
            if (running.value) {
              runLog.value.push({ type: 'error', message: 'Event stream disconnected', ts: new Date().toISOString() })
              running.value = false
            }
          }
        } catch (err) {
          runLog.value.push({ type: 'error', message: `SSE failed: ${err.message}`, ts: new Date().toISOString() })
          running.value = false
        }
      }
    }

    function handleRunEvent(type, data) {
      const ts = new Date().toISOString()
      switch (type) {
        case 'start':
          runLog.value.push({ type: 'info', message: `Starting pipeline (${data.total} blocks)`, ts }); break
        case 'block_start':
          setNodeStatus(data.block_uid, 'running')
          runLog.value.push({ type: 'info', message: `Running: ${data.label} (${data.block_type})`, ts, block_uid: data.block_uid })
          // Highlight incoming edges as "running"
          for (const e of edges.value) {
            if (e.target === data.block_uid) flashEdge(editor, uidToDfId, e.source, e.target, 'running')
          }
          break
        case 'block_done':
          setNodeStatus(data.block_uid, data.status)
          // Flash edges: transition incoming + outgoing to completed/failed with 2s fade
          for (const e of edges.value) {
            if (e.target === data.block_uid || e.source === data.block_uid) {
              const st = data.status === 'completed' ? 'completed' : 'failed'
              flashEdge(editor, uidToDfId, e.source, e.target, st)
              const src = e.source, tgt = e.target
              setTimeout(() => flashEdge(editor, uidToDfId, src, tgt, null), 2000)
            }
          }
          if (data.status === 'completed') {
            const dur = data.duration_ms < 1000 ? `${data.duration_ms}ms` : `${(data.duration_ms / 1000).toFixed(1)}s`
            const pc = data.parcel_count > 1 ? ` (${data.parcel_count} parcels)` : ''
            runLog.value.push({ type: 'success', message: `Done: ${data.label} in ${dur}${pc}`, ts, block_uid: data.block_uid, output: data.output })
          } else {
            const errorFields = data.error_fields || []
            const hints = errorFields.map(f => f.hint).filter(Boolean)
            let msg = data.error
            if (hints.length) msg += ` — ${hints[0]}`

            // Try to build a quickfix: auto-map missing fields from upstream
            let quickfix = null
            const missingFields = errorFields.filter(f => f.message?.includes('required')).map(f => f.field)
            if (missingFields.length > 0) {
              const incomingEdges = edges.value.filter(e => e.target === data.block_uid)
              if (incomingEdges.length > 0) {
                const fixes = {}
                for (const mf of missingFields) {
                  for (const edge of incomingEdges) {
                    const srcNode = nodes.value.find(n => n.id === edge.source)
                    if (!srcNode) continue
                    const srcEntry = catalog.value.find(c => c.block_type === srcNode.data.blockType)
                    const srcFields = Object.keys(srcEntry?.output_schema?.properties || {})
                    if (srcFields.length > 0 && !srcFields.includes(mf)) {
                      // Map the missing target field to the best source field
                      const match = srcFields.find(sf => srcEntry.output_schema.properties[sf].type === 'string') || srcFields[0]
                      fixes[mf] = { fmKey: _fmKey(edge.source, edge.target), sourceField: match, sourceLabel: srcNode.data.label }
                    }
                  }
                }
                if (Object.keys(fixes).length > 0) {
                  const desc = Object.entries(fixes).map(([tf, v]) => `"${tf}" ← "${v.sourceField}" from ${v.sourceLabel}`).join(', ')
                  quickfix = { fixes, description: `Auto-fix: map ${desc}` }
                }
              }
            }

            runLog.value.push({
              type: 'error', message: msg, ts,
              block_uid: data.block_uid,
              error_fields: errorFields.map(f => f.field),
              quickfix,
            })
            // Auto-select the failed block
            selectedNodeId.value = data.block_uid
            // Mark error fields for highlighting
            const node = nodes.value.find(n => n.id === data.block_uid)
            if (node) {
              node.data = { ...node.data, errorFields: errorFields.map(f => f.field) }
            }
          }
          break
        case 'error':
          if (!data.block_uid || data.message === runLog.value[runLog.value.length - 1]?.message) break // skip duplicate
          runLog.value.push({ type: 'error', message: data.message, ts, block_uid: data.block_uid }); break
        case 'done': {
          const td = data.total_ms < 1000 ? `${data.total_ms}ms` : `${(data.total_ms / 1000).toFixed(1)}s`
          runLog.value.push({ type: 'success', message: `Pipeline complete: ${data.blocks_run} blocks in ${td}`, ts })
          runOutput.value = data.output; consoleTab.value = 'output'
          // Stop any 'running' flashes
          if (editor) {
            editor.container.querySelectorAll('.edge-flash--running').forEach(el => el.classList.remove('edge-flash--running'))
          }
          // Scheduled repeats are handled by the backend scheduler
          break
        }
      }
    }

    let drawflowContainer = null

    function initDrawflow() {
      if (!drawflowContainer || editor) return
      editor = new DrawflowLib(drawflowContainer)
      editor.reroute = false
      editor.curvature = 0.5
      editor.start()

      // --- Drawflow events ---
      editor.on('nodeSelected', (id) => {
        clearTimeout(_unselectTimer)
        selectedNodeId.value = dfIdToUid[id] || null
        selectedEdgeId.value = null
      })
      editor.on('nodeUnselected', () => {
        _unselectTimer = setTimeout(() => {
          selectedNodeId.value = null
          selectedEdgeId.value = null
        }, 50)
      })
      editor.on('connectionCreated', ({ output_id, input_id, output_class, input_class }) => {
        if (_programmaticChange) return
        const sourceUid = dfIdToUid[output_id]
        const targetUid = dfIdToUid[input_id]
        if (!sourceUid || !targetUid) return
        const srcNode = nodes.value.find(n => n.id === sourceUid)
        const tgtNode = nodes.value.find(n => n.id === targetUid)
        const srcFittings = getBlockFittings(catalog.value, srcNode?.data.blockType).output
        const tgtFittings = getBlockFittings(catalog.value, tgtNode?.data.blockType).input
        const sourceHandle = portToFittingUid(srcFittings, output_class)
        const targetHandle = portToFittingUid(tgtFittings, input_class)
        const edgeId = uid()
        edges.value = [...edges.value, {
          id: edgeId, source: sourceUid, target: targetUid, sourceHandle, targetHandle,
        }]
        const fm = inferFieldMapping(sourceUid, targetUid)
        if (fm) edgeFieldMappings[_fmKey(sourceUid, targetUid)] = fm
        applyEdgeColor(editor, output_id, input_id, output_class, input_class, catalog.value, nodes.value, dfIdToUid)
        pushUndo()
      })
      editor.on('connectionRemoved', ({ output_id, input_id }) => {
        if (_programmaticChange) return
        const sourceUid = dfIdToUid[output_id]
        const targetUid = dfIdToUid[input_id]
        if (!sourceUid || !targetUid) return
        edges.value = edges.value.filter(e => !(e.source === sourceUid && e.target === targetUid))
        delete edgeFieldMappings[_fmKey(sourceUid, targetUid)]
        pushUndo()
      })
      editor.on('nodeMoved', (id) => {
        const blockUid = dfIdToUid[id]
        if (!blockUid) return
        const node = nodes.value.find(n => n.id === blockUid)
        const dfNode = editor.getNodeFromId(id)
        if (node && dfNode) {
          node.position = { x: Math.round(dfNode.pos_x), y: Math.round(dfNode.pos_y) }
        }
      })
      editor.on('nodeRemoved', (id) => {
        if (_programmaticChange) return
        const blockUid = dfIdToUid[id]
        if (!blockUid) return
        delete dfIdToUid[id]
        delete uidToDfId[blockUid]
        nodes.value = nodes.value.filter(n => n.id !== blockUid)
        edges.value = edges.value.filter(e => e.source !== blockUid && e.target !== blockUid)
        if (selectedNodeId.value === blockUid) selectedNodeId.value = null
        pushUndo()
      })
      editor.on('connectionSelected', ({ output_id, input_id }) => {
        const sourceUid = dfIdToUid[output_id]
        const targetUid = dfIdToUid[input_id]
        const edge = edges.value.find(e => e.source === sourceUid && e.target === targetUid)
        if (edge) { selectedEdgeId.value = edge.id; selectedNodeId.value = null }
      })

      // Double-click on node opens primary field editor
      drawflowContainer.addEventListener('dblclick', (e) => {
        const nodeEl = e.target.closest('.drawflow-node')
        if (nodeEl) {
          const dfId = parseInt(nodeEl.id.slice(5))
          const blockUid = dfIdToUid[dfId]
          if (blockUid) {
            const node = nodes.value.find(n => n.id === blockUid)
            if (node) onNodeDoubleClick(node)
          }
        }
      })

      // Right-click context menu on nodes
      drawflowContainer.addEventListener('contextmenu', (e) => {
        const nodeEl = e.target.closest('.drawflow-node')
        if (nodeEl) {
          e.preventDefault()
          const dfId = parseInt(nodeEl.id.slice(5))
          const blockUid = dfIdToUid[dfId]
          if (blockUid) {
            ctxMenu.value = { x: e.clientX, y: e.clientY, nodeId: blockUid }
          }
        }
      })

      // Close context menu on click anywhere
      document.addEventListener('click', () => { ctxMenu.value = null })
    }

    onMounted(async () => {
      try { catalog.value = await api('GET', '/blocks') } catch {}
      try { llmDefaults.value = await api('GET', '/llm-defaults') } catch {}
      try { globalVars.value = await api('GET', '/blocks/variables') } catch {}

      // Initialize Drawflow after DOM is ready
      nextTick(() => {
        initDrawflow()
        const route = router.currentRoute.value
        if (route.params.id) loadPipeline(route.params.id)
      })
    })
    Vue.onUnmounted(() => {
      if (pipelineEvtSource) { pipelineEvtSource.close(); pipelineEvtSource = null }
      if (editor) { editor.clear(); editor = null }
    })

    // --- Helpers: upstream output fields ---

    function getUpstreamFields(nodeId) {
      // Find all blocks connected upstream of this node, collect their output fields
      const fields = []
      for (const edge of edges.value) {
        if (edge.target !== nodeId) continue
        const sourceNode = nodes.value.find(n => n.id === edge.source)
        if (!sourceNode) continue
        const entry = catalog.value.find(c => c.block_type === sourceNode.data.blockType)
        if (!entry?.output_schema?.properties) continue
        const mapping = edgeFieldMappings[_fmKey(edge.source, edge.target)] || null
        for (const [fname, fprop] of Object.entries(entry.output_schema.properties)) {
          // If there's a field_mapping, show the mapped name
          let mappedName = fname
          if (mapping) {
            const mapped = Object.entries(mapping).find(([, src]) => src === fname)
            if (mapped) mappedName = mapped[0]
            else continue // field not mapped through
          }
          fields.push({
            name: fname,
            mappedName,
            title: fprop.title || fname,
            type: fprop.type || 'any',
            description: fprop.description || '',
            sourceLabel: sourceNode.data.label,
            sourceType: sourceNode.data.blockType,
          })
        }
      }
      return fields
    }

    function getOutputFields(nodeId) {
      const node = nodes.value.find(n => n.id === nodeId)
      if (!node) return []
      const entry = catalog.value.find(c => c.block_type === node.data.blockType)
      if (!entry?.output_schema?.properties) return []
      return Object.entries(entry.output_schema.properties).map(([fname, fprop]) => ({
        name: fname,
        title: fprop.title || fname,
        type: fprop.type || 'any',
        description: fprop.description || '',
      }))
    }

    // --- Variable autocomplete helpers ---
    function getAllVariables(nodeId) {
      // Global vars + upstream output fields
      const vars = globalVars.value.map(v => ({ ...v, source: 'global', icon: 'public' }))
      const upstream = getUpstreamFields(nodeId)
      for (const f of upstream) {
        vars.push({ name: f.mappedName || f.name, type: f.type, description: `from ${f.sourceLabel}`, source: 'upstream', icon: 'input' })
      }
      return vars
    }

    function showVarAutocomplete(el, nodeId) {
      const val = el.value
      const pos = el.selectionStart || 0
      // Find the opening { before cursor
      const before = val.slice(0, pos)
      const braceIdx = before.lastIndexOf('{')
      if (braceIdx === -1) { varAutocomplete.visible = false; return }
      // Check there's no } between brace and cursor
      if (before.indexOf('}', braceIdx) !== -1) { varAutocomplete.visible = false; return }
      const prefix = before.slice(braceIdx + 1).toLowerCase()
      const all = getAllVariables(nodeId)
      const filtered = prefix ? all.filter(v => v.name.toLowerCase().includes(prefix)) : all
      if (filtered.length === 0) { varAutocomplete.visible = false; return }
      // Position dropdown below input
      const rect = el.getBoundingClientRect()
      varAutocomplete.visible = true
      varAutocomplete.items = filtered
      varAutocomplete.x = rect.left
      varAutocomplete.y = rect.bottom + 2
      varAutocomplete.inputEl = el
      varAutocomplete.selected = 0
      varAutocomplete.prefix = prefix
      varAutocomplete.braceIdx = braceIdx
    }

    function insertVariable(varName) {
      const el = varAutocomplete.inputEl
      if (!el) return
      const pos = el.selectionStart || 0
      const val = el.value
      const braceIdx = varAutocomplete.braceIdx
      // Replace from { to cursor with {varName}
      const newVal = val.slice(0, braceIdx) + '{' + varName + '}' + val.slice(pos)
      // Trigger input event
      const nativeInputValueSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set
        || Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value')?.set
      if (nativeInputValueSetter) {
        nativeInputValueSetter.call(el, newVal)
        el.dispatchEvent(new Event('input', { bubbles: true }))
      }
      varAutocomplete.visible = false
      nextTick(() => {
        el.focus()
        const newPos = braceIdx + varName.length + 2
        el.setSelectionRange(newPos, newPos)
      })
    }

    function handleVarKeydown(e) {
      if (!varAutocomplete.visible) return
      if (e.key === 'ArrowDown') {
        e.preventDefault()
        varAutocomplete.selected = Math.min(varAutocomplete.selected + 1, varAutocomplete.items.length - 1)
      } else if (e.key === 'ArrowUp') {
        e.preventDefault()
        varAutocomplete.selected = Math.max(varAutocomplete.selected - 1, 0)
      } else if (e.key === 'Enter' || e.key === 'Tab') {
        if (varAutocomplete.items.length > 0) {
          e.preventDefault()
          insertVariable(varAutocomplete.items[varAutocomplete.selected].name)
        }
      } else if (e.key === 'Escape') {
        varAutocomplete.visible = false
      }
    }

    // --- Render: single config field ---
    function renderField(node, key, prop, val) {
      const type = prop.type
      const widget = prop.widget || (type === 'boolean' ? 'toggle' : type === 'integer' || type === 'number' ? 'number' : 'text')

      function setVal(v) {
        node.data = { ...node.data, config: { ...node.data.config, [key]: v } }
      }

      // Toggle
      if (widget === 'toggle') {
        return h('label', { class: 'popout-toggle' }, [
          h('input', { type: 'checkbox', checked: val ?? prop.default ?? false, onChange: (e) => setVal(e.target.checked) }),
          h('span', null, prop.title || key),
        ])
      }

      // Select
      if (widget === 'select' && prop.options) {
        return h('select', {
          class: 'config-input', value: val ?? prop.default ?? '',
          onChange: (e) => setVal(e.target.value),
        }, prop.options.map(opt => h('option', { value: opt }, opt)))
      }

      // Combobox — select dropdown + optional custom value
      if (widget === 'combobox') {
        let options = prop.options || []
        // Resolve effective default from LLM tier config
        let effectiveDefault = prop.default ?? ''
        if (prop.options_ref === 'llm_models' || (Array.isArray(options) && options.includes('anthropic') && options.includes('openai'))) {
          const catalogEntry = catalog.value.find(c => c.block_type === node.data.blockType)
          const tier = catalogEntry?.llm_tier || 'medium'
          const tierConf = llmDefaults.value.tiers?.[tier]
          if (key === 'provider' && tierConf) effectiveDefault = tierConf.provider
          if (key === 'model' && tierConf) effectiveDefault = tierConf.model
        }
        // For model fields, resolve options from provider-specific model list
        if (prop.options_ref === 'llm_models') {
          const providerVal = node.data.config.provider || effectiveDefault || 'anthropic'
          options = llmDefaults.value.models?.[providerVal] || []
        }
        const currentVal = val ?? effectiveDefault
        const isCustom = currentVal && !options.includes(currentVal)
        const editingCustom = node.data._comboCustom?.[key]

        if (editingCustom) {
          return h('div', { class: 'combobox-wrap' }, [
            h('input', {
              class: 'config-input', type: 'text', autofocus: true,
              value: currentVal, placeholder: 'Type custom value...',
              onInput: (e) => setVal(e.target.value),
              onBlur: () => {
                const cc = { ...(node.data._comboCustom || {}) }; delete cc[key]
                node.data = { ...node.data, _comboCustom: cc }
              },
              onKeydown: (e) => { if (e.key === 'Escape' || e.key === 'Enter') e.target.blur() },
            }),
          ])
        }

        const selectOptions = [...options.map(opt => h('option', { value: opt }, opt))]
        if (isCustom) selectOptions.unshift(h('option', { value: currentVal }, currentVal))
        selectOptions.push(h('option', { value: '__custom__' }, 'Other…'))

        return h('select', {
          class: 'config-input', value: currentVal,
          onChange: (e) => {
            if (e.target.value === '__custom__') {
              const cc = { ...(node.data._comboCustom || {}), [key]: true }
              node.data = { ...node.data, _comboCustom: cc }
              return
            }
            setVal(e.target.value)
            // When provider changes, reset model to first option for new provider
            if (key === 'provider') {
              const models = llmDefaults.value.models?.[e.target.value] || []
              if (models.length > 0) {
                node.data = { ...node.data, config: { ...node.data.config, provider: e.target.value, model: models[0] } }
              }
            }
          },
        }, selectOptions)
      }

      // Multiline — show preview + open modal button
      if (isMultilineField(key, prop, val)) {
        const preview = typeof val === 'object' ? JSON.stringify(val, null, 2) : (val ?? prop.default ?? '')
        const truncated = typeof preview === 'string' && preview.length > 60 ? preview.slice(0, 60) + '...' : preview
        return h('div', {
          class: 'config-input config-input--multiline',
          onClick: () => {
            textModal.value = {
              nodeId: node.id, key, title: prop.title || key,
              value: typeof val === 'object' ? JSON.stringify(val, null, 2) : (val ?? prop.default ?? ''),
              upstreamFields: getUpstreamFields(node.id),
            }
          },
        }, [
          h('span', { style: 'flex:1; overflow:hidden; text-overflow:ellipsis; white-space:nowrap' }, truncated || prop.description || 'Click to edit...'),
          h('span', { class: 'material-icons', style: 'font-size:14px; opacity:0.5; flex-shrink:0' }, 'open_in_new'),
        ])
      }

      // Number
      if (widget === 'number') {
        return h('input', {
          class: 'config-input', type: 'number', placeholder: prop.description || '',
          value: val ?? prop.default ?? '', onInput: (e) => setVal(Number(e.target.value)),
        })
      }

      // Secret — password field
      if (prop.secret) {
        return h('input', {
          class: 'config-input', type: 'password', placeholder: prop.description || 'Set via environment',
          value: val ?? '', onInput: (e) => setVal(e.target.value),
        })
      }

      // Default text (with variable autocomplete)
      return h('input', {
        class: 'config-input', type: 'text', placeholder: prop.placeholder || prop.description || '',
        value: val ?? prop.default ?? '',
        onInput: (e) => { setVal(e.target.value); showVarAutocomplete(e.target, node.id) },
        onKeydown: handleVarKeydown,
        onBlur: () => setTimeout(() => { varAutocomplete.visible = false }, 200),
        onFocus: (e) => { if (e.target.value.includes('{')) showVarAutocomplete(e.target, node.id) },
      })
    }

    // --- Render: config popout ---
    function renderConfigPopout() {
      const node = selectedNode.value
      if (!node) return null

      const data = node.data
      const catalogEntry = catalog.value.find(c => c.block_type === data.blockType)
      const inputSchema = catalogEntry?.input_schema || {}
      const properties = inputSchema.properties || {}
      const allKeys = Object.keys(properties)

      const userKeys = allKeys.filter(k => !isInfraField(k, properties[k]))
      const infraKeys = allKeys.filter(k => isInfraField(k, properties[k]))

      return h('div', { class: 'config-popout' }, [
        // Header
        h('div', { class: 'config-popout__header' }, [
          h('span', { class: 'material-icons', style: 'color:var(--p-primary); font-size:22px' }, blockIcon(data.blockType)),
          h('div', { style: 'flex:1; min-width:0' }, [
            h('div', { class: 'config-popout__title' }, data.blockType.replace(/_/g, ' ')),
            catalogEntry?.description ? h('div', { class: 'config-popout__desc' }, catalogEntry.description) : null,
          ]),
          h('button', {
            class: 'icon-btn', title: data.disabled ? 'Enable block' : 'Disable block',
            onClick: () => toggleBlockDisabled(node.id),
          }, [h('span', { class: 'material-icons', style: `font-size:16px; color:${data.disabled ? 'var(--p-node-completed)' : 'var(--p-text-secondary)'}` }, data.disabled ? 'check_circle' : 'block')]),
          h('button', { class: 'icon-btn', title: 'Delete block', onClick: () => deleteNode(node.id) },
            [h('span', { class: 'material-icons', style: 'font-size:16px; color:var(--p-node-failed)' }, 'delete')]),
          h('button', { class: 'icon-btn', title: 'Close', onClick: () => { selectedNodeId.value = null } },
            [h('span', { class: 'material-icons', style: 'font-size:16px' }, 'close')]),
        ]),

        // Label
        h('div', { class: 'config-popout__section' }, [
          h('div', { class: 'config-label' }, 'Label'),
          h('input', {
            class: 'config-input', value: data.label,
            onInput: (e) => {
              node.data = { ...node.data, label: e.target.value }
              // Sync label to Drawflow DOM
              const dfId = uidToDfId[node.id]
              if (dfId != null) {
                const labelEl = document.querySelector(`#node-${dfId} .block-node__label`)
                if (labelEl) labelEl.textContent = e.target.value
              }
            },
          }),
        ]),

        // User-facing config fields
        userKeys.length > 0 ? h('div', { class: 'config-popout__section' },
          userKeys.map(key => {
            const hasError = (data.errorFields || []).includes(key)

            // Check if this field is "wired" via an incoming pipe mapping
            let wiredFrom = null
            let fix = null
            const incomingEdges = edges.value.filter(e => e.target === node.id)
            for (const edge of incomingEdges) {
              const fmk = _fmKey(edge.source, edge.target)
              const fm = edgeFieldMappings[fmk] || {}
              const srcNode = nodes.value.find(n => n.id === edge.source)
              if (!srcNode) continue
              if (fm[key]) {
                wiredFrom = { fmKey: fmk, sourceField: fm[key], sourceLabel: srcNode.data.label }
                break
              }
              // Offer fix if field has error and is not wired
              if (hasError && !fix) {
                const srcEntry = catalog.value.find(c => c.block_type === srcNode.data.blockType)
                const srcProps = srcEntry?.output_schema?.properties
                if (srcProps) {
                  const srcFields = Object.keys(srcProps)
                  const tgtType = properties[key]?.type
                  const match = srcFields.find(sf => srcProps[sf].type === tgtType && !Object.values(fm).includes(sf))
                    || srcFields.find(sf => !Object.values(fm).includes(sf))
                  if (match) fix = { fmKey: fmk, sourceField: match, sourceLabel: srcNode.data.label }
                }
              }
            }

            return h('div', { key, class: hasError && !wiredFrom ? 'config-field--error' : '' }, [
              h('div', { class: 'config-label' }, [
                properties[key].title || key,
                hasError && !wiredFrom ? h('span', { class: 'config-field__error-icon material-icons', style: 'font-size:14px; color:var(--p-node-failed); margin-left:4px' }, 'error') : null,
              ]),
              // Show wired indicator OR the normal field input
              wiredFrom
                ? h('div', { class: 'config-field-wired' }, [
                    h('span', { class: 'material-icons', style: 'font-size:14px' }, 'cable'),
                    h('span', null, [
                      h('strong', null, `{${wiredFrom.sourceField}}`),
                      ` from ${wiredFrom.sourceLabel}`,
                    ]),
                    h('button', {
                      class: 'config-field-wired__remove',
                      title: 'Remove mapping (use manual value instead)',
                      onClick: () => {
                        const fm = { ...edgeFieldMappings[wiredFrom.fmKey] }
                        delete fm[key]
                        if (Object.keys(fm).length === 0) delete edgeFieldMappings[wiredFrom.fmKey]
                        else edgeFieldMappings[wiredFrom.fmKey] = fm
                        pushUndo()
                      },
                    }, [h('span', { class: 'material-icons', style: 'font-size:12px' }, 'close')]),
                  ])
                : renderField(node, key, properties[key], data.config[key]),
              // Offer fix if field has error and no wire
              !wiredFrom && fix ? h('button', {
                class: 'quickfix-inline',
                onClick: () => {
                  const existing = edgeFieldMappings[fix.fmKey] || {}
                  edgeFieldMappings[fix.fmKey] = { ...existing, [key]: fix.sourceField }
                  const ef = (node.data.errorFields || []).filter(f => f !== key)
                  node.data = { ...node.data, errorFields: ef }
                  pushUndo()
                  showStatus(`Mapped "${key}" ← "${fix.sourceField}" from ${fix.sourceLabel}`)
                },
              }, [
                h('span', { class: 'material-icons', style: 'font-size:13px' }, 'auto_fix_high'),
                ` Fix: use "${fix.sourceField}" from ${fix.sourceLabel}`,
              ]) : null,
            ])
          }),
        ) : null,

        // Output fields
        (() => {
          const outFields = getOutputFields(node.id)
          if (outFields.length === 0) return null
          return h('div', { class: 'config-popout__section' }, [
            h('div', { class: 'config-label', style: 'display:flex; align-items:center; gap:4px' }, [
              h('span', { class: 'material-icons', style: 'font-size:13px; color:var(--p-primary)' }, 'output'),
              'Output Fields',
            ]),
            h('div', { class: 'output-fields' },
              outFields.map(f => h('div', { class: 'output-field', key: f.name }, [
                h('code', { class: 'output-field__name' }, `{${f.name}}`),
                h('span', { class: 'output-field__type' }, f.type),
                f.title !== f.name ? h('span', { class: 'output-field__desc' }, f.title) : null,
              ])),
            ),
          ])
        })(),

        // Ports & connections — always shown
        (() => {
          const inF = catalogEntry?.input_fittings?.length
            ? catalogEntry.input_fittings
            : [{ uid: 'input', label: 'Input', color: '', description: '' }]
          const outF = catalogEntry?.output_fittings?.length
            ? catalogEntry.output_fittings
            : [{ uid: 'output', label: 'Output', color: '', description: '' }]
          const nodeEdges = edges.value.filter(e => e.source === node.id || e.target === node.id)
          const portRows = []
          for (const f of inF) {
            const conns = nodeEdges.filter(e => e.target === node.id && (e.targetHandle === f.uid || (!e.targetHandle && f.uid === 'input')))
            const connInfo = conns.map(e => ({ label: nodes.value.find(x => x.id === e.source)?.data?.label || e.source, edgeId: e.id }))
            portRows.push({ dir: 'in', fitting: f, connInfo })
          }
          for (const f of outF) {
            const conns = nodeEdges.filter(e => e.source === node.id && (e.sourceHandle === f.uid || (!e.sourceHandle && f.uid === 'output')))
            const connInfo = conns.map(e => ({ label: nodes.value.find(x => x.id === e.target)?.data?.label || e.target, edgeId: e.id }))
            portRows.push({ dir: 'out', fitting: f, connInfo })
          }
          // Get other blocks for the "add connection" dropdown
          const otherBlocks = nodes.value.filter(n => n.id !== node.id)

          return h('div', { class: 'config-popout__section' }, [
            h('div', { class: 'config-label', style: 'display:flex; align-items:center; gap:4px' }, [
              h('span', { class: 'material-icons', style: 'font-size:13px; color:var(--p-primary)' }, 'settings_input_component'),
              'Ports',
            ]),
            h('div', { class: 'port-list' }, portRows.map(r =>
              h('div', { class: 'port-row', key: r.dir + r.fitting.uid }, [
                h('span', {
                  class: 'port-dot',
                  style: `background:${r.fitting.color || 'var(--p-edge-default)'}`,
                }),
                h('span', { class: 'port-dir' }, r.dir === 'in' ? 'IN' : 'OUT'),
                h('span', { class: 'port-label' }, r.fitting.label),
                h('span', { class: 'port-conn' }, [
                  ...r.connInfo.map((c, i) =>
                    h('span', { key: i, class: 'port-conn__tag' }, [
                      h('span', { class: 'material-icons', style: 'font-size:10px' }, r.dir === 'in' ? 'arrow_back' : 'arrow_forward'),
                      c.label,
                      h('span', {
                        class: 'material-icons port-conn__remove',
                        title: 'Remove connection',
                        onClick: () => {
                          edges.value = edges.value.filter(e => e.id !== c.edgeId)
                          pushUndo()
                        },
                      }, 'close'),
                    ])
                  ),
                  r.connInfo.length === 0
                    ? h('span', { class: 'port-conn--none' }, 'none')
                    : null,
                  // + button to add connection
                  otherBlocks.length > 0
                    ? h('select', {
                        class: 'port-add-select',
                        value: '',
                        title: r.dir === 'out' ? 'Connect to...' : 'Connect from...',
                        onChange: (ev) => {
                          const targetId = ev.target.value
                          ev.target.value = ''
                          if (!targetId) return
                          const edgeId = `e-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`
                          if (r.dir === 'out') {
                            edges.value = [...edges.value, {
                              id: edgeId, source: node.id, target: targetId,
                              sourceHandle: r.fitting.uid, targetHandle: 'input',
                              type: 'smoothstep',
                            }]
                          } else {
                            edges.value = [...edges.value, {
                              id: edgeId, source: targetId, target: node.id,
                              sourceHandle: 'output', targetHandle: r.fitting.uid,
                              type: 'smoothstep',
                            }]
                          }
                          pushUndo()
                        },
                      }, [
                        h('option', { value: '' }, '+'),
                        ...otherBlocks.map(b =>
                          h('option', { value: b.id, key: b.id }, b.data.label || b.data.blockType)
                        ),
                      ])
                    : null,
                ]),
              ])
            )),
          ])
        })(),

        // Infrastructure toggle
        infraKeys.length > 0 ? h('div', { class: 'config-popout__section' }, [
          h('button', {
            class: 'config-infra-toggle',
            onClick: () => { showInfra.value = !showInfra.value },
          }, [
            h('span', { class: 'material-icons', style: 'font-size:14px' }, showInfra.value ? 'expand_less' : 'expand_more'),
            `Advanced (${infraKeys.length})`,
          ]),
          showInfra.value ? h('div', { class: 'config-infra-fields' },
            infraKeys.map(key => h('div', { key }, [
              h('div', { class: 'config-label' }, properties[key].title || key),
              renderField(node, key, properties[key], data.config[key]),
            ])),
          ) : null,
        ]) : null,
      ])
    }

    // --- Render: text editor modal ---
    let _textareaRef = null

    function insertAtCursor(text) {
      if (!_textareaRef) return
      const ta = _textareaRef
      const start = ta.selectionStart
      const end = ta.selectionEnd
      const before = ta.value.substring(0, start)
      const after = ta.value.substring(end)
      const newVal = before + text + after
      textModal.value = { ...textModal.value, value: newVal }
      nextTick(() => {
        ta.focus()
        ta.selectionStart = ta.selectionEnd = start + text.length
      })
    }

    function renderTextModal() {
      if (!textModal.value) return null
      const m = textModal.value
      const fields = m.upstreamFields || []
      const gvars = globalVars.value || []
      const hasFields = fields.length > 0 || gvars.length > 0

      return h('div', { class: 'text-modal-overlay' }, [
        h('div', { class: 'text-modal', style: hasFields ? 'display:flex; flex-direction:row; gap:0' : '' }, [
          // Main editor area
          h('div', { style: 'flex:1; display:flex; flex-direction:column; min-width:0' }, [
            h('div', { class: 'text-modal__header' }, [
              h('span', { style: 'font-weight:600; font-size:13px' }, m.title),
              h('div', { style: 'flex:1' }),
              h('button', { class: 'toolbar-btn toolbar-btn--primary', onClick: () => {
                const node = nodes.value.find(n => n.id === m.nodeId)
                if (node) {
                  let v = m.value
                  try { v = JSON.parse(v) } catch {}
                  node.data = { ...node.data, config: { ...node.data.config, [m.key]: v } }
                }
                textModal.value = null
              } }, 'Save'),
              h('button', { class: 'toolbar-btn toolbar-btn--ghost', onClick: () => { textModal.value = null } }, 'Cancel'),
            ]),
            h('textarea', {
              class: 'text-modal__editor',
              value: m.value,
              onInput: (e) => { textModal.value = { ...m, value: e.target.value } },
              ref: (el) => { _textareaRef = el },
              autofocus: true,
            }),
          ]),

          // Variables sidebar
          hasFields ? h('div', { class: 'text-modal__fields' }, [
            h('div', { class: 'text-modal__fields-header' }, 'Variables'),
            h('div', { class: 'text-modal__fields-hint' }, 'Click to insert at cursor'),

            // Global variables
            gvars.length > 0 ? h('div', { class: 'text-modal__section-label' }, [
              h('span', { class: 'material-icons', style: 'font-size:12px' }, 'public'), ' Global',
            ]) : null,
            ...gvars.map(v => h('button', {
              key: `global-${v.name}`,
              class: 'text-modal__field-item',
              title: v.description,
              onClick: () => insertAtCursor(`{${v.name}}`),
            }, [
              h('code', { class: 'text-modal__field-name' }, `{${v.name}}`),
              h('div', { class: 'text-modal__field-meta' }, [
                h('span', { class: 'text-modal__field-type' }, v.type),
                h('span', { class: 'text-modal__field-source' }, v.description),
              ]),
            ])),

            // Upstream fields
            fields.length > 0 ? h('div', { class: 'text-modal__section-label', style: 'margin-top:8px' }, [
              h('span', { class: 'material-icons', style: 'font-size:12px' }, 'input'), ' Upstream',
            ]) : null,
            ...fields.map(f => h('button', {
              key: `${f.sourceLabel}-${f.name}`,
              class: 'text-modal__field-item',
              title: `${f.title} (${f.type}) from ${f.sourceLabel}`,
              onClick: () => insertAtCursor(`{${f.name}}`),
            }, [
              h('code', { class: 'text-modal__field-name' }, `{${f.name}}`),
              h('div', { class: 'text-modal__field-meta' }, [
                h('span', { class: 'text-modal__field-type' }, f.type),
                h('span', { class: 'text-modal__field-source' }, `← ${f.sourceLabel}`),
              ]),
              f.title !== f.name ? h('div', { class: 'text-modal__field-desc' }, f.title) : null,
            ])),
          ]) : null,
        ]),
      ])
    }

    return () => h('div', { style: 'display:flex; flex-direction:column; height:calc(100vh - 48px)' }, [
      // Editor toolbar
      h('div', { class: 'editor-toolbar' }, [
        h('input', {
          class: 'editor-toolbar__name', value: pipelineName.value,
          onInput: (e) => { pipelineName.value = e.target.value },
        }),
        statusMsg.value ? h('span', { style: 'font-size:11px; color:var(--p-text-secondary); margin-left:8px' }, statusMsg.value) : null,
        // Undo/Redo
        h('button', { class: 'icon-btn', title: 'Undo (⌘Z)', disabled: undoStack.length < 2, onClick: undo },
          [h('span', { class: 'material-icons', style: 'font-size:18px' }, 'undo')]),
        h('button', { class: 'icon-btn', title: 'Redo (⌘⇧Z)', disabled: redoStack.length === 0, onClick: redo },
          [h('span', { class: 'material-icons', style: 'font-size:18px' }, 'redo')]),
        h('div', { style: 'flex:1' }),
        // Presets
        h('div', { class: 'theme-dropdown' }, [
          h('button', { class: 'toolbar-btn toolbar-btn--ghost', onClick: () => { presetMenuOpen.value = !presetMenuOpen.value } }, [
            h('span', { class: 'material-icons', style: 'font-size:16px' }, 'science'), ' Presets',
          ]),
          presetMenuOpen.value && h('div', { class: 'theme-menu', style: 'min-width:260px' },
            PRESETS.map(p => h('button', { class: 'theme-menu__item', onClick: () => loadPreset(p) }, [
              h('div', null, [
                h('div', { style: 'font-weight:600' }, p.name),
                h('div', { style: 'font-size:10px; color:var(--p-text-secondary); margin-top:2px' }, p.description),
              ]),
            ])),
          ),
        ]),
        h('button', { class: 'toolbar-btn toolbar-btn--ghost', onClick: savePipeline, disabled: saving.value }, [
          h('span', { class: 'material-icons', style: 'font-size:16px' }, 'save'), saving.value ? ' Saving...' : ' Save',
        ]),
        h('button', {
          class: 'toolbar-btn toolbar-btn--primary', onClick: runPipeline,
          disabled: running.value || nodes.value.length === 0,
        }, [
          h('span', { class: 'material-icons', style: 'font-size:16px' }, running.value ? 'hourglass_empty' : 'play_arrow'),
          running.value ? ' Running...' : ' Run',
        ]),
        hasSchedule.value ? h('span', { class: 'auto-run-badge' }, [
          h('span', { class: 'material-icons', style: 'font-size:12px; margin-right:3px' }, 'timer'),
          'Scheduled',
        ]) : null,
      ]),

      // Body: sidebar + main area (canvas + console)
      h('div', { class: 'plumber-body' }, [
        // Sidebar
        h('div', { class: 'plumber-sidebar' }, [
          h('div', { class: 'search-wrapper' }, [
            h('span', { class: 'material-icons' }, 'search'),
            h('input', {
              class: 'search-input', placeholder: 'Search blocks...',
              value: search.value, onInput: (e) => { search.value = e.target.value },
            }),
          ]),
          ...Array.from(groupedCatalog.value.entries()).flatMap(([cat, entries]) => [
            h('div', { class: 'sidebar-heading' }, cat.replace(/\//g, ' / ')),
            ...entries.map(entry => h('div', {
              class: 'sidebar-item', draggable: true, title: entry.description,
              onDragstart: (e) => {
                e.dataTransfer.setData('application/plumber-block', JSON.stringify(entry))
                e.dataTransfer.effectAllowed = 'move'
              },
              onDblclick: () => addBlock(entry.block_type, entry.block_type.replace(/_/g, ' '), {}, 200 + Math.random() * 200, 100 + Math.random() * 300),
            }, [
              h('span', { class: 'material-icons sidebar-item__icon' }, blockIcon(entry.block_type)),
              h('span', null, entry.block_type.replace(/_/g, ' ')),
            ])),
          ]),
        ]),

        // Main area: canvas + console
        h('div', { class: 'plumber-main' }, [
        // Canvas
        h('div', { class: 'plumber-canvas', onDrop: onDrop, onDragover: onDragOver }, [
          nodes.value.length === 0 ? h('div', {
            style: 'position:absolute; inset:0; display:flex; align-items:center; justify-content:center; pointer-events:none; z-index:1',
          }, [h('div', { style: 'text-align:center; color: var(--p-text-secondary)' }, [
            h('span', { class: 'material-icons', style: 'font-size:48px; opacity:0.5' }, 'account_tree'),
            h('div', { style: 'margin-top:8px; font-size:14px' }, 'Drag blocks from the palette to start building'),
            h('div', { style: 'margin-top:4px; font-size:12px; opacity:0.6' }, `${catalog.value.length} blocks available · or use Presets`),
          ])]) : null,

          // Drawflow canvas container — initialized in onMounted
          h('div', {
            ref: (el) => {
              if (el && !drawflowContainer) {
                drawflowContainer = el
                nextTick(initDrawflow)
              }
            },
            style: 'width:100%; height:100%',
          }),

          // Config popout (floating over canvas)
          renderConfigPopout(),

          // Variable autocomplete dropdown
          varAutocomplete.visible ? h('div', {
            class: 'var-autocomplete',
            style: { position: 'fixed', left: varAutocomplete.x + 'px', top: varAutocomplete.y + 'px', zIndex: 9999 },
          }, varAutocomplete.items.map((v, i) =>
            h('div', {
              class: 'var-autocomplete__item' + (i === varAutocomplete.selected ? ' var-autocomplete__item--selected' : ''),
              onMousedown: (e) => { e.preventDefault(); insertVariable(v.name) },
              onMouseenter: () => { varAutocomplete.selected = i },
            }, [
              h('span', { class: 'material-icons', style: 'font-size:12px; color:' + (v.source === 'global' ? 'var(--p-primary)' : 'var(--p-node-completed)') }, v.icon),
              h('span', { class: 'var-autocomplete__name' }, `{${v.name}}`),
              h('span', { class: 'var-autocomplete__type' }, v.type),
              h('span', { class: 'var-autocomplete__desc' }, v.description),
            ]),
          )) : null,

          // Block right-click context menu
          ctxMenu.value ? h('div', {
            class: 'block-ctx-menu',
            style: { left: ctxMenu.value.x + 'px', top: ctxMenu.value.y + 'px' },
            onClick: (e) => e.stopPropagation(),
          }, [
            (() => {
              const node = nodes.value.find(n => n.id === ctxMenu.value.nodeId)
              const isDisabled = node?.data.disabled
              return h('button', {
                class: 'block-ctx-menu__item',
                onClick: () => { toggleBlockDisabled(ctxMenu.value.nodeId); ctxMenu.value = null },
              }, [
                h('span', { class: 'material-icons', style: 'font-size:16px' }, isDisabled ? 'check_circle' : 'block'),
                isDisabled ? 'Enable Block' : 'Disable Block',
              ])
            })(),
            h('button', {
              class: 'block-ctx-menu__item block-ctx-menu__item--danger',
              onClick: () => { deleteNode(ctxMenu.value.nodeId); ctxMenu.value = null },
            }, [
              h('span', { class: 'material-icons', style: 'font-size:16px' }, 'delete'),
              'Delete Block',
            ]),
          ]) : null,
        ]),

      // Console — always visible, resizable like VS Code terminal
      h('div', { class: 'run-console', style: { height: `${consoleHeight.value}px` } }, [
        // Resize handle
        h('div', {
          class: 'run-console__resize',
          onMousedown: (e) => {
            e.preventDefault()
            const startY = e.clientY
            const startH = consoleHeight.value
            const onMove = (ev) => {
              const newH = Math.max(60, Math.min(600, startH - (ev.clientY - startY)))
              consoleHeight.value = newH
            }
            const onUp = () => {
              document.removeEventListener('mousemove', onMove)
              document.removeEventListener('mouseup', onUp)
              localStorage.setItem('plumber-console-h', String(consoleHeight.value))
            }
            document.addEventListener('mousemove', onMove)
            document.addEventListener('mouseup', onUp)
          },
        }),
        h('div', { class: 'run-console__tabs' }, [
          h('button', { class: ['run-console__tab', consoleTab.value === 'log' && 'run-console__tab--active'], onClick: () => { consoleTab.value = 'log' } }, [
            h('span', { class: 'material-icons', style: 'font-size:13px' }, 'terminal'),
            ` Log${runLog.value.length ? ` (${runLog.value.length})` : ''}`,
          ]),
          h('button', { class: ['run-console__tab', consoleTab.value === 'output' && 'run-console__tab--active'], onClick: () => { consoleTab.value = 'output' } }, [
            h('span', { class: 'material-icons', style: 'font-size:13px' }, 'data_object'),
            ' Output',
          ]),
          h('div', { style: 'flex:1' }),
          runLog.value.length > 0 ? h('button', { class: 'icon-btn', title: 'Clear', onClick: () => { runLog.value = []; runOutput.value = null; resetAllNodeStatuses() } },
            [h('span', { class: 'material-icons', style: 'font-size:14px' }, 'delete_sweep')]) : null,
        ]),
        consoleTab.value === 'log'
          ? h('div', { class: 'run-console__log' },
              runLog.value.length === 0
                ? [h('div', { style: 'color:var(--p-text-secondary); padding:16px; text-align:center; font-size:12px' }, 'Run a pipeline to see output here')]
                : runLog.value.map((entry, i) => {
                    const icons = { info: 'info', success: 'check_circle', error: 'error' }
                    const colors = { info: 'var(--p-text-secondary)', success: 'var(--p-node-completed)', error: 'var(--p-node-failed)' }
                    return h('div', {
                      key: i, class: 'run-console__entry',
                      onClick: () => {
                        if (entry.block_uid) {
                          selectedNodeId.value = entry.block_uid
                          if (entry.error_fields?.length) {
                            const node = nodes.value.find(n => n.id === entry.block_uid)
                            if (node) node.data = { ...node.data, errorFields: entry.error_fields }
                          }
                        }
                        if (entry.output) { runOutput.value = entry.output; consoleTab.value = 'output' }
                      },
                      style: entry.block_uid ? 'cursor:pointer' : '',
                    }, [
                      h('span', { class: 'material-icons', style: `font-size:14px; color:${colors[entry.type]}; flex-shrink:0` }, icons[entry.type] || 'info'),
                      h('span', { class: 'run-console__time' }, entry.ts?.slice(11, 19) || ''),
                      h('span', { style: `color:${colors[entry.type] || 'var(--p-text)'}; flex:1` }, entry.message),
                      entry.quickfix ? h('button', {
                        class: 'quickfix-btn',
                        title: entry.quickfix.description,
                        onClick: (ev) => {
                          ev.stopPropagation()
                          for (const [targetField, fix] of Object.entries(entry.quickfix.fixes)) {
                            const existing = edgeFieldMappings[fix.fmKey] || {}
                            edgeFieldMappings[fix.fmKey] = { ...existing, [targetField]: fix.sourceField }
                          }
                          pushUndo()
                          showStatus('Applied field mapping fix — try running again')
                        },
                      }, [
                        h('span', { class: 'material-icons', style: 'font-size:13px' }, 'auto_fix_high'),
                        ' Fix',
                      ]) : null,
                    ])
                  }),
            )
          : h('div', { class: 'run-console__output' }, [
              runOutput.value
                ? h('pre', { class: 'run-console__json' }, JSON.stringify(runOutput.value, null, 2))
                : h('div', { style: 'color:var(--p-text-secondary); padding:16px; text-align:center; font-size:12px' }, 'No output yet'),
            ]),
      ]), // close console
      ]), // close plumber-main
      ]), // close plumber-body

      // Text editor modal
      renderTextModal(),
    ])
  },
})


// ---------- Router ----------

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', component: PipelineListPage },
    { path: '/editor/:id?', component: PipelineEditorPage },
  ],
})

// ---------- Root App ----------

const App = defineComponent({
  name: 'PlumberApp',
  setup() {
    const themeMenuOpen = ref(false)
    const userMenuOpen = ref(false)
    const editingHandle = ref(false)
    const handleInput = ref('')
    const themes = getAllThemes()
    const currentThemeId = ref(localStorage.getItem('plumber-theme') || 'lodge')
    function switchTheme(id) {
      const t = getTheme(id)
      if (t) { applyTheme(t); currentThemeId.value = id }
      themeMenuOpen.value = false
    }
    async function saveHandle() {
      const h_ = handleInput.value.trim()
      if (h_) {
        try {
          const data = await api('PUT', '/me', { handle: h_ })
          userHandle.value = data.handle
        } catch {}
      }
      editingHandle.value = false
      userMenuOpen.value = false
    }
    function startEditHandle() {
      handleInput.value = userHandle.value
      editingHandle.value = true
    }
    return () => h('div', { class: 'plumber-layout' }, [
      h('div', { class: 'plumber-header' }, [
        h('span', { class: 'plumber-header__title' }, 'Plumber'),
        h('div', { class: 'plumber-header__nav' }, [
          h('a', { class: 'nav-btn', href: '/', onClick: (e) => { e.preventDefault(); router.push('/') } }, [
            h('span', { class: 'material-icons', style: 'font-size:18px' }, 'dashboard'), 'Pipelines',
          ]),
        ]),
        h('div', { class: 'plumber-header__spacer' }),
        // Theme picker
        h('div', { class: 'theme-dropdown' }, [
          h('button', { class: 'icon-btn', onClick: () => { themeMenuOpen.value = !themeMenuOpen.value; userMenuOpen.value = false } },
            [h('span', { class: 'material-icons' }, 'palette')]),
          themeMenuOpen.value && h('div', { class: 'theme-menu' },
            themes.map(t => h('button', { class: 'theme-menu__item', onClick: () => switchTheme(t.id) }, [
              h('span', { class: 'theme-swatch', style: { background: t.colors.primary } }), t.label,
              currentThemeId.value === t.id ? h('span', { class: 'material-icons', style: 'font-size:14px; margin-left:auto; color:var(--p-node-completed)' }, 'check') : null,
            ])),
          ),
        ]),
        // User handle
        h('div', { class: 'user-menu-wrapper' }, [
          h('button', {
            class: 'user-badge',
            onClick: () => { userMenuOpen.value = !userMenuOpen.value; themeMenuOpen.value = false },
          }, [
            h('span', { class: 'material-icons', style: 'font-size:18px' }, 'person'),
            h('span', { class: 'user-badge__name' }, userHandle.value || '...'),
          ]),
          userMenuOpen.value && h('div', { class: 'user-menu' }, [
            editingHandle.value
              ? h('div', { class: 'user-menu__edit' }, [
                  h('input', {
                    class: 'user-menu__input',
                    value: handleInput.value,
                    onInput: (e) => { handleInput.value = e.target.value },
                    onKeydown: (e) => { if (e.key === 'Enter') saveHandle() },
                    autofocus: true,
                    maxlength: 32,
                  }),
                  h('button', { class: 'toolbar-btn toolbar-btn--primary', style: 'padding:4px 10px; font-size:12px', onClick: saveHandle }, 'Save'),
                ])
              : h('button', { class: 'user-menu__item', onClick: startEditHandle }, [
                  h('span', { class: 'material-icons', style: 'font-size:16px' }, 'edit'),
                  'Change name',
                ]),
          ]),
        ]),
      ]),
      h(Vue.resolveComponent('router-view')),
    ])
  },
})

// ---------- Mount ----------

const app = createApp(App)
app.use(createPinia())
app.use(router)
app.use(Quasar)
app.mount('#app')

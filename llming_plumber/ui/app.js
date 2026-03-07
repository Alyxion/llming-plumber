/**
 * Plumber UI — zero-build entry point.
 *
 * Uses Vue (global) + Quasar (UMD) loaded via <script> tags,
 * and Vue Flow / Pinia via importmap ESM modules.
 * All dependencies vendored locally — no npm, no CDN.
 */

import { createRouter, createWebHistory } from 'vue-router'
import { createPinia } from 'pinia'
import { VueFlow, useVueFlow, Position, MarkerType } from '@vue-flow/core'
import { getAllThemes, getTheme, applyTheme } from './themes/index.js'

const { createApp, ref, reactive, computed, onMounted, watch, h, defineComponent, markRaw, nextTick, toRaw } = Vue

const IMAGINARY_OWNER = 'plumber-dev-user'

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
  datetime_formatter: 'schedule', safe_eval: 'calculate',
  pdf_reader: 'picture_as_pdf', excel_reader: 'table_chart',
  word_reader: 'description', word_writer: 'description',
  news_api: 'newspaper', dwd_weather: 'cloud',
  nina: 'warning', autobahn: 'directions_car', feiertage: 'celebration',
  pegel_online: 'water', azure_blob_write: 'cloud_upload',
  azure_blob_read: 'cloud_download', azure_blob_list: 'folder',
  azure_blob_delete: 'delete', azure_blob_trigger: 'bolt',
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

// ---------- Convert pipeline model ↔ Vue Flow nodes/edges ----------

function blocksToNodes(blocks) {
  return blocks.map(b => ({
    id: b.uid, type: 'block',
    position: { x: b.position?.x || 0, y: b.position?.y || 0 },
    data: { label: b.label, blockType: b.block_type, config: b.config || {}, status: 'idle' },
    sourcePosition: Position.Right, targetPosition: Position.Left,
  }))
}
function _fmKey(source, target) { return `${source}→${target}` }

function pipesToEdges(pipes, edgeFieldMappings) {
  return pipes.map(p => {
    if (p.field_mapping) {
      const key = _fmKey(p.source_block_uid, p.target_block_uid)
      edgeFieldMappings[key] = p.field_mapping
    }
    return {
      id: p.uid, source: p.source_block_uid, target: p.target_block_uid,
      sourceHandle: p.source_fitting_uid, targetHandle: p.target_fitting_uid,
      type: 'smoothstep', animated: false,
      style: { stroke: 'var(--p-edge-default)', strokeWidth: 2 },
      markerEnd: { type: MarkerType.ArrowClosed, color: 'var(--p-edge-default)' },
    }
  })
}
function nodesToBlocks(nodes) {
  return nodes.map(n => ({
    uid: n.id, block_type: n.data.blockType, label: n.data.label,
    config: n.data.config || {},
    position: { x: Math.round(n.position.x), y: Math.round(n.position.y) },
  }))
}
function edgesToPipes(edges, edgeFieldMappings) {
  return edges.map(e => {
    const key = _fmKey(e.source, e.target)
    const fm = edgeFieldMappings[key]
    return {
      uid: e.id, source_block_uid: e.source, source_fitting_uid: e.sourceHandle || 'output',
      target_block_uid: e.target, target_fitting_uid: e.targetHandle || 'input',
      ...(fm ? { field_mapping: fm } : {}),
    }
  })
}

// ---------- Pages ----------

const PipelineListPage = defineComponent({
  name: 'PipelineListPage',
  setup() {
    const pipelines = ref([])
    const loading = ref(true)
    async function load() {
      loading.value = true
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
    onMounted(load)

    return () => h('div', { class: 'pipeline-list' }, [
      h('div', { class: 'pipeline-list__header' }, [
        h('h2', null, 'Pipelines'),
        h('div', { style: 'flex:1' }),
        h('button', { class: 'toolbar-btn toolbar-btn--primary', onClick: () => router.push('/editor') }, [
          h('span', { class: 'material-icons', style: 'font-size:16px' }, 'add'), ' New Pipeline',
        ]),
      ]),
      loading.value ? h('div', { class: 'empty-state' }, 'Loading...')
        : pipelines.value.length === 0
          ? h('div', { class: 'empty-state' }, [
              h('span', { class: 'material-icons' }, 'account_tree'),
              h('div', { style: 'font-size:16px; margin-top:8px' }, 'No pipelines yet'),
              h('div', { style: 'margin-top:4px' }, 'Create your first pipeline to get started'),
            ])
          : h('div', { class: 'pipeline-grid' }, pipelines.value.map(p =>
              h('div', { class: 'pipeline-card', onClick: () => router.push(`/editor/${p.id || p._id}`) }, [
                h('div', { style: 'display:flex; align-items:center; gap:8px' }, [
                  h('div', { class: 'pipeline-card__name', style: 'flex:1' }, p.name),
                  h('button', {
                    class: 'icon-btn', title: 'Delete', style: 'width:24px; height:24px',
                    onClick: (e) => deletePipeline(p.id || p._id, e),
                  }, [h('span', { class: 'material-icons', style: 'font-size:14px; color:var(--p-node-failed)' }, 'delete')]),
                ]),
                h('div', { class: 'pipeline-card__meta' }, `${(p.blocks || []).length} blocks · v${p.version || 1}`),
              ]),
            )),
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

    // Vue Flow state
    const nodes = ref([])
    const edges = ref([])
    // Field mappings stored outside Vue Flow (it strips edge.data)
    const edgeFieldMappings = reactive({})

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
      nodes.value = data.nodes
      edges.value = data.edges
      // Restore field mappings
      Object.keys(edgeFieldMappings).forEach(k => delete edgeFieldMappings[k])
      if (data.edgeFieldMappings) Object.assign(edgeFieldMappings, data.edgeFieldMappings)
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
    }
    onMounted(() => document.addEventListener('keydown', _handleKeyboard))

    // Run state
    const running = ref(false)
    const runLog = ref([])
    const runOutput = ref(null)
    const consoleTab = ref('log')
    const consoleHeight = ref(parseInt(localStorage.getItem('plumber-console-h')) || 180)

    // LLM defaults (fetched from API)
    const llmDefaults = ref({ tiers: {}, providers: [], models: {} })

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

    function addBlock(blockType, label, config, x, y) {
      const id = uid()
      nodes.value = [...nodes.value, {
        id, type: 'block', position: { x, y },
        data: { label: label || blockType.replace(/_/g, ' '), blockType, config: config || {}, status: 'idle' },
        sourcePosition: Position.Right, targetPosition: Position.Left,
      }]
      return id
    }

    function deleteNode(nodeId) {
      nodes.value = nodes.value.filter(n => n.id !== nodeId)
      edges.value = edges.value.filter(e => e.source !== nodeId && e.target !== nodeId)
      if (selectedNodeId.value === nodeId) selectedNodeId.value = null
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

    function onConnect(params) {
      const id = uid()
      const fm = inferFieldMapping(params.source, params.target)
      if (fm) edgeFieldMappings[_fmKey(params.source, params.target)] = fm
      edges.value = [...edges.value, {
        id, source: params.source, target: params.target,
        sourceHandle: params.sourceHandle || 'output', targetHandle: params.targetHandle || 'input',
        type: 'smoothstep', animated: false,
        style: { stroke: 'var(--p-edge-default)', strokeWidth: 2 },
        markerEnd: { type: MarkerType.ArrowClosed, color: 'var(--p-edge-default)' },
      }]
    }

    function onNodeClick(event) { selectedNodeId.value = event.node.id }
    function onPaneClick() { selectedNodeId.value = null }

    function onNodeDoubleClick(event) {
      const node = event.node
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
      const bounds = event.currentTarget.getBoundingClientRect()
      addBlock(entry.block_type, entry.block_type.replace(/_/g, ' '), {}, event.clientX - bounds.left, event.clientY - bounds.top)
    }
    function onDragOver(event) { event.preventDefault(); event.dataTransfer.dropEffect = 'move' }

    function loadPreset(preset) {
      pipelineName.value = preset.name
      pipelineId.value = null
      Object.keys(edgeFieldMappings).forEach(k => delete edgeFieldMappings[k])
      nodes.value = blocksToNodes(preset.blocks)
      edges.value = pipesToEdges(preset.pipes, edgeFieldMappings)
      presetMenuOpen.value = false
      selectedNodeId.value = null
      undoStack.length = 0; redoStack.length = 0
      nextTick(pushUndo)
      showStatus(`Loaded preset: ${preset.name}`)
    }

    async function savePipeline() {
      saving.value = true
      const payload = {
        name: pipelineName.value, blocks: nodesToBlocks(nodes.value),
        pipes: edgesToPipes(edges.value, edgeFieldMappings), owner_id: IMAGINARY_OWNER, owner_type: 'user', tags: [],
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
      } catch (err) { showStatus(`Error: ${err.message}`) }
      saving.value = false
    }

    async function loadPipeline(id) {
      try {
        const p = await api('GET', `/pipelines/${id}`)
        pipelineName.value = p.name
        pipelineId.value = p.id || p._id
        nodes.value = blocksToNodes(p.blocks || [])
        edges.value = pipesToEdges(p.pipes || [], edgeFieldMappings)
        undoStack.length = 0; redoStack.length = 0
        nextTick(pushUndo)
      } catch (err) { showStatus(`Failed to load: ${err.message}`) }
    }

    function setNodeStatus(nodeId, status) {
      const node = nodes.value.find(n => n.id === nodeId)
      if (node) node.data = { ...node.data, status }
    }
    function resetAllNodeStatuses() {
      nodes.value = nodes.value.map(n => ({ ...n, data: { ...n.data, status: 'idle', errorFields: [] } }))
    }

    async function runPipeline() {
      if (nodes.value.length === 0) { showStatus('No blocks to run'); return }
      running.value = true; runLog.value = []; runOutput.value = null
      consoleTab.value = 'log'
      resetAllNodeStatuses()

      const payload = { name: pipelineName.value, blocks: nodesToBlocks(nodes.value), pipes: edgesToPipes(edges.value, edgeFieldMappings) }
      try {
        const resp = await fetch('/api/run-inline', {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload),
        })
        const reader = resp.body.getReader()
        const decoder = new TextDecoder()
        let buffer = ''
        while (true) {
          const { done, value } = await reader.read()
          if (done) break
          buffer += decoder.decode(value, { stream: true })
          const lines = buffer.split('\n')
          buffer = lines.pop()
          let eventType = '', eventData = ''
          for (const line of lines) {
            if (line.startsWith('event: ')) eventType = line.slice(7)
            else if (line.startsWith('data: ')) eventData = line.slice(6)
            else if (line === '' && eventType && eventData) {
              try { handleRunEvent(eventType, JSON.parse(eventData)) } catch {}
              eventType = ''; eventData = ''
            }
          }
        }
      } catch (err) {
        runLog.value.push({ type: 'error', message: `Connection failed: ${err.message}`, ts: new Date().toISOString() })
      }
      running.value = false
    }

    function handleRunEvent(type, data) {
      const ts = new Date().toISOString()
      switch (type) {
        case 'start':
          runLog.value.push({ type: 'info', message: `Starting pipeline (${data.total} blocks)`, ts }); break
        case 'block_start':
          setNodeStatus(data.block_uid, 'running')
          runLog.value.push({ type: 'info', message: `Running: ${data.label} (${data.block_type})`, ts, block_uid: data.block_uid })
          edges.value = edges.value.map(e => ({
            ...e, animated: e.target === data.block_uid,
            style: { stroke: e.target === data.block_uid ? 'var(--p-edge-active)' : 'var(--p-edge-default)', strokeWidth: 2 },
          }))
          break
        case 'block_done':
          setNodeStatus(data.block_uid, data.status)
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
          edges.value = edges.value.map(e => ({ ...e, animated: false, style: { stroke: 'var(--p-edge-default)', strokeWidth: 2 } }))
          break
        }
      }
    }

    onMounted(async () => {
      try { catalog.value = await api('GET', '/blocks') } catch {}
      try { llmDefaults.value = await api('GET', '/llm-defaults') } catch {}
      const route = router.currentRoute.value
      if (route.params.id) await loadPipeline(route.params.id)
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

      // Default text
      return h('input', {
        class: 'config-input', type: 'text', placeholder: prop.placeholder || prop.description || '',
        value: val ?? prop.default ?? '', onInput: (e) => setVal(e.target.value),
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
            onInput: (e) => { node.data = { ...node.data, label: e.target.value } },
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
      const hasFields = fields.length > 0

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

          // Upstream fields sidebar
          hasFields ? h('div', { class: 'text-modal__fields' }, [
            h('div', { class: 'text-modal__fields-header' }, 'Available Fields'),
            h('div', { class: 'text-modal__fields-hint' }, 'Click to insert at cursor'),
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

          h(VueFlow, {
            nodes: nodes.value, edges: edges.value,
            'onUpdate:modelValue': () => {},
            onConnect, onNodeClick, onPaneClick, onNodeDoubleClick,
            onNodesChange: (changes) => {
              for (const c of changes) {
                if (c.type === 'position' && c.position) {
                  const n = nodes.value.find(n => n.id === c.id)
                  if (n) n.position = c.position
                }
              }
            },
            fitView: true, class: 'vue-flow', style: { width: '100%', height: '100%' },
            defaultEdgeOptions: {
              type: 'smoothstep',
              style: { stroke: 'var(--p-edge-default)', strokeWidth: 2 },
              markerEnd: { type: MarkerType.ArrowClosed, color: 'var(--p-edge-default)' },
            },
          }, {
            'node-block': (props) => {
              const d = props.data, status = d.status || 'idle'
              const isSelected = selectedNodeId.value === props.id
              const sc = { running: 'var(--p-node-running)', completed: 'var(--p-node-completed)', failed: 'var(--p-node-failed)', idle: 'var(--p-node-idle)' }
              const HandleComp = Vue.resolveComponent('Handle')
              return h('div', { class: ['block-node', `block-node--${status}`, isSelected && 'block-node--selected'] }, [
                h(HandleComp, { type: 'target', position: Position.Left, id: 'input', class: 'block-handle' }),
                h('div', { class: 'block-node__status', style: { background: sc[status] || sc.idle } }),
                h('div', { class: 'block-node__content' }, [
                  h('span', { class: 'material-icons block-node__icon' }, blockIcon(d.blockType)),
                  h('div', null, [
                    h('div', { class: 'block-node__label' }, d.label),
                    h('div', { class: 'block-node__type' }, d.blockType),
                  ]),
                ]),
                h(HandleComp, { type: 'source', position: Position.Right, id: 'output', class: 'block-handle' }),
              ])
            },
          }),

          // Config popout (floating over canvas)
          renderConfigPopout(),
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

const RunListPage = defineComponent({
  name: 'RunListPage',
  setup() {
    const runs = ref([]); const loading = ref(true)
    onMounted(async () => { try { runs.value = await api('GET', '/runs') } catch {}; loading.value = false })
    return () => h('div', { class: 'pipeline-list' }, [
      h('h2', { style: 'color: var(--p-text)' }, 'Runs'),
      loading.value ? h('div', { style: 'color: var(--p-text-secondary)' }, 'Loading...')
        : runs.value.length === 0
          ? h('div', { style: 'color: var(--p-text-secondary)' }, 'Run history will appear here once pipelines have been executed.')
          : h('div', { class: 'pipeline-grid' }, runs.value.map(r =>
              h('div', { class: 'pipeline-card' }, [
                h('div', { class: 'pipeline-card__name' }, `Run ${(r.id || r._id || '').slice(-8)}`),
                h('div', { class: 'pipeline-card__meta' }, [
                  h('span', { class: `status-badge status-badge--${r.status}` }, r.status),
                  ` · pipeline ${(r.pipeline_id || '').slice(-8)}`,
                ]),
              ]),
            )),
    ])
  },
})

// ---------- Router ----------

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', component: PipelineListPage },
    { path: '/editor/:id?', component: PipelineEditorPage },
    { path: '/runs', component: RunListPage },
  ],
})

// ---------- Root App ----------

const App = defineComponent({
  name: 'PlumberApp',
  setup() {
    const themeMenuOpen = ref(false)
    const themes = getAllThemes()
    const currentThemeId = ref(localStorage.getItem('plumber-theme') || 'lodge')
    function switchTheme(id) {
      const t = getTheme(id)
      if (t) { applyTheme(t); currentThemeId.value = id }
      themeMenuOpen.value = false
    }
    return () => h('div', { class: 'plumber-layout' }, [
      h('div', { class: 'plumber-header' }, [
        h('span', { class: 'plumber-header__title' }, 'Plumber'),
        h('div', { class: 'plumber-header__nav' }, [
          h('a', { class: 'nav-btn', href: '/', onClick: (e) => { e.preventDefault(); router.push('/') } }, [
            h('span', { class: 'material-icons', style: 'font-size:18px' }, 'dashboard'), 'Pipelines',
          ]),
          h('a', { class: 'nav-btn', href: '/runs', onClick: (e) => { e.preventDefault(); router.push('/runs') } }, [
            h('span', { class: 'material-icons', style: 'font-size:18px' }, 'play_circle'), 'Runs',
          ]),
        ]),
        h('div', { class: 'plumber-header__spacer' }),
        h('div', { class: 'theme-dropdown' }, [
          h('button', { class: 'icon-btn', onClick: () => { themeMenuOpen.value = !themeMenuOpen.value } },
            [h('span', { class: 'material-icons' }, 'palette')]),
          themeMenuOpen.value && h('div', { class: 'theme-menu' },
            themes.map(t => h('button', { class: 'theme-menu__item', onClick: () => switchTheme(t.id) }, [
              h('span', { class: 'theme-swatch', style: { background: t.colors.primary } }), t.label,
              currentThemeId.value === t.id ? h('span', { class: 'material-icons', style: 'font-size:14px; margin-left:auto; color:var(--p-node-completed)' }, 'check') : null,
            ])),
          ),
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

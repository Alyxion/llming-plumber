/**
 * Plumber Plugin System
 *
 * Plugins extend the UI without modifying core code:
 *
 *   import { registerPlugin } from './plugins/index.js'
 *
 *   registerPlugin({
 *     id: 'my-plugin',
 *     name: 'My Plugin',
 *     version: '1.0.0',
 *     nodeRenderers: { 'my_block': MyBlockComponent },
 *     sidebarPanels: [{ id: 'panel', label: 'My Panel', icon: 'star', component: MyPanel }],
 *     themes: [myTheme],
 *     toolbarActions: [{ id: 'act', label: 'Act', icon: 'bolt', handler: () => {} }],
 *     hooks: { 'block:select': (uid) => console.log('selected', uid) },
 *     onActivate: () => console.log('activated'),
 *   })
 */

import { registerTheme } from '../themes/index.js'

const plugins = new Map()
const nodeRenderers = new Map()
const sidebarPanels = new Map()
const toolbarActions = new Map()
const hooks = new Map()

export function registerPlugin(plugin) {
  if (plugins.has(plugin.id)) unregisterPlugin(plugin.id)
  plugins.set(plugin.id, plugin)

  if (plugin.nodeRenderers) {
    for (const [type, comp] of Object.entries(plugin.nodeRenderers)) {
      nodeRenderers.set(type, comp)
    }
  }
  if (plugin.sidebarPanels) {
    for (const panel of plugin.sidebarPanels) sidebarPanels.set(panel.id, panel)
  }
  if (plugin.themes) {
    for (const theme of plugin.themes) registerTheme(theme)
  }
  if (plugin.toolbarActions) {
    for (const action of plugin.toolbarActions) toolbarActions.set(action.id, action)
  }
  if (plugin.hooks) {
    for (const [event, fn] of Object.entries(plugin.hooks)) {
      if (!hooks.has(event)) hooks.set(event, [])
      hooks.get(event).push(fn)
    }
  }
  if (plugin.onActivate) plugin.onActivate()
}

export function unregisterPlugin(id) {
  const p = plugins.get(id)
  if (!p) return
  if (p.nodeRenderers) for (const t of Object.keys(p.nodeRenderers)) nodeRenderers.delete(t)
  if (p.sidebarPanels) for (const s of p.sidebarPanels) sidebarPanels.delete(s.id)
  if (p.toolbarActions) for (const a of p.toolbarActions) toolbarActions.delete(a.id)
  if (p.onDeactivate) p.onDeactivate()
  plugins.delete(id)
}

export function getNodeRenderer(blockType) { return nodeRenderers.get(blockType) }
export function getSidebarPanels() { return Array.from(sidebarPanels.values()) }
export function getToolbarActions() { return Array.from(toolbarActions.values()) }

export async function emitHook(event, ...args) {
  for (const fn of (hooks.get(event) || [])) await fn(...args)
}

export function getRegisteredPlugins() { return Array.from(plugins.values()) }

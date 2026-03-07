/**
 * Theme system — CSS custom properties, switchable at runtime.
 * Plugin authors can register custom themes via registerTheme().
 */

export const lodgeTheme = {
  id: 'lodge', label: 'Lodge', dark: true,
  colors: {
    primary: '#003D8F', secondary: '#1A6BC4', accent: '#F59E0B',
    background: '#151521', surface: '#1D1D2B', 'surface-variant': '#252538',
    text: '#E8E8F0', 'text-secondary': '#9CA3AF', border: '#2D2D42',
    'node-running': '#3B82F6', 'node-completed': '#22C55E',
    'node-failed': '#EF4444', 'node-idle': '#4B5563',
    'edge-default': '#4B5563', 'edge-active': '#3B82F6',
    'sidebar-bg': '#1A1A28', 'sidebar-text': '#C9C9D9',
  },
}

export const daylightTheme = {
  id: 'daylight', label: 'Daylight', dark: false,
  colors: {
    primary: '#003D8F', secondary: '#2563EB', accent: '#D97706',
    background: '#F8FAFC', surface: '#FFFFFF', 'surface-variant': '#F1F5F9',
    text: '#1E293B', 'text-secondary': '#64748B', border: '#E2E8F0',
    'node-running': '#2563EB', 'node-completed': '#16A34A',
    'node-failed': '#DC2626', 'node-idle': '#94A3B8',
    'edge-default': '#94A3B8', 'edge-active': '#2563EB',
    'sidebar-bg': '#FFFFFF', 'sidebar-text': '#334155',
  },
}

export const midnightTheme = {
  id: 'midnight', label: 'Midnight', dark: true,
  colors: {
    primary: '#7C3AED', secondary: '#A78BFA', accent: '#F472B6',
    background: '#0F0F1A', surface: '#18182B', 'surface-variant': '#1F1F35',
    text: '#E2E8F0', 'text-secondary': '#94A3B8', border: '#2A2A45',
    'node-running': '#A78BFA', 'node-completed': '#34D399',
    'node-failed': '#FB7185', 'node-idle': '#475569',
    'edge-default': '#475569', 'edge-active': '#A78BFA',
    'sidebar-bg': '#131325', 'sidebar-text': '#CBD5E1',
  },
}

export const forestTheme = {
  id: 'forest', label: 'Forest', dark: true,
  colors: {
    primary: '#059669', secondary: '#34D399', accent: '#FBBF24',
    background: '#0C1A14', surface: '#132A1F', 'surface-variant': '#1A3829',
    text: '#E2F0E8', 'text-secondary': '#86BBAA', border: '#1E4030',
    'node-running': '#34D399', 'node-completed': '#22D3EE',
    'node-failed': '#F87171', 'node-idle': '#4B6358',
    'edge-default': '#4B6358', 'edge-active': '#34D399',
    'sidebar-bg': '#0E2218', 'sidebar-text': '#B8D8CC',
  },
}

const registry = new Map()

export function registerTheme(theme) { registry.set(theme.id, theme) }
export function getTheme(id) { return registry.get(id) }
export function getAllThemes() { return Array.from(registry.values()) }

export function applyTheme(theme) {
  const root = document.documentElement
  for (const [key, value] of Object.entries(theme.colors)) {
    root.style.setProperty(`--p-${key}`, value)
  }
  if (theme.dark) {
    document.body.classList.add('body--dark')
  } else {
    document.body.classList.remove('body--dark')
  }
  localStorage.setItem('plumber-theme', theme.id)
}

// Register built-in themes
registerTheme(lodgeTheme)
registerTheme(daylightTheme)
registerTheme(midnightTheme)
registerTheme(forestTheme)

// Apply saved theme or default
const saved = localStorage.getItem('plumber-theme') || 'lodge'
applyTheme(getTheme(saved) || lodgeTheme)

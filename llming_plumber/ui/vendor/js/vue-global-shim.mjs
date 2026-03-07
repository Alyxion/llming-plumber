/**
 * Shim that re-exports the global Vue (loaded via <script> UMD)
 * as an ES module so that vue-router, pinia, and vue-flow all
 * share the same Vue instance as Quasar and the app.
 *
 * We dynamically export every property from the global Vue object
 * so nothing is missed (e.g. hasInjectionContext added in 3.3+).
 */
const V = window.Vue
export default V

// Re-export every named export from the global Vue object.
// ESM requires static exports, so we list everything Vue 3.5 exposes.
// This is generated from Object.keys(Vue) on Vue 3.5.13.
export const {
  BaseTransition, BaseTransitionPropsValidators, Comment, DeprecationTypes,
  EffectScope, ErrorCodes, ErrorTypeStrings, Fragment, KeepAlive, ReactiveEffect,
  Static, Suspense, Teleport, Text, TrackOpTypes, Transition, TransitionGroup,
  TriggerOpTypes, VueElement,
  callWithAsyncErrorHandling, callWithErrorHandling, camelize, capitalize,
  cloneVNode, compatUtils, compile, computed, createApp, createBlock,
  createCommentVNode, createElementBlock, createElementVNode, createHydrationRenderer,
  createPropsRestProxy, createRenderer, createSSRApp, createSlots, createStaticVNode,
  createTextVNode, createVNode, customRef,
  defineAsyncComponent, defineComponent, defineCustomElement, defineEmits,
  defineExpose, defineModel, defineOptions, defineProps, defineSSRCustomElement,
  defineSlots,
  effect, effectScope, getCurrentInstance, getCurrentScope, getCurrentWatcher,
  getTransitionRawChildren, guardReactiveProps,
  h, handleError, hasInjectionContext, hydrateOnIdle, hydrateOnInteraction,
  hydrateOnMediaQuery, hydrateOnVisible,
  initCustomFormatter, inject, isMemoSame, isProxy, isReactive, isReadonly,
  isRef, isRuntimeOnly, isShallow, isVNode,
  markRaw, mergeDefaults, mergeModels, mergeProps,
  nextTick, normalizeClass, normalizeProps, normalizeStyle,
  onActivated, onBeforeMount, onBeforeUnmount, onBeforeUpdate,
  onDeactivated, onErrorCaptured, onMounted, onRenderTracked,
  onRenderTriggered, onScopeDispose, onServerPrefetch, onUnmounted,
  onUpdated, onWatcherCleanup, openBlock,
  popScopeId, provide, proxyRefs, pushScopeId,
  queuePostFlushCb, reactive, readonly, ref,
  registerRuntimeCompiler, render, renderList, renderSlot,
  resolveComponent, resolveDirective, resolveDynamicComponent, resolveFilter,
  resolveTransitionHooks,
  setBlockTracking, setDevtoolsHook, setTransitionHooks,
  shallowReactive, shallowReadonly, shallowRef, ssrContextKey, ssrUtils, stop,
  toDisplayString, toHandlerKey, toHandlers, toRaw, toRef, toRefs, toValue,
  transformVNodeArgs, triggerRef,
  unref, useAttrs, useCssModule, useCssVars, useHost, useId, useModel,
  useSSRContext, useShadowRoot, useSlots, useTemplateRef, useTransitionState,
  vModelCheckbox, vModelDynamic, vModelRadio, vModelSelect, vModelText, vShow,
  version, warn, watch, watchEffect, watchPostEffect, watchSyncEffect,
  withAsyncContext, withCtx, withDefaults, withDirectives, withKeys, withMemo,
  withModifiers, withScopeId,
} = V

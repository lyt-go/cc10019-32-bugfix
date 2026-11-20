<template>
  <section class="page" data-module="event_aggregate">
    <header class="page-head">
      <div><h2>事件聚合</h2><p class="page-desc">维护埋点事件的多维聚合计算与预聚合结果管理，为事件聚合提供可验证的数据工程流程。</p></div>
    </header>
    <div class="analysis-summary">
      <article v-for="m in summaryMetrics" :key="m.label" class="analysis-card" data-testid="kpi-card">
        <span style="font-size:12px;color:var(--muted)">{{ m.label }}</span>
        <strong style="font-size:22px;display:block">{{ m.value }}</strong>
        <span :class="m.trend > 0 ? 'trend-up' : 'trend-down'" style="font-size:12px">{{ m.trend > 0 ? '↑' : '↓' }}{{ Math.abs(m.trend) }}%</span>
      </article>
    </div>
    <form class="analysis-form" @submit.prevent="runAnalysis">
      <label v-for="field in paramFields" :key="field"><span>{{ field }}</span>
        <select v-if="field.includes('方法')||field.includes('策略')||field.includes('粒度')" v-model="params[field]"><option v-for="o in options[field]" :key="o" :value="o">{{ o }}</option></select>
        <input v-else v-model="params[field]" :placeholder="`请输入${field}`" />
      </label>
      <button class="btn primary" type="submit" data-testid="run-analysis">运行分析</button>
    </form>
    <div v-if="chartData.length" class="analysis-chart">
      <div v-for="(bar, i) in chartData" :key="i" class="analysis-bar" :style="{ height: barHeight(bar.value) + 'px' }"><span>{{ bar.label }}</span></div>
    </div>
    <table class="data-table" v-if="results.length">
      <thead><tr><th v-for="c in resultColumns" :key="c">{{ c }}</th></tr></thead>
      <tbody><tr v-for="(row, i) in results" :key="i"><td v-for="c in resultColumns" :key="c">{{ row[c] ?? '—' }}</td></tr></tbody>
    </table>
    <p v-if="sourceBatchId" data-testid="source-batch" style="text-align:right;color:var(--muted);font-size:12px">
      来源清洗批次：{{ sourceBatchId }}（规则版本 {{ sourceVersion || '—' }}）
    </p>
    <p v-if="!results.length && !loading" style="text-align:center;color:var(--muted)">请配置参数后运行分析</p>
  </section>
</template>
<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { request } from '@/api/client'
// 后端路由使用连字符前缀，聚合结果按清洗批次生成。
const ENDPOINT = '/api/event-aggregate'
const summaryMetrics = ref<{label:string;value:number;trend:number}[]>([])
const results = ref<any[]>([]); const chartData = ref<{label:string;value:number}[]>([])
const loading = ref(false)
const sourceBatchId = ref(''); const sourceVersion = ref('')
const params = ref<Record<string,string>>({})
const paramFields = ["配置编号", "事件名称", "聚合维度", "聚合函数", "时间粒度", "过滤条件", "输出表名", "更新周期"].filter((f: string) => !['编号','名称','版本'].includes(f)).slice(0, 4)
const options: Record<string,string[]> = { '统计粒度': ['按日','按周','按月'], '分组字段': ['全量','按类型','按区域'], '比较方法': ['环比','同比','绝对值'], '时间范围': ['最近7天','最近30天','最近90天'] }
const resultColumns = computed(() => ["配置编号", "事件名称", "聚合维度", "聚合函数", "时间粒度", "过滤条件", "输出表名", "更新周期"].slice(0, 5))
function barHeight(v: number) { const max = Math.max(...chartData.value.map(d => d.value), 1); return Math.max(10, (v / max) * 140) }
function decorate(rows: any[]) {
  return rows.map((r: any) => ({
    配置编号: params.value['配置编号'] || r.config_id,
    事件名称: r.event_id,
    聚合维度: r.agg_dimensions ? r.agg_dimensions.join(' / ') : '—',
    聚合函数: r.agg_function ?? '—',
    时间粒度: r.time_grain ?? r.time_bucket,
    过滤条件: r.filter_expr ?? '',
    输出表名: r.output_table ?? '',
    更新周期: r.time_bucket ?? '',
  }))
}
async function fetchSummary() {
  try {
    const res = await request(`${ENDPOINT}/summary`)
    summaryMetrics.value = (await res.json()).metrics || []
  } catch (e) { /* 指标加载失败不影响运行分析 */ }
}
async function loadPublished() {
  // 页面刷新后回显已发布结果，而不是使用示例行。
  try {
    const res = await request(`${ENDPOINT}/results`)
    if (!res.ok) return
    const payload = await res.json()
    const published = payload.items || []
    if (!published.length) return
    const latest = published.reduce((best: any, item: any) =>
      (item.generated_at || 0) >= (best.generated_at || 0) ? item : best, published[0])
    results.value = decorate(latest.result_rows || [])
    sourceBatchId.value = latest.source_batch_id || ''
    sourceVersion.value = latest.source_cleanse_version || ''
    chartData.value = (latest.result_rows || []).slice(0, 6).map((r: any) => ({
      label: String(r.time_bucket ?? r.time_grain ?? ''),
      value: Number(r.agg_value ?? 0),
    }))
  } catch (e) { /* 无已发布结果时保持空态 */ }
}
async function runAnalysis() {
  loading.value = true
  try {
    // 未填配置编号时由后端按全部配置聚合，避免页面落到示例数据。
    const res = await request(`${ENDPOINT}/run`, { method: 'POST', body: JSON.stringify({ values: params.value }) })
    const data = await res.json()
    if (!res.ok) throw new Error(data.detail || '聚合失败')
    const entry = data.entry || {}
    const rows = entry.result_rows || entry.items || []
    results.value = decorate(rows)
    sourceBatchId.value = entry.source_batch_id || ''
    sourceVersion.value = entry.source_cleanse_version || ''
    chartData.value = rows.slice(0, 6).map((r: any) => ({
      label: String(r.time_bucket ?? r.time_grain ?? ''),
      value: Number(r.agg_value ?? 0),
    }))
    await fetchSummary()
  } catch (e) { results.value = []; chartData.value = []; sourceBatchId.value = ''; sourceVersion.value = '' }
  loading.value = false
}
onMounted(async () => {
  await fetchSummary()
  await loadPublished()
})
</script>

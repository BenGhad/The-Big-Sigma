import { apiRequest } from '../client'

export type DatasetColumn = {
  name: string
  dtype: 'int' | 'float' | 'string' | 'bool' | 'datetime' | 'unknown'
  nullable: boolean
  unique_count?: number
  null_count?: number
}

export type Dataset = {
  id: number
  name: string
  filename: string
  row_count: number
  column_count: number
  columns: DatasetColumn[]
  y_columns: string[]
  is_time_series: boolean
  created_at: string
}

export type NumericSummary = {
  min: number
  max: number
  mean: number
  std: number
}

export type ColumnStats = DatasetColumn & {
  summary?: NumericSummary
}

export type DatasetStats = {
  dataset_id: number
  row_count: number
  column_count: number
  columns: ColumnStats[]
}

type UploadDatasetParams = {
  file: File
  name?: string
  yColumns?: string[]
  isTimeSeries?: boolean
}

type GetDatasetsParams = {
  limit?: number
  offset?: number
}

type GetDatasetViewsParams = {
  limit?: number
  offset?: number
}

export type FilterOp =
  | 'eq'
  | 'neq'
  | 'lt'
  | 'lte'
  | 'gt'
  | 'gte'
  | 'contains'
  | 'starts_with'
  | 'ends_with'
  | 'in'
  | 'not_in'
  | 'is_null'
  | 'not_null'
  | 'between'

type FilterValue = string | number | boolean | null

export type FilterClause = {
  column: string
  op: FilterOp
  value?: FilterValue | FilterValue[]
}

export type SortClause = {
  column: string
  direction: 'asc' | 'desc'
}

export type HighlightRule = {
  column: string
  op: 'eq' | 'lt' | 'gt' | 'between' | 'contains'
  value: string | number | boolean | (string | number)[]
  label?: string
}

export type QuerySpec = {
  select?: string[]
  filters?: FilterClause[][]
  sort?: SortClause[]
  limit?: number
  offset?: number
  y_columns?: string[]
  highlights?: HighlightRule[]
}

export type QuerySpecPatch = {
  select?: string[]
  filters?: FilterClause[][]
  sort?: SortClause[]
  limit?: number
  offset?: number
  y_columns?: string[]
  highlights?: HighlightRule[]
}

export type SavedView = {
  id: number
  dataset_id: number
  name: string
  query: QuerySpec
  created_at: string
}

export type QueryResponse = {
  rows: Record<string, unknown>[]
  total_rows: number
  returned_rows: number
  next_offset: number | null
  applied_query: QuerySpec
}

export type DatasetListResponse = {
  items: Dataset[]
  total: number
  limit: number
  offset: number
}

export type SavedViewListResponse = {
  items: SavedView[]
  total: number
}

export type DatasetSettings = {
  dataset_id: number
  y_columns: string[]
  is_time_series: boolean
}

type DatasetSettingsUpdate = {
  y_columns?: string[]
  is_time_series?: boolean
}

type SavedViewCreate = {
  name: string
  query: QuerySpec
}

type SavedViewUpdate = {
  name?: string
  query?: QuerySpec
}

function normalizeDataset(dataset: Dataset): Dataset {
  const yColumns = Array.isArray(dataset.y_columns)
    ? dataset.y_columns
        .filter((column): column is string => typeof column === 'string')
        .map((column) => column.trim())
        .filter((column) => column.length > 0)
    : []

  return {
    ...dataset,
    y_columns: Array.from(new Set(yColumns)),
    is_time_series: typeof dataset.is_time_series === 'boolean' ? dataset.is_time_series : false,
  }
}

export async function uploadDataset(params: UploadDatasetParams): Promise<Dataset> {
  const formData = new FormData()
  formData.append('file', params.file)

  const normalizedName = params.name?.trim()
  if (normalizedName) {
    formData.append('name', normalizedName)
  }

  if (params.yColumns && params.yColumns.length > 0) {
    formData.append('y_columns', params.yColumns.join(','))
  }

  if (typeof params.isTimeSeries === 'boolean') {
    formData.append('is_time_series', String(params.isTimeSeries))
  }

  const dataset = await apiRequest<Dataset>('/v1/datasets', {
    method: 'POST',
    body: formData,
  })
  return normalizeDataset(dataset)
}

export async function getDatasets(
  params: GetDatasetsParams = {},
): Promise<DatasetListResponse> {
  const searchParams = new URLSearchParams()

  if (typeof params.limit === 'number') {
    searchParams.set('limit', String(params.limit))
  }
  if (typeof params.offset === 'number') {
    searchParams.set('offset', String(params.offset))
  }

  const query = searchParams.toString()
  const path = query ? `/v1/datasets?${query}` : '/v1/datasets'
  const response = await apiRequest<DatasetListResponse>(path)
  return {
    ...response,
    items: response.items.map((dataset) => normalizeDataset(dataset)),
  }
}

export async function getDataset(datasetId: number): Promise<Dataset> {
  const dataset = await apiRequest<Dataset>(`/v1/datasets/${datasetId}`)
  return normalizeDataset(dataset)
}

export async function deleteDataset(datasetId: number): Promise<void> {
  await apiRequest<unknown>(`/v1/datasets/${datasetId}`, {
    method: 'DELETE',
  })
}

export async function getDatasetStats(datasetId: number): Promise<DatasetStats> {
  return apiRequest<DatasetStats>(`/v1/datasets/${datasetId}/stats`)
}

export async function getDatasetSettings(datasetId: number): Promise<DatasetSettings> {
  return apiRequest<DatasetSettings>(`/v1/datasets/${datasetId}/settings`)
}

export async function updateDatasetSettings(
  datasetId: number,
  payload: DatasetSettingsUpdate,
): Promise<DatasetSettings> {
  return apiRequest<DatasetSettings>(`/v1/datasets/${datasetId}/settings`, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  })
}

export async function queryDataset(datasetId: number, query: QuerySpec): Promise<QueryResponse> {
  return apiRequest<QueryResponse>(`/v1/datasets/${datasetId}/query`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(query),
  })
}

export async function createSavedView(datasetId: number, payload: SavedViewCreate): Promise<SavedView> {
  return apiRequest<SavedView>(`/v1/datasets/${datasetId}/views`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  })
}

export async function getDatasetViews(
  datasetId: number,
  params: GetDatasetViewsParams = {},
): Promise<SavedViewListResponse> {
  const searchParams = new URLSearchParams()

  if (typeof params.limit === 'number') {
    searchParams.set('limit', String(params.limit))
  }
  if (typeof params.offset === 'number') {
    searchParams.set('offset', String(params.offset))
  }

  const query = searchParams.toString()
  const path = query ? `/v1/datasets/${datasetId}/views?${query}` : `/v1/datasets/${datasetId}/views`
  return apiRequest<SavedViewListResponse>(path)
}

export async function runSavedViewQuery(
  viewId: number,
  override: QuerySpecPatch | null = null,
): Promise<QueryResponse> {
  return apiRequest<QueryResponse>(`/v1/views/${viewId}/query`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(override),
  })
}

export async function updateSavedView(
  viewId: number,
  payload: SavedViewUpdate,
): Promise<SavedView> {
  return apiRequest<SavedView>(`/v1/views/${viewId}`, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  })
}

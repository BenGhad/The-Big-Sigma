import {apiRequest} from '../client'


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
    created_at: string
}

export type NumericSummary = {
    min: number,
    max: number,
    mean: number,
    std: number,
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
}

type GetDatasetsParams = {
    limit?: number
    offset?: number
}


type QuerySpec = {
    select?: string[]
    filters?: FilterClause[]
    sort?: SortClause[]
    limit?: number
    offset?: number
    highlights?: HighlightRule[]
}

export type FilterOp =
    | "eq" | "neq"
    | "lt" | "lte" | "gt" | "gte"
    | "contains" | "starts_with" | "ends_with"
    | "in" | "not_in"
    | "is_null" | "not_null"
    | "between"

type FilterValue = string | number | boolean | null
type FilterClause = {
    column: string
    op: FilterOp
    value?: FilterValue | FilterValue[]
}

type SortClause = {
    column: string
    direction: "asc" | "desc"
}

type HighlightRule = {
    column: string
    op: "eq" | "lt" | "gt" | "between" | "contains"
    value: string | number | boolean | (string | number)[]
    label?: string
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
    total_rows: number             // after filters, before pagination
    returned_rows: number          // size of rows in this response
    next_offset: number | null     // null => no more rows
    applied_query: QuerySpec
}

export async function uploadDataset(params: UploadDatasetParams): Promise<Dataset> {
    const formData = new FormData()
    formData.append('file', params.file)

    const normalizedName = params.name?.trim()
    if (normalizedName) {
        formData.append('name', normalizedName)
    }

    return apiRequest<Dataset>('/v1/datasets', {
        method: 'POST',
        body: formData,
    })
}

export type DatasetListResponse = {
    items: Dataset[]
    total: number
    limit: number
    offset: number
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
  return apiRequest<DatasetListResponse>(path)
}

export async function getDataset(datasetId: number): Promise<Dataset> {
  return apiRequest<Dataset>(`/v1/datasets/${datasetId}`)
}

export async function deleteDataset(datasetId: number): Promise<void> {
  await apiRequest<unknown>(`/v1/datasets/${datasetId}`, {
    method: 'DELETE',
  })
}

export async function getDatasetStats(datasetId: number): Promise<DatasetStats> {
  return apiRequest<DatasetStats>(`/v1/datasets/${datasetId}/stats`)
}

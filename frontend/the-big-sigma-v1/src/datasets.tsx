import { type FormEvent, useCallback, useEffect, useMemo, useRef, useState } from 'react'

import { ApiError } from './client'
import {
  createSavedView,
  deleteDataset,
  getDatasets,
  getDatasetViews,
  updateSavedView,
  type Dataset,
  type DatasetColumn,
  type FilterClause,
  type FilterOp,
  type QuerySpec,
  type SavedView,
  uploadDataset,
} from './api/datasets'

const EMPTY_COLUMNS: DatasetColumn[] = []
const EMPTY_QUERY: QuerySpec = {}

const FILTER_OP_OPTIONS: Array<{ value: FilterOp; label: string }> = [
  { value: 'eq', label: 'equals' },
  { value: 'neq', label: 'not equals' },
  { value: 'lt', label: 'less than' },
  { value: 'lte', label: 'less than or equal' },
  { value: 'gt', label: 'greater than' },
  { value: 'gte', label: 'greater than or equal' },
  { value: 'contains', label: 'contains' },
  { value: 'starts_with', label: 'starts with' },
  { value: 'ends_with', label: 'ends with' },
  { value: 'in', label: 'in list' },
  { value: 'not_in', label: 'not in list' },
  { value: 'is_null', label: 'is null' },
  { value: 'not_null', label: 'is not null' },
  { value: 'between', label: 'between' },
]

const NULL_VALUE_OPS = new Set<FilterOp>(['is_null', 'not_null'])
const LIST_VALUE_OPS = new Set<FilterOp>(['in', 'not_in', 'between'])

export type DatasetSelection = {
  dataset: Dataset | null
  savedView: SavedView | null
  query: QuerySpec
}

type DatasetsPanelProps = {
  onSelectionChange: (selection: DatasetSelection) => void
}

type UploadState = {
  file: File | null
  name: string
  yColumns: string
  isTimeSeries: boolean
}

type ViewFilterDraft = {
  id: number
  column: string
  op: FilterOp
  value: string
}

type ViewFilterGroupDraft = {
  id: number
  clauses: ViewFilterDraft[]
}

type ViewSortDraft = {
  id: number
  column: string
  direction: 'asc' | 'desc'
}

type QueryBuildResult = {
  query: QuerySpec | null
  error: string | null
}

type BuildQueryInput = {
  datasetColumns: DatasetColumn[]
  selectedColumns: string[]
  selectedYColumns: string[]
  filterGroups: ViewFilterGroupDraft[]
  sorts: ViewSortDraft[]
}

type HashSelectionState = {
  datasetId: number | null
  savedViewId: number | null
}

const EMPTY_UPLOAD_STATE: UploadState = {
  file: null,
  name: '',
  yColumns: '',
  isTimeSeries: false,
}

function parsePositiveHashId(rawValue: string | null): number | null {
  if (rawValue === null || rawValue.trim().length === 0) {
    return null
  }

  const parsed = Number(rawValue)
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return null
  }

  return parsed
}

function parseSelectionHash(rawHash: string): HashSelectionState {
  const normalizedHash = rawHash.startsWith('#') ? rawHash.slice(1) : rawHash
  if (normalizedHash.length === 0) {
    return {
      datasetId: null,
      savedViewId: null,
    }
  }

  const params = new URLSearchParams(normalizedHash)
  const datasetId = parsePositiveHashId(params.get('dataset'))
  if (datasetId === null) {
    return {
      datasetId: null,
      savedViewId: null,
    }
  }

  return {
    datasetId,
    savedViewId: parsePositiveHashId(params.get('view')),
  }
}

function formatSelectionHash(datasetId: number | null, savedViewId: number | null): string {
  if (datasetId === null) {
    return ''
  }

  const params = new URLSearchParams()
  params.set('dataset', String(datasetId))

  if (savedViewId !== null) {
    params.set('view', String(savedViewId))
  }

  return params.toString()
}

function parseYColumns(raw: string): string[] {
  const parsed = raw
    .split(',')
    .map((column) => column.trim())
    .filter((column) => column.length > 0)

  return Array.from(new Set(parsed))
}

function getErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof ApiError) {
    return error.message
  }
  if (error instanceof Error && error.message.length > 0) {
    return error.message
  }
  return fallback
}

function isNullValueOp(op: FilterOp): boolean {
  return NULL_VALUE_OPS.has(op)
}

function isListValueOp(op: FilterOp): boolean {
  return LIST_VALUE_OPS.has(op)
}

function parseBooleanToken(raw: string): boolean | null {
  const normalized = raw.trim().toLowerCase()
  if (['true', 't', '1', 'yes', 'y'].includes(normalized)) {
    return true
  }
  if (['false', 'f', '0', 'no', 'n'].includes(normalized)) {
    return false
  }
  return null
}

function parseScalarFilterValue(raw: string, dtype: DatasetColumn['dtype']): string | number | boolean {
  if (dtype === 'int') {
    if (!/^[+-]?\d+$/.test(raw)) {
      throw new Error('must be an integer value')
    }
    return Number(raw)
  }

  if (dtype === 'float') {
    const parsed = Number(raw)
    if (!Number.isFinite(parsed)) {
      throw new Error('must be a numeric value')
    }
    return parsed
  }

  if (dtype === 'bool') {
    const parsed = parseBooleanToken(raw)
    if (parsed === null) {
      throw new Error('must be a boolean value (true/false)')
    }
    return parsed
  }

  return raw
}

function createFilterClauseDraft(
  filterClauseIdRef: { current: number },
  column: string,
): ViewFilterDraft {
  const nextClauseId = filterClauseIdRef.current
  filterClauseIdRef.current += 1
  return {
    id: nextClauseId,
    column,
    op: 'eq',
    value: '',
  }
}

function isDefaultViewName(name: string): boolean {
  return name.trim().toLowerCase() === 'default'
}

function stringifyFilterValue(value: FilterClause['value']): string {
  if (Array.isArray(value)) {
    return value.map((item) => String(item)).join(', ')
  }
  if (value === undefined || value === null) {
    return ''
  }
  return String(value)
}

function cloneQuerySpec(query: QuerySpec): QuerySpec {
  return JSON.parse(JSON.stringify(query)) as QuerySpec
}

function buildViewQuerySpecFromDraft(input: BuildQueryInput): QueryBuildResult {
  if (input.datasetColumns.length === 0) {
    return { query: null, error: 'Select a dataset before building view query.' }
  }

  const columnMap = new Map(input.datasetColumns.map((column) => [column.name, column]))
  const allColumns = input.datasetColumns.map((column) => column.name)

  const selectedColumns = Array.from(
    new Set(input.selectedColumns.filter((column) => columnMap.has(column))),
  )
  const selectedColumnScope = selectedColumns.length > 0 ? new Set(selectedColumns) : new Set(allColumns)

  const query: QuerySpec = {}

  if (selectedColumns.length > 0) {
    query.select = selectedColumns
  }

  const selectedYColumns = Array.from(
    new Set(input.selectedYColumns.filter((column) => columnMap.has(column))),
  )
  if (selectedYColumns.length > 0) {
    query.y_columns = selectedYColumns
  }

  const parsedSorts: Array<{ column: string; direction: 'asc' | 'desc' }> = []
  for (let index = 0; index < input.sorts.length; index += 1) {
    const sort = input.sorts[index]

    if (!columnMap.has(sort.column)) {
      return { query: null, error: `Sort clause ${index + 1} has an invalid column.` }
    }

    if (!selectedColumnScope.has(sort.column)) {
      return {
        query: null,
        error: `Sort clause ${index + 1} column '${sort.column}' is not in selected columns.`,
      }
    }

    parsedSorts.push({
      column: sort.column,
      direction: sort.direction,
    })
  }

  if (parsedSorts.length > 0) {
    query.sort = parsedSorts
  }

  const parsedFilterGroups: FilterClause[][] = []
  for (let groupIndex = 0; groupIndex < input.filterGroups.length; groupIndex += 1) {
    const group = input.filterGroups[groupIndex]
    if (group.clauses.length === 0) {
      return { query: null, error: `Filter group ${groupIndex + 1} must include at least one clause.` }
    }

    const parsedGroup: FilterClause[] = []

    for (let clauseIndex = 0; clauseIndex < group.clauses.length; clauseIndex += 1) {
      const filter = group.clauses[clauseIndex]
      const clauseLabel = `Group ${groupIndex + 1}, clause ${clauseIndex + 1}`

      if (!columnMap.has(filter.column)) {
        return { query: null, error: `${clauseLabel} has an invalid column.` }
      }

      if (!selectedColumnScope.has(filter.column)) {
        return {
          query: null,
          error: `${clauseLabel} column '${filter.column}' is not in selected columns.`,
        }
      }

      const columnMeta = columnMap.get(filter.column)
      if (columnMeta === undefined) {
        return { query: null, error: `${clauseLabel} has an unknown column type.` }
      }

      if (isNullValueOp(filter.op)) {
        parsedGroup.push({
          column: filter.column,
          op: filter.op,
        })
        continue
      }

      const rawValue = filter.value.trim()
      if (rawValue.length === 0) {
        return { query: null, error: `${clauseLabel} requires a value.` }
      }

      if (isListValueOp(filter.op)) {
        const rawList = rawValue
          .split(',')
          .map((value) => value.trim())
          .filter((value) => value.length > 0)

        if (rawList.length === 0) {
          return { query: null, error: `${clauseLabel} requires at least one list value.` }
        }

        if (filter.op === 'between' && rawList.length !== 2) {
          return { query: null, error: `${clauseLabel} must have exactly two values for 'between'.` }
        }

        try {
          parsedGroup.push({
            column: filter.column,
            op: filter.op,
            value: rawList.map((value) => parseScalarFilterValue(value, columnMeta.dtype)),
          })
        } catch (error: unknown) {
          const reason = error instanceof Error ? error.message : 'invalid value'
          return { query: null, error: `${clauseLabel} ${reason}.` }
        }

        continue
      }

      try {
        parsedGroup.push({
          column: filter.column,
          op: filter.op,
          value: parseScalarFilterValue(rawValue, columnMeta.dtype),
        })
      } catch (error: unknown) {
        const reason = error instanceof Error ? error.message : 'invalid value'
        return { query: null, error: `${clauseLabel} ${reason}.` }
      }
    }

    parsedFilterGroups.push(parsedGroup)
  }

  if (parsedFilterGroups.length > 0) {
    query.filters = parsedFilterGroups
  }

  return { query, error: null }
}

export default function DatasetsPanel({ onSelectionChange }: DatasetsPanelProps) {
  const initialHashSelection = parseSelectionHash(
    typeof window === 'undefined' ? '' : window.location.hash,
  )

  const [datasets, setDatasets] = useState<Dataset[]>([])
  const [selectedDatasetId, setSelectedDatasetId] = useState<number | null>(initialHashSelection.datasetId)
  const [savedViews, setSavedViews] = useState<SavedView[]>([])
  const [selectedSavedViewId, setSelectedSavedViewId] = useState<number | null>(initialHashSelection.savedViewId)

  const [datasetsLoading, setDatasetsLoading] = useState(false)
  const [savedViewsLoading, setSavedViewsLoading] = useState(false)

  const [datasetsError, setDatasetsError] = useState<string | null>(null)
  const [uploadError, setUploadError] = useState<string | null>(null)
  const [savedViewsError, setSavedViewsError] = useState<string | null>(null)
  const [viewFormError, setViewFormError] = useState<string | null>(null)
  const [refreshError, setRefreshError] = useState<string | null>(null)

  const [uploadState, setUploadState] = useState<UploadState>(EMPTY_UPLOAD_STATE)
  const [uploading, setUploading] = useState(false)
  const [deletingDataset, setDeletingDataset] = useState(false)

  const [viewNameDraft, setViewNameDraft] = useState('')
  const [savingView, setSavingView] = useState(false)

  const [builderSelectedColumns, setBuilderSelectedColumns] = useState<string[]>([])
  const [builderSelectedYColumns, setBuilderSelectedYColumns] = useState<string[]>([])
  const [builderFilterGroups, setBuilderFilterGroups] = useState<ViewFilterGroupDraft[]>([])
  const [builderSorts, setBuilderSorts] = useState<ViewSortDraft[]>([])

  const [committedQuery, setCommittedQuery] = useState<QuerySpec>(EMPTY_QUERY)
  const [autoRefresh, setAutoRefresh] = useState(false)

  const fileInputRef = useRef<HTMLInputElement | null>(null)
  const filterGroupIdRef = useRef(1)
  const filterClauseIdRef = useRef(1)
  const sortIdRef = useRef(1)
  const hydratedSavedViewIdRef = useRef<number | null>(null)

  const selectedDataset = useMemo(
    () => datasets.find((dataset) => dataset.id === selectedDatasetId) ?? null,
    [datasets, selectedDatasetId],
  )

  const selectedSavedView = useMemo(
    () => savedViews.find((view) => view.id === selectedSavedViewId) ?? null,
    [savedViews, selectedSavedViewId],
  )

  const datasetColumns = useMemo(() => selectedDataset?.columns ?? EMPTY_COLUMNS, [selectedDataset])

  const orderedColumns = useMemo(
    () => [...datasetColumns],
    [datasetColumns],
  )

  const allColumnNames = useMemo(() => orderedColumns.map((column) => column.name), [orderedColumns])

  const columnMap = useMemo(
    () => new Map(orderedColumns.map((column) => [column.name, column])),
    [orderedColumns],
  )

  const clauseColumnNames = useMemo(() => {
    if (builderSelectedColumns.length > 0) {
      return builderSelectedColumns.filter((name) => columnMap.has(name))
    }
    return allColumnNames
  }, [allColumnNames, builderSelectedColumns, columnMap])

  const clauseColumnSet = useMemo(() => new Set(clauseColumnNames), [clauseColumnNames])

  const invalidFilterIds = useMemo(() => {
    const invalid = new Set<number>()
    for (const group of builderFilterGroups) {
      for (const clause of group.clauses) {
        if (!clauseColumnSet.has(clause.column)) {
          invalid.add(clause.id)
        }
      }
    }
    return invalid
  }, [builderFilterGroups, clauseColumnSet])

  const invalidSortIds = useMemo(() => {
    const invalid = new Set<number>()
    for (const sort of builderSorts) {
      if (!clauseColumnSet.has(sort.column)) {
        invalid.add(sort.id)
      }
    }
    return invalid
  }, [builderSorts, clauseColumnSet])

  const allColumnsSelected =
    orderedColumns.length > 0 && builderSelectedColumns.length === orderedColumns.length

  const loadDatasets = useCallback(async () => {
    setDatasetsLoading(true)
    setDatasetsError(null)

    try {
      const response = await getDatasets({ limit: 200, offset: 0 })
      setDatasets(response.items)

      setSelectedDatasetId((currentSelection) => {
        if (currentSelection === null) {
          return currentSelection
        }

        const stillExists = response.items.some((dataset) => dataset.id === currentSelection)
        if (!stillExists) {
          setSelectedSavedViewId(null)
          return null
        }

        return currentSelection
      })
    } catch (error: unknown) {
      setDatasetsError(getErrorMessage(error, 'Could not load datasets.'))
    } finally {
      setDatasetsLoading(false)
    }
  }, [])

  useEffect(() => {
    void loadDatasets()
  }, [loadDatasets])

  useEffect(() => {
    let active = true

    if (selectedDatasetId === null) {
      setSavedViews([])
      setSelectedSavedViewId(null)
      setSavedViewsError(null)
      return () => {
        active = false
      }
    }

    setSavedViewsLoading(true)
    setSavedViewsError(null)

    void getDatasetViews(selectedDatasetId, { limit: 200, offset: 0 })
      .then((response) => {
        if (!active) {
          return
        }

        setSavedViews(response.items)
        const defaultViewId = response.items.find((view) => isDefaultViewName(view.name))?.id ?? null
        setSelectedSavedViewId((currentSelection) => {
          if (currentSelection !== null) {
            const stillExists = response.items.some((view) => view.id === currentSelection)
            if (stillExists) {
              return currentSelection
            }
          }

          if (defaultViewId !== null) {
            return defaultViewId
          }

          return response.items.length > 0 ? response.items[0].id : null
        })
      })
      .catch((error: unknown) => {
        if (!active) {
          return
        }

        setSavedViews([])
        setSavedViewsError(getErrorMessage(error, 'Could not load saved views.'))
      })
      .finally(() => {
        if (!active) {
          return
        }
        setSavedViewsLoading(false)
      })

    return () => {
      active = false
    }
  }, [selectedDatasetId])

  useEffect(() => {
    if (selectedDatasetId === null) {
      setBuilderSelectedColumns([])
      setBuilderSelectedYColumns([])
      setBuilderFilterGroups([])
      setBuilderSorts([])
      filterGroupIdRef.current = 1
      filterClauseIdRef.current = 1
      sortIdRef.current = 1
      setViewNameDraft('')
      setCommittedQuery(EMPTY_QUERY)
      setAutoRefresh(false)
      setRefreshError(null)
      setViewFormError(null)
      hydratedSavedViewIdRef.current = null
      return
    }

    const nextDefaultY = selectedDataset?.y_columns ?? []
    const datasetDefaultQuery = nextDefaultY.length > 0 ? { y_columns: [...nextDefaultY] } : EMPTY_QUERY

    setBuilderSelectedColumns([])
    setBuilderSelectedYColumns(nextDefaultY)
    setBuilderFilterGroups([])
    setBuilderSorts([])
    filterGroupIdRef.current = 1
    filterClauseIdRef.current = 1
    sortIdRef.current = 1
    setViewNameDraft('')
    setCommittedQuery(cloneQuerySpec(datasetDefaultQuery))
    setAutoRefresh(false)
    setRefreshError(null)
    setViewFormError(null)
    hydratedSavedViewIdRef.current = null
  }, [selectedDataset?.y_columns, selectedDatasetId])

  const hydrateBuilderFromQuery = useCallback(
    (query: QuerySpec) => {
      const querySelect = query.select ?? []
      const validSelectedColumns = querySelect.filter((column) => columnMap.has(column))
      setBuilderSelectedColumns(validSelectedColumns)

      const validYColumns = (query.y_columns ?? []).filter((column) => columnMap.has(column))
      setBuilderSelectedYColumns(validYColumns)

      const nextSorts: ViewSortDraft[] = (query.sort ?? []).map((sort) => {
        const nextId = sortIdRef.current
        sortIdRef.current += 1
        return {
          id: nextId,
          column: sort.column,
          direction: sort.direction,
        }
      })
      setBuilderSorts(nextSorts)

      const nextFilterGroups: ViewFilterGroupDraft[] = (query.filters ?? [])
        .filter((group) => Array.isArray(group) && group.length > 0)
        .map((group) => {
          const nextGroupId = filterGroupIdRef.current
          filterGroupIdRef.current += 1

          const clauses: ViewFilterDraft[] = group.map((filter) => {
            const nextClauseId = filterClauseIdRef.current
            filterClauseIdRef.current += 1

            return {
              id: nextClauseId,
              column: filter.column,
              op: filter.op,
              value: stringifyFilterValue(filter.value),
            }
          })

          return {
            id: nextGroupId,
            clauses,
          }
        })
      setBuilderFilterGroups(nextFilterGroups)
    },
    [columnMap],
  )

  useEffect(() => {
    if (selectedSavedView === null) {
      hydratedSavedViewIdRef.current = null
      return
    }

    if (hydratedSavedViewIdRef.current === selectedSavedView.id) {
      return
    }

    hydrateBuilderFromQuery(selectedSavedView.query)
    setCommittedQuery(cloneQuerySpec(selectedSavedView.query))
    setViewNameDraft(selectedSavedView.name)
    setRefreshError(null)
    setViewFormError(null)
    hydratedSavedViewIdRef.current = selectedSavedView.id
  }, [hydrateBuilderFromQuery, selectedSavedView])

  const customQueryResult = useMemo(
    () =>
      buildViewQuerySpecFromDraft({
        datasetColumns: orderedColumns,
        selectedColumns: builderSelectedColumns,
        selectedYColumns: builderSelectedYColumns,
        filterGroups: builderFilterGroups,
        sorts: builderSorts,
      }),
    [
      builderFilterGroups,
      builderSelectedColumns,
      builderSelectedYColumns,
      builderSorts,
      orderedColumns,
    ],
  )

  const draftQuerySignature = useMemo(
    () => JSON.stringify(customQueryResult.query ?? EMPTY_QUERY),
    [customQueryResult.query],
  )
  const committedQuerySignature = useMemo(() => JSON.stringify(committedQuery), [committedQuery])
  const hasPendingDraftChanges =
    customQueryResult.query === null || draftQuerySignature !== committedQuerySignature

  useEffect(() => {
    if (!autoRefresh) {
      return
    }

    if (customQueryResult.query === null) {
      setRefreshError(customQueryResult.error ?? 'View query is invalid.')
      return
    }

    setRefreshError(null)
    setCommittedQuery(cloneQuerySpec(customQueryResult.query))
  }, [autoRefresh, customQueryResult.error, customQueryResult.query])

  useEffect(() => {
    onSelectionChange({
      dataset: selectedDataset,
      savedView: selectedSavedView,
      query: committedQuery,
    })
  }, [committedQuery, onSelectionChange, selectedDataset, selectedSavedView])

  const resetToDatasetChoice = useCallback(() => {
    setSelectedDatasetId(null)
    setSelectedSavedViewId(null)
    setSavedViews([])
    setSavedViewsError(null)
    setCommittedQuery(EMPTY_QUERY)
    setAutoRefresh(false)
    setRefreshError(null)
    setViewFormError(null)
  }, [])

  const applySelectionFromHash = useCallback(
    (rawHash: string) => {
      const parsed = parseSelectionHash(rawHash)

      if (parsed.datasetId === null) {
        resetToDatasetChoice()
        return
      }

      setRefreshError(null)
      setViewFormError(null)
      setSelectedDatasetId(parsed.datasetId)
      setSelectedSavedViewId(parsed.savedViewId)
    },
    [resetToDatasetChoice],
  )

  useEffect(() => {
    const handleHashChange = () => {
      applySelectionFromHash(window.location.hash)
    }

    window.addEventListener('hashchange', handleHashChange)
    return () => {
      window.removeEventListener('hashchange', handleHashChange)
    }
  }, [applySelectionFromHash])

  useEffect(() => {
    const nextHash = formatSelectionHash(selectedDatasetId, selectedSavedViewId)
    const currentHash = window.location.hash.startsWith('#')
      ? window.location.hash.slice(1)
      : window.location.hash

    if (currentHash === nextHash) {
      return
    }

    if (nextHash.length === 0) {
      window.history.pushState(null, '', `${window.location.pathname}${window.location.search}`)
      return
    }

    window.location.hash = nextHash
  }, [selectedDatasetId, selectedSavedViewId])

  const handleDatasetSelection = useCallback(
    (nextValue: string) => {
      setRefreshError(null)
      setViewFormError(null)

      if (nextValue === '') {
        resetToDatasetChoice()
        return
      }

      const parsed = Number(nextValue)
      if (Number.isNaN(parsed)) {
        return
      }

      setSelectedDatasetId(parsed)
      setSelectedSavedViewId(null)
    },
    [resetToDatasetChoice],
  )

  const handleSavedViewSelection = useCallback((nextValue: string) => {
    setRefreshError(null)
    setViewFormError(null)

    if (nextValue === '') {
      setSelectedSavedViewId(null)
      return
    }

    const parsed = Number(nextValue)
    if (Number.isNaN(parsed)) {
      return
    }

    setSelectedSavedViewId(parsed)
  }, [])

  const handleUploadSubmit = useCallback(
    async (event: FormEvent<HTMLFormElement>) => {
      event.preventDefault()
      setUploadError(null)

      if (uploadState.file === null) {
        setUploadError('Choose a CSV file before uploading.')
        return
      }

      setUploading(true)

      try {
        const uploaded = await uploadDataset({
          file: uploadState.file,
          name: uploadState.name,
          yColumns: parseYColumns(uploadState.yColumns),
          isTimeSeries: uploadState.isTimeSeries,
        })

        setDatasets((previous) => {
          const withoutDuplicate = previous.filter((dataset) => dataset.id !== uploaded.id)
          return [uploaded, ...withoutDuplicate]
        })

        setSelectedDatasetId(uploaded.id)
        setSelectedSavedViewId(null)
        setUploadState(EMPTY_UPLOAD_STATE)

        if (fileInputRef.current !== null) {
          fileInputRef.current.value = ''
        }
      } catch (error: unknown) {
        setUploadError(getErrorMessage(error, 'Dataset upload failed.'))
      } finally {
        setUploading(false)
      }
    },
    [uploadState],
  )

  const handleDeleteSelectedDataset = useCallback(async () => {
    setDatasetsError(null)

    if (selectedDataset === null) {
      return
    }

    const shouldDelete = window.confirm(
      `Delete dataset '${selectedDataset.name}'? This also removes its saved views and files.`,
    )
    if (!shouldDelete) {
      return
    }

    setDeletingDataset(true)
    try {
      await deleteDataset(selectedDataset.id)
      setDatasets((previous) => previous.filter((dataset) => dataset.id !== selectedDataset.id))
      resetToDatasetChoice()
    } catch (error: unknown) {
      setDatasetsError(getErrorMessage(error, 'Could not delete dataset.'))
    } finally {
      setDeletingDataset(false)
    }
  }, [resetToDatasetChoice, selectedDataset])

  const toggleSelectedColumn = useCallback((columnName: string, checked: boolean) => {
    setViewFormError(null)

    setBuilderSelectedColumns((previous) => {
      if (checked) {
        if (previous.includes(columnName)) {
          return previous
        }
        return [...previous, columnName]
      }

      return previous.filter((column) => column !== columnName)
    })

    if (!checked) {
      setBuilderSelectedYColumns((previous) => previous.filter((column) => column !== columnName))
    }
  }, [])

  const toggleAllSelectedColumns = useCallback(() => {
    setViewFormError(null)

    if (orderedColumns.length === 0) {
      return
    }

    setBuilderSelectedColumns((previous) => {
      if (previous.length === orderedColumns.length) {
        setBuilderSelectedYColumns([])
        return []
      }
      return orderedColumns.map((column) => column.name)
    })
  }, [orderedColumns])

  const toggleYColumn = useCallback((columnName: string, checked: boolean) => {
    setViewFormError(null)

    if (checked) {
      setBuilderSelectedYColumns((previous) => {
        if (previous.includes(columnName)) {
          return previous
        }
        return [...previous, columnName]
      })

      setBuilderSelectedColumns((previous) => {
        if (previous.includes(columnName)) {
          return previous
        }
        return [...previous, columnName]
      })

      return
    }

    setBuilderSelectedYColumns((previous) => previous.filter((column) => column !== columnName))
  }, [])

  const addFilterGroup = useCallback(() => {
    setViewFormError(null)

    if (orderedColumns.length === 0) {
      setViewFormError('Select a dataset before adding filters.')
      return
    }

    const defaultColumn = clauseColumnNames[0] ?? orderedColumns[0].name
    const nextGroupId = filterGroupIdRef.current
    filterGroupIdRef.current += 1

    setBuilderFilterGroups((previous) => [
      ...previous,
      {
        id: nextGroupId,
        clauses: [createFilterClauseDraft(filterClauseIdRef, defaultColumn)],
      },
    ])
  }, [clauseColumnNames, orderedColumns])

  const addFilterClause = useCallback(
    (groupId: number) => {
      setViewFormError(null)

      if (orderedColumns.length === 0) {
        setViewFormError('Select a dataset before adding filters.')
        return
      }

      const defaultColumn = clauseColumnNames[0] ?? orderedColumns[0].name
      setBuilderFilterGroups((previous) =>
        previous.map((group) => {
          if (group.id !== groupId) {
            return group
          }
          return {
            ...group,
            clauses: [...group.clauses, createFilterClauseDraft(filterClauseIdRef, defaultColumn)],
          }
        }),
      )
    },
    [clauseColumnNames, orderedColumns],
  )

  const removeFilterGroup = useCallback((groupId: number) => {
    setViewFormError(null)
    setBuilderFilterGroups((previous) => previous.filter((group) => group.id !== groupId))
  }, [])

  const removeFilterClause = useCallback((groupId: number, clauseId: number) => {
    setViewFormError(null)

    setBuilderFilterGroups((previous) =>
      previous.flatMap((group) => {
        if (group.id !== groupId) {
          return [group]
        }

        const remainingClauses = group.clauses.filter((clause) => clause.id !== clauseId)
        if (remainingClauses.length === 0) {
          return []
        }

        return {
          ...group,
          clauses: remainingClauses,
        }
      }),
    )
  }, [])

  const updateFilterClause = useCallback(
    (
      groupId: number,
      clauseId: number,
      patch: Partial<Omit<ViewFilterDraft, 'id'>>,
    ) => {
      setViewFormError(null)

      setBuilderFilterGroups((previous) =>
        previous.map((group) => {
          if (group.id !== groupId) {
            return group
          }

          return {
            ...group,
            clauses: group.clauses.map((clause) => {
              if (clause.id !== clauseId) {
                return clause
              }

              const nextClause: ViewFilterDraft = {
                ...clause,
                ...patch,
              }

              if (patch.op !== undefined && isNullValueOp(patch.op)) {
                nextClause.value = ''
              }

              return nextClause
            }),
          }
        }),
      )
    },
    [],
  )

  const addSort = useCallback(() => {
    setViewFormError(null)

    if (orderedColumns.length === 0) {
      setViewFormError('Select a dataset before adding sort clauses.')
      return
    }

    const defaultColumn = clauseColumnNames[0] ?? orderedColumns[0].name

    const nextId = sortIdRef.current
    sortIdRef.current += 1

    setBuilderSorts((previous) => [
      ...previous,
      {
        id: nextId,
        column: defaultColumn,
        direction: 'asc',
      },
    ])
  }, [clauseColumnNames, orderedColumns])

  const removeSort = useCallback((sortId: number) => {
    setViewFormError(null)
    setBuilderSorts((previous) => previous.filter((sort) => sort.id !== sortId))
  }, [])

  const updateSort = useCallback((sortId: number, patch: Partial<Omit<ViewSortDraft, 'id'>>) => {
    setViewFormError(null)

    setBuilderSorts((previous) =>
      previous.map((sort) => {
        if (sort.id !== sortId) {
          return sort
        }

        return {
          ...sort,
          ...patch,
        }
      }),
    )
  }, [])

  const handleRefreshPreview = useCallback(() => {
    if (customQueryResult.query === null) {
      setRefreshError(customQueryResult.error ?? 'View query is invalid.')
      return
    }

    setRefreshError(null)
    setCommittedQuery(cloneQuerySpec(customQueryResult.query))
  }, [customQueryResult.error, customQueryResult.query])

  const handleSaveAsNewView = useCallback(async () => {
    setSavedViewsError(null)
    setViewFormError(null)

    if (selectedDataset === null) {
      setViewFormError('Select a dataset before saving view changes.')
      return
    }

    const trimmedName = viewNameDraft.trim()
    if (trimmedName.length === 0) {
      setViewFormError('View name is required.')
      return
    }

    if (customQueryResult.query === null) {
      setViewFormError(customQueryResult.error ?? 'View query is invalid.')
      return
    }

    setSavingView(true)
    try {
      const created = await createSavedView(selectedDataset.id, {
        name: trimmedName,
        query: customQueryResult.query,
      })

      setSavedViews((previous) => [created, ...previous])
      setSelectedSavedViewId(created.id)
      setCommittedQuery(cloneQuerySpec(created.query))
      setRefreshError(null)
      setViewNameDraft(created.name)
      hydratedSavedViewIdRef.current = created.id
    } catch (error: unknown) {
      setViewFormError(getErrorMessage(error, 'Could not save view as new.'))
    } finally {
      setSavingView(false)
    }
  }, [customQueryResult.error, customQueryResult.query, selectedDataset, viewNameDraft])

  const handleSaveChangesToView = useCallback(async () => {
    setSavedViewsError(null)
    setViewFormError(null)

    if (selectedSavedView === null) {
      setViewFormError('Select a saved view first, or use Save As New View.')
      return
    }

    const trimmedName = viewNameDraft.trim()
    if (trimmedName.length === 0) {
      setViewFormError('View name is required.')
      return
    }

    if (customQueryResult.query === null) {
      setViewFormError(customQueryResult.error ?? 'View query is invalid.')
      return
    }

    setSavingView(true)
    try {
      const updated = await updateSavedView(selectedSavedView.id, {
        name: trimmedName,
        query: customQueryResult.query,
      })

      setSavedViews((previous) =>
        previous.map((view) => {
          if (view.id !== updated.id) {
            return view
          }
          return updated
        }),
      )
      setCommittedQuery(cloneQuerySpec(updated.query))
      setRefreshError(null)
      setViewNameDraft(updated.name)
      hydratedSavedViewIdRef.current = updated.id
    } catch (error: unknown) {
      setViewFormError(getErrorMessage(error, 'Could not save changes to this view.'))
    } finally {
      setSavingView(false)
    }
  }, [customQueryResult.error, customQueryResult.query, selectedSavedView, viewNameDraft])

  return (
    <div className="datasets-panel">
      {selectedDataset === null && (
        <div className="dataset-choice-split">
          <section className="section-block">
            <div className="section-head">
              <h2>Select Existing Dataset</h2>
              <button
                type="button"
                className="secondary-button"
                onClick={() => {
                  void loadDatasets()
                }}
                disabled={datasetsLoading}
              >
                {datasetsLoading ? 'Refreshing...' : 'Refresh'}
              </button>
            </div>

            <label>
              <span>Choose dataset</span>
              <select
                value={selectedDatasetId === null ? '' : String(selectedDatasetId)}
                onChange={(event) => {
                  handleDatasetSelection(event.target.value)
                }}
              >
                <option value="">Select a dataset</option>
                {datasets.map((dataset) => (
                  <option key={dataset.id} value={dataset.id}>
                    {dataset.name} ({dataset.row_count} rows)
                  </option>
                ))}
              </select>
            </label>

            {datasetsError !== null && <p className="inline-error">{datasetsError}</p>}
            {!datasetsLoading && datasets.length === 0 && (
              <p className="inline-note">No datasets uploaded yet.</p>
            )}
          </section>

          <section className="section-block">
            <h2>Upload New Dataset</h2>
            <form className="upload-form" onSubmit={handleUploadSubmit}>
              <label>
                <span>CSV file</span>
                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".csv,text/csv"
                  onChange={(event) => {
                    const file = event.target.files?.[0] ?? null
                    setUploadState((previous) => ({ ...previous, file }))
                  }}
                />
              </label>

              <label>
                <span>Name (optional)</span>
                <input
                  type="text"
                  placeholder="Auto-fills from filename if empty"
                  value={uploadState.name}
                  onChange={(event) => {
                    setUploadState((previous) => ({ ...previous, name: event.target.value }))
                  }}
                />
              </label>

              <label>
                <span>y columns (optional, comma-separated)</span>
                <input
                  type="text"
                  placeholder="target, price, outcome"
                  value={uploadState.yColumns}
                  onChange={(event) => {
                    setUploadState((previous) => ({ ...previous, yColumns: event.target.value }))
                  }}
                />
              </label>

              <p className="warning-note">
                Most operations require at least one y column. Upload now or define y columns in a view later.
              </p>

              <label className="checkbox-label">
                <input
                  type="checkbox"
                  checked={uploadState.isTimeSeries}
                  onChange={(event) => {
                    setUploadState((previous) => ({ ...previous, isTimeSeries: event.target.checked }))
                  }}
                />
                <span>Dataset is time series</span>
              </label>

              <button type="submit" disabled={uploading}>
                {uploading ? 'Uploading...' : 'Upload and Select'}
              </button>
            </form>

            {uploadError !== null && <p className="inline-error">{uploadError}</p>}
          </section>
        </div>
      )}

      {selectedDataset !== null && (
        <>
          <section className="section-block">
            <div className="selected-dataset-head">
              <h2>Selected Dataset</h2>
              <div className="section-actions">
                <button
                  type="button"
                  className="secondary-button"
                  onClick={() => {
                    resetToDatasetChoice()
                  }}
                >
                  Back
                </button>
                <button
                  type="button"
                  className="danger-button"
                  onClick={() => {
                    void handleDeleteSelectedDataset()
                  }}
                  disabled={deletingDataset}
                >
                  {deletingDataset ? 'Deleting...' : 'Delete Dataset'}
                </button>
              </div>
            </div>

            <div className="dataset-summary" aria-live="polite">
              <p>
                <strong>Name:</strong> {selectedDataset.name}
              </p>
              <p>
                <strong>Rows / Columns:</strong> {selectedDataset.row_count} / {selectedDataset.column_count}
              </p>
              <p>
                <strong>Dataset-level y columns:</strong>{' '}
                {selectedDataset.y_columns.length > 0 ? selectedDataset.y_columns.join(', ') : 'Not configured'}
              </p>
            </div>

            {datasetsError !== null && <p className="inline-error">{datasetsError}</p>}
          </section>

          <section className="section-block">
            <h2>Dataset View</h2>

            <label>
              <span>Selected saved view</span>
              <select
                value={selectedSavedViewId === null ? '' : String(selectedSavedViewId)}
                onChange={(event) => {
                  handleSavedViewSelection(event.target.value)
                }}
                disabled={savedViewsLoading}
              >
                <option value="" disabled>
                  Select a view
                </option>
                {savedViews.map((view) => (
                  <option key={view.id} value={view.id}>
                    {view.name}
                  </option>
                ))}
              </select>
            </label>

            {!savedViewsLoading && savedViews.length === 0 && (
              <p className="inline-note">No saved views for this dataset yet.</p>
            )}

            <div className="query-builder">
              <div className="builder-head">
                <h3>View Logic</h3>
                {selectedSavedView !== null && (
                  <p className="inline-note">
                    Editing <strong>{selectedSavedView.name}</strong>. Refresh shows draft output; Save persists.
                  </p>
                )}

                <div className="builder-actions">
                  <button
                    type="button"
                    className="secondary-button"
                    onClick={handleRefreshPreview}
                  >
                    Refresh Table
                  </button>
                  <label className="inline-checkbox">
                    <input
                      type="checkbox"
                      checked={autoRefresh}
                      onChange={(event) => {
                        setRefreshError(null)
                        setAutoRefresh(event.target.checked)
                      }}
                    />
                    <span>Auto refresh</span>
                  </label>
                </div>

                <p className="hint-note">
                  {autoRefresh
                    ? 'Auto refresh is ON.'
                    : hasPendingDraftChanges
                      ? 'Draft changes are not applied yet. Press Refresh Table.'
                      : 'Draft is in sync with table preview.'}
                </p>
                {refreshError !== null && <p className="inline-error">{refreshError}</p>}
              </div>

              <div className="columns-select">
                <div className="builder-columns-head">
                  <p className="subhead">Selected columns</p>
                  <button
                    type="button"
                    className="secondary-button"
                    onClick={toggleAllSelectedColumns}
                    disabled={orderedColumns.length === 0}
                  >
                    {allColumnsSelected ? 'Clear selected' : 'Select all'}
                  </button>
                </div>
                <p className="inline-note">
                  If none are selected, all columns are available for the query.
                </p>

                <div className="chip-list">
                  {orderedColumns.map((column) => {
                    const isChecked = builderSelectedColumns.includes(column.name)
                    return (
                      <label key={`select-${column.name}`} className="chip-toggle">
                        <input
                          type="checkbox"
                          checked={isChecked}
                          onChange={(event) => {
                            toggleSelectedColumn(column.name, event.target.checked)
                          }}
                        />
                        <span>
                          {column.name} <small>({column.dtype})</small>
                        </span>
                      </label>
                    )
                  })}
                </div>
              </div>

              <div className="columns-select">
                <p className="subhead">y columns for this view</p>
                <p className="inline-note">
                  Selecting a y column auto-selects it in regular columns. Deselecting a regular column removes it from y.
                </p>

                <div className="chip-list">
                  {orderedColumns.map((column) => {
                    const isChecked = builderSelectedYColumns.includes(column.name)
                    return (
                      <label key={`y-${column.name}`} className="chip-toggle">
                        <input
                          type="checkbox"
                          checked={isChecked}
                          onChange={(event) => {
                            toggleYColumn(column.name, event.target.checked)
                          }}
                        />
                        <span>{column.name}</span>
                      </label>
                    )
                  })}
                </div>
              </div>

              <div className="filters-head">
                <p className="subhead">Sort clauses (tie-breaker order)</p>
                <button type="button" className="secondary-button" onClick={addSort}>
                  Add sort
                </button>
              </div>

              {builderSorts.length === 0 && <p className="inline-note">No sort clauses set.</p>}

              <div className="sorts-list">
                {builderSorts.map((sort, index) => {
                  const sortOptions = [...clauseColumnNames]
                  if (!sortOptions.includes(sort.column)) {
                    sortOptions.unshift(sort.column)
                  }

                  const sortInvalid = invalidSortIds.has(sort.id)

                  return (
                    <div key={sort.id} className="sort-row">
                      <label>
                        <span>Sort #{index + 1} column</span>
                        <select
                          value={sort.column}
                          onChange={(event) => {
                            updateSort(sort.id, { column: event.target.value })
                          }}
                        >
                          {sortOptions.map((columnName) => {
                            const isInvalidOption = !clauseColumnSet.has(columnName)
                            return (
                              <option key={`sort-${sort.id}-${columnName}`} value={columnName}>
                                {isInvalidOption ? `${columnName} (invalid)` : columnName}
                              </option>
                            )
                          })}
                        </select>
                      </label>

                      <label>
                        <span>Direction</span>
                        <select
                          value={sort.direction}
                          onChange={(event) => {
                            updateSort(sort.id, {
                              direction: event.target.value === 'desc' ? 'desc' : 'asc',
                            })
                          }}
                        >
                          <option value="asc">Ascending</option>
                          <option value="desc">Descending</option>
                        </select>
                      </label>

                      <button
                        type="button"
                        className="secondary-button"
                        onClick={() => {
                          removeSort(sort.id)
                        }}
                      >
                        Remove
                      </button>

                      {sortInvalid && (
                        <p className="inline-error row-meta">
                          Invalid sort clause: column '{sort.column}' is no longer selected.
                        </p>
                      )}
                    </div>
                  )
                })}
              </div>

              <div className="filters-head">
                <p className="subhead">Filter groups (OR of AND clauses)</p>
                <button type="button" className="secondary-button" onClick={addFilterGroup}>
                  Add OR group
                </button>
              </div>

              {builderFilterGroups.length === 0 && (
                <p className="inline-note">No filter groups added. All rows are included.</p>
              )}

              <div className="filter-groups-list">
                {builderFilterGroups.map((group, groupIndex) => (
                  <div key={group.id} className="filter-group">
                    <div className="filter-group-head">
                      <p className="subhead">OR Group #{groupIndex + 1}</p>
                      <div className="filter-group-actions">
                        <button
                          type="button"
                          className="secondary-button"
                          onClick={() => {
                            addFilterClause(group.id)
                          }}
                        >
                          Add AND clause
                        </button>
                        <button
                          type="button"
                          className="secondary-button"
                          onClick={() => {
                            removeFilterGroup(group.id)
                          }}
                        >
                          Remove group
                        </button>
                      </div>
                    </div>

                    <p className="inline-note group-note">
                      Clauses inside this group are ANDed together. If any group matches, the row passes.
                    </p>

                    <div className="filters-list">
                      {group.clauses.map((filter, clauseIndex) => {
                        const filterOptions = [...clauseColumnNames]
                        if (!filterOptions.includes(filter.column)) {
                          filterOptions.unshift(filter.column)
                        }

                        const filterInvalid = invalidFilterIds.has(filter.id)
                        const requiresNoValue = isNullValueOp(filter.op)

                        return (
                          <div key={filter.id} className="filter-row">
                            <label>
                              <span>Clause #{clauseIndex + 1} column</span>
                              <select
                                value={filter.column}
                                onChange={(event) => {
                                  updateFilterClause(group.id, filter.id, { column: event.target.value })
                                }}
                              >
                                {filterOptions.map((columnName) => {
                                  const isInvalidOption = !clauseColumnSet.has(columnName)
                                  return (
                                    <option key={`filter-${filter.id}-${columnName}`} value={columnName}>
                                      {isInvalidOption ? `${columnName} (invalid)` : columnName}
                                    </option>
                                  )
                                })}
                              </select>
                            </label>

                            <label>
                              <span>Operation</span>
                              <select
                                value={filter.op}
                                onChange={(event) => {
                                  updateFilterClause(group.id, filter.id, {
                                    op: event.target.value as FilterOp,
                                  })
                                }}
                              >
                                {FILTER_OP_OPTIONS.map((option) => (
                                  <option key={option.value} value={option.value}>
                                    {option.label}
                                  </option>
                                ))}
                              </select>
                            </label>

                            <label>
                              <span>Value</span>
                              <input
                                type="text"
                                value={filter.value}
                                onChange={(event) => {
                                  updateFilterClause(group.id, filter.id, { value: event.target.value })
                                }}
                                placeholder={requiresNoValue ? '' : 'Value'}
                                disabled={requiresNoValue}
                                className={requiresNoValue ? 'filter-value-disabled' : undefined}
                              />
                            </label>

                            <button
                              type="button"
                              className="secondary-button"
                              onClick={() => {
                                removeFilterClause(group.id, filter.id)
                              }}
                            >
                              Remove clause
                            </button>

                            {filterInvalid && (
                              <p className="inline-error row-meta">
                                Invalid filter clause: column '{filter.column}' is no longer selected.
                              </p>
                            )}
                          </div>
                        )
                      })}
                    </div>
                  </div>
                ))}
              </div>

              {customQueryResult.error !== null && <p className="inline-error">{customQueryResult.error}</p>}

              <details className="query-preview">
                <summary>Draft query payload preview</summary>
                <pre>{JSON.stringify(customQueryResult.query ?? {}, null, 2)}</pre>
              </details>
            </div>

            <div className="create-view-row">
              <label>
                <span>View name</span>
                <input
                  type="text"
                  placeholder="View name"
                  value={viewNameDraft}
                  onChange={(event) => {
                    setViewFormError(null)
                    setViewNameDraft(event.target.value)
                  }}
                  disabled={savingView}
                />
              </label>

              <button
                type="button"
                onClick={() => {
                  void handleSaveChangesToView()
                }}
                disabled={savingView || selectedSavedView === null}
              >
                {savingView ? 'Saving...' : 'Save Changes'}
              </button>

              <button
                type="button"
                className="secondary-button"
                onClick={() => {
                  void handleSaveAsNewView()
                }}
                disabled={savingView}
              >
                {savingView ? 'Saving...' : 'Save As New View'}
              </button>
            </div>

            {savedViewsError !== null && <p className="inline-error">{savedViewsError}</p>}
            {viewFormError !== null && <p className="inline-error">{viewFormError}</p>}
          </section>
        </>
      )}
    </div>
  )
}

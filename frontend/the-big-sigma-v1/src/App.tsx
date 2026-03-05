import { type AnimationEvent, useCallback, useEffect, useMemo, useState } from 'react'

import {
  queryDataset,
  type QueryResponse,
} from './api/datasets'
import { ApiError } from './client'
import { type DatasetSelection } from './datasets'
import DatasetsPanel from './datasets'

import './App.css'

const DEFAULT_TABLE_LIMIT = 25
const EMPTY_QUERY = {}
type TablePaneState = 'hidden' | 'entering' | 'visible' | 'exiting'

function getErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof ApiError) {
    return error.message
  }
  if (error instanceof Error && error.message.length > 0) {
    return error.message
  }
  return fallback
}

function renderCellValue(value: unknown): string {
  if (value === null || value === undefined) {
    return ''
  }
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return String(value)
  }
  return JSON.stringify(value)
}

function moveYColumnsToRight(columns: string[], yColumns: string[] | undefined): string[] {
  if (!yColumns || yColumns.length === 0) {
    return columns
  }

  const ySet = new Set(yColumns)
  const regular: string[] = []
  const yValues: string[] = []

  for (const column of columns) {
    if (ySet.has(column)) {
      yValues.push(column)
    } else {
      regular.push(column)
    }
  }

  return [...regular, ...yValues]
}

export default function App() {
  const [selection, setSelection] = useState<DatasetSelection>({
    dataset: null,
    savedView: null,
    query: {},
  })

  const [tableLimit, setTableLimit] = useState(DEFAULT_TABLE_LIMIT)
  const [tableOffset, setTableOffset] = useState(0)
  const [tableError, setTableError] = useState<string | null>(null)
  const [tableData, setTableData] = useState<QueryResponse | null>(null)
  const [tablePaneState, setTablePaneState] = useState<TablePaneState>('hidden')
  const [lastTableSelection, setLastTableSelection] = useState<DatasetSelection | null>(null)

  const handleSelectionChange = useCallback((nextSelection: DatasetSelection) => {
    setSelection(nextSelection)
    setTableOffset(0)
    setTableData(null)
    setTableError(null)
    if (nextSelection.dataset !== null) {
      setLastTableSelection(nextSelection)
      setTablePaneState((current) => {
        if (current === 'visible' || current === 'entering') {
          return current
        }
        return 'entering'
      })
      return
    }

    setTablePaneState((current) => {
      if (current === 'hidden' || current === 'exiting') {
        return current
      }
      return 'exiting'
    })
  }, [])

  const hasDatasetSelection = selection.dataset !== null

  const handleWorkspacePaneAnimationEnd = useCallback((event: AnimationEvent<HTMLElement>) => {
    if (event.target !== event.currentTarget) {
      return
    }

    if (tablePaneState === 'entering') {
      setTablePaneState('visible')
      return
    }

    if (tablePaneState === 'exiting') {
      setTablePaneState('hidden')
      if (!hasDatasetSelection) {
        setLastTableSelection(null)
      }
    }
  }, [hasDatasetSelection, tablePaneState])

  const isTablePaneMounted = tablePaneState !== 'hidden'
  const tablePaneSelection = selection.dataset !== null ? selection : lastTableSelection

  const workspaceSplitClassName = [
    'workspace-split',
    isTablePaneMounted ? 'workspace-split--with-table' : 'workspace-split--selection',
  ].join(' ')

  const datasetPaneClassName = [
    'pane',
    'dataset-pane',
    !isTablePaneMounted ? 'dataset-pane--expanded' : '',
  ]
    .filter(Boolean)
    .join(' ')

  const workspacePaneClassName = [
    'pane',
    'workspace-pane',
    tablePaneState === 'entering' ? 'workspace-pane--entering' : '',
    tablePaneState === 'exiting' ? 'workspace-pane--exiting' : '',
  ]
    .filter(Boolean)
    .join(' ')

  const datasetId = selection.dataset?.id ?? null

  useEffect(() => {
    let active = true

    if (datasetId === null) {
      return () => {
        active = false
      }
    }

    const loadTable = async () => {
      return queryDataset(datasetId, {
        ...selection.query,
        limit: tableLimit,
        offset: tableOffset,
      })
    }

    void loadTable()
      .then((response) => {
        if (!active) {
          return
        }
        setTableError(null)
        setTableData(response)
      })
      .catch((error: unknown) => {
        if (!active) {
          return
        }
        setTableData(null)
        setTableError(getErrorMessage(error, 'Failed to load dataset rows.'))
      })

    return () => {
      active = false
    }
  }, [datasetId, selection.query, tableLimit, tableOffset])

  const tableLoading =
    datasetId !== null &&
    tableData === null &&
    tableError === null

  const effectiveTableError = tableError

  const tableColumns = useMemo(() => {
    const datasetColumnOrder = tablePaneSelection?.dataset?.columns.map((column) => column.name) ?? []
    const yColumns = tablePaneSelection?.query.y_columns

    if (tableData !== null && tableData.rows.length > 0) {
      const rowColumns = Object.keys(tableData.rows[0])
      if (datasetColumnOrder.length === 0) {
        return moveYColumnsToRight(rowColumns, yColumns)
      }

      const rowColumnSet = new Set(rowColumns)
      const datasetColumnSet = new Set(datasetColumnOrder)
      const orderedFromDataset = datasetColumnOrder.filter((column) => rowColumnSet.has(column))
      const extraColumns = rowColumns.filter((column) => !datasetColumnSet.has(column))
      return moveYColumnsToRight([...orderedFromDataset, ...extraColumns], yColumns)
    }

    if (tablePaneSelection?.query.select && tablePaneSelection.query.select.length > 0) {
      const selectedSet = new Set(tablePaneSelection.query.select)
      const orderedFromDataset =
        datasetColumnOrder.length > 0
          ? datasetColumnOrder.filter((column) => selectedSet.has(column))
          : [...tablePaneSelection.query.select]
      const orderedSet = new Set(orderedFromDataset)
      const extraColumns = tablePaneSelection.query.select.filter((column) => !orderedSet.has(column))
      return moveYColumnsToRight([...orderedFromDataset, ...extraColumns], yColumns)
    }

    return moveYColumnsToRight(datasetColumnOrder, yColumns)
  }, [tableData, tablePaneSelection])

  return (
    <main className="modeler-app">
      <header className="app-header">
        <p className="eyebrow">The Big Sigma</p>
        <h1>Modeler Workspace</h1>
      </header>

      <div className={workspaceSplitClassName}>
        <section className={datasetPaneClassName}>
          <DatasetsPanel onSelectionChange={handleSelectionChange} />
        </section>

        {isTablePaneMounted && (
          <section
            className={workspacePaneClassName}
            onAnimationEnd={handleWorkspacePaneAnimationEnd}
          >
            <h2>Table View</h2>

            {tablePaneSelection === null && <p>Select or upload a dataset on the left to begin.</p>}

            {tablePaneSelection !== null && (
              <>
                <div className="selection-preview" aria-live="polite">
                  <p>
                    <strong>Dataset:</strong> {tablePaneSelection.dataset?.name}
                  </p>
                  <p>
                    <strong>Time series:</strong> {tablePaneSelection.dataset?.is_time_series ? 'Yes' : 'No'}
                  </p>
                  <p>
                    <strong>View:</strong>{' '}
                    {tablePaneSelection.savedView === null ? 'No saved view selected' : tablePaneSelection.savedView.name}
                  </p>
                  <p>
                    <strong>y columns:</strong>{' '}
                    {tablePaneSelection.query.y_columns && tablePaneSelection.query.y_columns.length > 0
                      ? tablePaneSelection.query.y_columns.join(', ')
                      : 'None set for current view'}
                  </p>
                </div>

                <div className="table-controls">
                  <label>
                    <span>Rows per page</span>
                    <select
                      value={tableLimit}
                      onChange={(event) => {
                        setTableError(null)
                        setTableData(null)
                        setTableLimit(Number(event.target.value))
                        setTableOffset(0)
                      }}
                    >
                      <option value={25}>25</option>
                      <option value={50}>50</option>
                      <option value={100}>100</option>
                      <option value={250}>250</option>
                    </select>
                  </label>

                  <div className="pager-actions">
                    <button
                      type="button"
                      className="secondary-button"
                      onClick={() => {
                        setTableError(null)
                        setTableData(null)
                        setTableOffset((current) => Math.max(0, current - tableLimit))
                      }}
                      disabled={tableLoading || tableOffset === 0}
                    >
                      Previous
                    </button>

                    <button
                      type="button"
                      className="secondary-button"
                      onClick={() => {
                        if (tableData?.next_offset !== null && tableData?.next_offset !== undefined) {
                          setTableError(null)
                          setTableData(null)
                          setTableOffset(tableData.next_offset)
                        }
                      }}
                      disabled={tableLoading || tableData?.next_offset === null || tableData === null}
                    >
                      Next
                    </button>
                  </div>
                </div>

                <p className="inline-note">
                  Offset {tableOffset}
                  {tableData !== null ? ` | returned ${tableData.returned_rows} of ${tableData.total_rows}` : ''}
                </p>

                {effectiveTableError !== null && <p className="inline-error">{effectiveTableError}</p>}
                {tableLoading && <p className="inline-note">Loading rows...</p>}

                {!tableLoading && effectiveTableError === null && tableData !== null && tableData.rows.length === 0 && (
                  <p className="table-empty">No rows match this view.</p>
                )}

                {!tableLoading && effectiveTableError === null && tableData !== null && tableData.rows.length > 0 && (
                  <div className="table-wrap">
                    <table>
                      <thead>
                        <tr>
                          <th className="row-label-head">Row</th>
                          {tableColumns.map((column) => (
                            <th key={`head-${column}`}>{column}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {tableData.rows.map((row, rowIndex) => (
                          <tr key={`row-${rowIndex}-${tableOffset}`}>
                            <th scope="row" className="row-label-cell">{tableOffset + rowIndex + 1}</th>
                            {tableColumns.map((column) => (
                              <td key={`cell-${rowIndex}-${column}`}>{renderCellValue(row[column])}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}

                <details className="query-preview table-query-preview">
                  <summary>Applied query preview</summary>
                  <pre>{JSON.stringify(tableData?.applied_query ?? tablePaneSelection.query ?? EMPTY_QUERY, null, 2)}</pre>
                </details>
              </>
            )}
          </section>
        )}
      </div>
    </main>
  )
}

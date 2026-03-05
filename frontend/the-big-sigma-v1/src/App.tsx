import { type FormEvent, useState } from 'react'
import { uploadDataset, type Dataset } from './api/datasets'
import { ApiError } from './client'

export default function App() {
  const [name, setName] = useState('')
  const [file, setFile] = useState<File | null>(null)
  const [isUploading, setIsUploading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [uploadedDataset, setUploadedDataset] = useState<Dataset | null>(null)

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setError(null)
    setUploadedDataset(null)

    if (!file) {
      setError('Select a CSV file first.')
      return
    }

    try {
      setIsUploading(true)
      const created = await uploadDataset({ name, file })
      setUploadedDataset(created)
      setName('')
      setFile(null)
      event.currentTarget.reset()
    } catch (err) {
      if (err instanceof ApiError) {
        setError(err.code ? `${err.code}: ${err.message}` : err.message)
      } else {
        setError('Upload failed. Check that the backend is running.')
      }
    } finally {
      setIsUploading(false)
    }
  }

  return (
    <div
      style={{
        minHeight: '100vh',
        margin: 0,
        padding: '2rem',
        fontFamily: 'system-ui, -apple-system, Segoe UI, Roboto, sans-serif',
        background: '#f5f7fb',
        color: '#111827',
      }}
    >
      <header style={{ marginBottom: '1rem' }}>
        <h1 style={{ margin: 0 }}>The Big Sigma</h1>
        <p style={{ margin: '0.5rem 0 0', color: '#4b5563' }}>Upload a dataset CSV</p>
      </header>

      <main
        style={{
          background: '#fff',
          border: '1px solid #e5e7eb',
          borderRadius: 12,
          padding: '1.25rem',
          maxWidth: 560,
        }}
      >
        <form onSubmit={handleSubmit}>
          <label style={{ display: 'block', marginBottom: '0.75rem' }}>
            <span style={{ display: 'block', marginBottom: '0.35rem' }}>Dataset name (optional)</span>
            <input
              type="text"
              value={name}
              onChange={(event) => setName(event.target.value)}
              placeholder="sales_q1_2026"
              style={{ width: '100%', padding: '0.55rem' }}
            />
          </label>

          <label style={{ display: 'block', marginBottom: '1rem' }}>
            <span style={{ display: 'block', marginBottom: '0.35rem' }}>CSV file</span>
            <input
              type="file"
              accept=".csv,text/csv"
              onChange={(event) => setFile(event.target.files?.[0] ?? null)}
              style={{ width: '100%' }}
            />
          </label>

          <button
            type="submit"
            disabled={isUploading}
            style={{ padding: '0.55rem 0.85rem', cursor: isUploading ? 'not-allowed' : 'pointer' }}
          >
            {isUploading ? 'Uploading...' : 'Upload Dataset'}
          </button>
        </form>

        {error ? (
          <p style={{ marginTop: '1rem', color: '#b91c1c' }}>
            <strong>Error:</strong> {error}
          </p>
        ) : null}

        {uploadedDataset ? (
          <div
            style={{
              marginTop: '1rem',
              border: '1px solid #d1fae5',
              background: '#ecfdf5',
              borderRadius: 8,
              padding: '0.8rem',
            }}
          >
            <strong>Uploaded:</strong> {uploadedDataset.name} (id {uploadedDataset.id})<br />
            {uploadedDataset.row_count} rows, {uploadedDataset.column_count} columns
          </div>
        ) : null}
      </main>
    </div>
  )
}
